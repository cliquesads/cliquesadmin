from pymongo import MongoClient
import os
from cliquesadmin import logger
from cliquesadmin.pagerduty_utils import stacktrace_to_pd_event, create_pd_event_wrapper
from cliquesadmin.misc_utils import parse_hourly_etl_args
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.gce_utils.bigquery import BigQueryMongoETL, BigQueryIntermediateETL, BqMongoKeywordETL, \
    cliques_bq_settings

config = JsonConfigParser()

mongo_host = config.get('ETL', 'mongodb', 'host')
mongo_port = config.get('ETL', 'mongodb', 'port')
mongo_user = config.get('ETL', 'mongodb', 'user')
mongo_pwd = config.get('ETL', 'mongodb', 'pwd')
mongo_source_db = config.get('ETL', 'mongodb', 'db')

view_lookback = config.get('ETL', 'action_lookback', 'view')
click_lookback = config.get('ETL', 'action_lookback', 'click')

pd_api_key = config.get('PagerDuty', 'api_key')
pd_subdomain = config.get('PagerDuty', 'subdomain')
pd_service_key = config.get('PagerDuty', 'service_key')
if os.environ.get('ENV', None) == 'production':
    pd_error_callback = create_pd_event_wrapper(pd_subdomain, pd_api_key, pd_service_key)
else:
    pd_error_callback = None

# get dataset from config
dataset = config.get('ETL', 'bigQuery', 'adEventDataset')

client = MongoClient(mongo_host, mongo_port)
if os.environ.get('ENV', None) != 'production':
    destination_db = client.exchange_dev
else:
    destination_db = client.exchange
destination_db.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)

GLOBAL_QUERY_OPTS = {
    'destinationTable':
        {
            'datasetId': dataset,
            'projectId': cliques_bq_settings.PROJECT_ID,
        },
        'createDisposition': 'CREATE_AS_NEEDED',
        'writeDisposition': 'WRITE_APPEND'
}
name = 'HourlyAdStats'

if __name__ == '__main__':

    args = parse_hourly_etl_args(name)
    logger.info('Environment "%s" loaded' % os.environ.get('ENV', None))
    logger.info('Connected to mongodb at %s:%s/%s' % (mongo_host, mongo_port, mongo_source_db))
    logger.info('Beginning %s ETLs for interval %s to %s' % (name, args.start, args.end))

    # get pricing config, i.e. "CPC" or "CPM". NOTE: Default is CPM and is also the fallback.
    # I.e. if pricing not set to either, will use CPM queries.
    pricing = config.get('Pricing')
    if (pricing != 'CPC') & (pricing != 'CPM'):
        logger.warn('Warning: received invalid Pricing config: \'%s\'. Please change to either \'CPC\' or \'CPM\'. '
                    'Will default to CPM, but you should change this ASAP.')
    else:
        logger.info('Pricing structure set to %s, will calculate spend based on this metric.' % pricing)

    # Wrap whole thing in blanket exception handler to write to log
    try:
        ###########################
        # IMP_MATCHED_ACTIONS ETL #
        ###########################
        imp_matched_query_opts = GLOBAL_QUERY_OPTS

        imp_matched_query_opts['destinationTable']['tableId'] = 'imp_matched_actions'
        imp_matched_actions_etl = BigQueryIntermediateETL('intermediates/imp_matched_actions.sql',
                                                          cliques_bq_settings,
                                                          query_options=imp_matched_query_opts)
        logger.info('Now matching imps to actions, storing in BigQuery')
        imp_matched_result = imp_matched_actions_etl.run(start=args.start,
                                                         end=args.end,
                                                         dataset=dataset,
                                                         error_callback=pd_error_callback,
                                                         lookback=view_lookback)
        logger.info('Done')

        #############################
        # CLICK_MATCHED_ACTIONS ETL #
        #############################
        click_matched_query_opts = GLOBAL_QUERY_OPTS
        click_matched_query_opts['destinationTable']['tableId'] = 'click_matched_actions'
        imp_matched_actions_etl = BigQueryIntermediateETL('intermediates/click_matched_actions.sql',
                                                          cliques_bq_settings,
                                                          query_options=click_matched_query_opts)
        logger.info('Now matching clicks to actions, storing in BigQuery')
        imp_matched_result = imp_matched_actions_etl.run(start=args.start,
                                                         end=args.end,
                                                         dataset=dataset,
                                                         error_callback=pd_error_callback,
                                                         lookback=click_lookback)
        logger.info('Done')

        #####################
        # AUCTION_STATS ETL #
        #####################
        auction_query_opts = GLOBAL_QUERY_OPTS
        auction_query_opts['destinationTable']['tableId'] = 'auction_stats'
        auction_stats_etl = BigQueryIntermediateETL('intermediates/auction_stats.sql',
                                                    cliques_bq_settings,
                                                    query_options=auction_query_opts)

        logger.info('Now creating auction stats, containing bid density & clearprice')
        auction_stats_result = auction_stats_etl.run(start=args.start,
                                                     end=args.end,
                                                     dataset=dataset,
                                                     error_callback=pd_error_callback)
        logger.info('Done')

        #################################
        # AUCTION_STATS DEFAULT ADS ETL #
        #################################
        auction_stats_defaults_etl = BigQueryIntermediateETL('intermediates/auction_stats_defaults.sql',
                                                             cliques_bq_settings,
                                                             query_options=auction_query_opts)

        logger.info('Now creating auction stats for all errored auctions (i.e. auctions w/ no bids)')
        auction_stats_defaults_result = auction_stats_defaults_etl.run(start=args.start,
                                                                       end=args.end,
                                                                       dataset=dataset,
                                                                       error_callback=pd_error_callback)
        logger.info('Done')

        ##########################################
        # LOAD IMP & CLICK AGGREGATES TO MONGODB #
        ##########################################
        HOURLY_ADSTAT_COLLECTION = destination_db.hourlyadstats
        # Toggle query based on pricing config
        HOURLY_ADSTAT_QUERY = 'hourlyadstats/hourlyadstats_imps_clicks_cpc.sql' if pricing == 'CPC' else \
            'hourlyadstats/hourlyadstats_imps_clicks_cpm.sql'
        main_etl = BigQueryMongoETL(HOURLY_ADSTAT_QUERY, cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading imps and clicks aggregates to MongoDB')
        result = main_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Primary ETL Complete' % name)
        else:
            logger.info('No rows to insert, Primary ETL complete.')

        #####################################
        # LOAD ACTION AGGREGATES TO MONGODB #
        #####################################
        actions_etl = BigQueryMongoETL('hourlyadstats/hourlyadstats_actions.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading matched action aggregates to MongoDB')
        result = actions_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Actions ETL Complete' % name)
        else:
            logger.info('No rows to insert, Actions ETL complete.')

        ##############################################
        # LOAD DEFAULT AUCTION AGGREGATES TO MONGODB #
        ##############################################
        defaults_etl = BigQueryMongoETL('hourlyadstats/hourlyadstats_defaults.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading auction default aggregates to MongoDB')
        new_result = defaults_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if new_result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(new_result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Auction Defaults ETL Complete' % name)
        else:
            logger.info('No rows to insert, Auction Defaults ETL complete.')

        ##############################################
        # LOAD GEO IMP & CLICK AGGREGATES TO MONGODB #
        ##############################################
        GEO_ADSTAT_COLLECTION = destination_db.geoadstats
        GEO_ADSTAT_QUERY = 'geoadstats/geoadstats_imps_clicks_cpc.sql' if pricing == 'CPC' else \
            'geoadstats/geoadstats_imps_clicks_cpm.sql'
        geo_main_etl = BigQueryMongoETL(GEO_ADSTAT_QUERY, cliques_bq_settings,
                                        GEO_ADSTAT_COLLECTION)
        logger.info('Now loading GEO imps and clicks aggregates to MongoDB')
        result = geo_main_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), GEO_ADSTAT_COLLECTION.full_name))
            logger.info('%s Primary ETL Complete' % GEO_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, Geo Primary ETL complete.')

        #########################################
        # LOAD GEO ACTION AGGREGATES TO MONGODB #
        #########################################
        geo_actions_etl = BigQueryMongoETL('geoadstats/geoadstats_actions.sql', cliques_bq_settings,
                                           GEO_ADSTAT_COLLECTION)
        logger.info('Now loading GEO matched action aggregates to MongoDB')
        result = geo_actions_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), GEO_ADSTAT_COLLECTION.full_name))
            logger.info('%s Actions ETL Complete' % GEO_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, Geo Actions ETL complete.')

        ##################################################
        # LOAD GEO DEFAULT AUCTION AGGREGATES TO MONGODB #
        ##################################################
        geo_defaults_etl = BigQueryMongoETL('geoadstats/geoadstats_defaults.sql', cliques_bq_settings,
                                            GEO_ADSTAT_COLLECTION)
        logger.info('Now loading GEO auction default aggregates to MongoDB')
        new_result = geo_defaults_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if new_result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(new_result.inserted_ids), GEO_ADSTAT_COLLECTION.full_name))
            logger.info('%s Auction Defaults ETL Complete' % GEO_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, GEO Auction Defaults ETL complete.')

        ###################################################
        # LOAD KEYWORDS IMP & CLICK AGGREGATES TO MONGODB #
        ###################################################
        KEYWORD_ADSTAT_COLLECTION = destination_db.keywordadstats
        KEYWORD_ADSTAT_QUERY = 'keywordadstats/keywordadstats_imps_clicks_cpc.sql' if pricing == 'CPC' else \
            'keywordadstats/keywordadstats_imps_clicks_cpm.sql'
        keyword_main_etl = BqMongoKeywordETL(KEYWORD_ADSTAT_QUERY, cliques_bq_settings,
                                             KEYWORD_ADSTAT_COLLECTION)
        logger.info('Now loading KEYWORD imps and clicks aggregates to MongoDB')
        result = keyword_main_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), KEYWORD_ADSTAT_COLLECTION.full_name))
            logger.info('%s Primary ETL Complete' % KEYWORD_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, Keyword Primary ETL complete.')

        #############################################
        # LOAD KEYWORD ACTION AGGREGATES TO MONGODB #
        #############################################
        keyword_actions_etl = BqMongoKeywordETL('keywordadstats/keywordadstats_actions.sql', cliques_bq_settings,
                                                KEYWORD_ADSTAT_COLLECTION)
        logger.info('Now loading KEYWORD matched action aggregates to MongoDB')
        result = keyword_actions_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), KEYWORD_ADSTAT_COLLECTION.full_name))
            logger.info('%s Actions ETL Complete' % KEYWORD_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, Keyword Actions ETL complete.')

        ##################################################
        # LOAD KEYWORD DEFAULT AUCTION AGGREGATES TO MONGODB #
        ##################################################
        keyword_defaults_etl = BqMongoKeywordETL('keywordadstats/keywordadstats_defaults.sql', cliques_bq_settings,
                                                 KEYWORD_ADSTAT_COLLECTION)
        logger.info('Now loading KEYWORD auction default aggregates to MongoDB')
        new_result = keyword_defaults_etl.run(start=args.start, end=args.end, dataset=dataset, error_callback=pd_error_callback)
        if new_result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(new_result.inserted_ids), KEYWORD_ADSTAT_COLLECTION.full_name))
            logger.info('%s Auction Defaults ETL Complete' % KEYWORD_ADSTAT_COLLECTION.full_name)
        else:
            logger.info('No rows to insert, KEYWORD Auction Defaults ETL complete.')

    except:
        # Trigger incident in PagerDuty, then write out to log file
        if os.environ.get('ENV', None) == 'production':
            stacktrace_to_pd_event(pd_subdomain, pd_api_key, pd_service_key)
        logger.exception('Uncaught exception while running ETL!')
        raise