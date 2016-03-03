from pymongo import MongoClient

from cliquesadmin import logger
from cliquesadmin.pagerduty_utils import stacktrace_to_pd_event, create_pd_event_wrapper
from cliquesadmin.misc_utils import parse_hourly_etl_args
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.gce_utils.bigquery import BigQueryMongoETL, BigQueryIntermediateETL, cliques_bq_settings

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
pd_error_callback = create_pd_event_wrapper(pd_subdomain, pd_api_key, pd_service_key)

client = MongoClient(mongo_host, mongo_port)
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)

GLOBAL_QUERY_OPTS = {
    'destinationTable':
        {
            'datasetId': 'ad_events',
            'projectId': cliques_bq_settings.PROJECT_ID,
        },
        'createDisposition': 'CREATE_AS_NEEDED',
        'writeDisposition': 'WRITE_APPEND'
}
name = 'HourlyAdStats'

if __name__ == '__main__':

    args = parse_hourly_etl_args(name)

    logger.info('Beginning %s ETLs for interval %s to %s' % (name, args.start, args.end))

    # Wrap whole thing in blanket exception handler to write to log
    try:
        ###########################
        # IMP_MATCHED_ACTIONS ETL #
        ###########################
        imp_matched_query_opts = GLOBAL_QUERY_OPTS
        imp_matched_query_opts['destinationTable']['tableId'] = 'imp_matched_actions'
        imp_matched_actions_etl = BigQueryIntermediateETL('imp_matched_actions.sql',
                                                          cliques_bq_settings,
                                                          query_options=imp_matched_query_opts)
        logger.info('Now matching imps to actions, storing in BigQuery')
        imp_matched_result = imp_matched_actions_etl.run(start=args.start,
                                                         end=args.end,
                                                         error_callback=pd_error_callback,
                                                         lookback=view_lookback)
        logger.info('Done')

        #############################
        # CLICK_MATCHED_ACTIONS ETL #
        #############################
        click_matched_query_opts = GLOBAL_QUERY_OPTS
        click_matched_query_opts['destinationTable']['tableId'] = 'click_matched_actions'
        imp_matched_actions_etl = BigQueryIntermediateETL('click_matched_actions.sql',
                                                          cliques_bq_settings,
                                                          query_options=click_matched_query_opts)
        logger.info('Now matching clicks to actions, storing in BigQuery')
        imp_matched_result = imp_matched_actions_etl.run(start=args.start,
                                                         end=args.end,
                                                         error_callback=pd_error_callback,
                                                         lookback=click_lookback)
        logger.info('Done')

        #####################
        # AUCTION_STATS ETL #
        #####################
        auction_query_opts = GLOBAL_QUERY_OPTS
        auction_query_opts['destinationTable']['tableId'] = 'auction_stats'
        auction_stats_etl = BigQueryIntermediateETL('auction_stats.sql',
                                                    cliques_bq_settings,
                                                    query_options=auction_query_opts)

        logger.info('Now creating auction stats, containing bid density & clearprice')
        auction_stats_result = auction_stats_etl.run(start=args.start,
                                                     end=args.end,
                                                     error_callback=pd_error_callback)
        logger.info('Done')

        #################################
        # AUCTION_STATS DEFAULT ADS ETL #
        #################################
        auction_stats_defaults_etl = BigQueryIntermediateETL('auction_stats_defaults.sql',
                                                             cliques_bq_settings,
                                                             query_options=auction_query_opts)

        logger.info('Now creating auction stats for all errored auctions (i.e. auctions w/ no bids)')
        auction_stats_defaults_result = auction_stats_defaults_etl.run(start=args.start,
                                                                       end=args.end,
                                                                       error_callback=pd_error_callback)
        logger.info('Done')

        ##########################################
        # LOAD IMP & CLICK AGGREGATES TO MONGODB #
        ##########################################
        HOURLY_ADSTAT_COLLECTION = client.exchange.hourlyadstats
        main_etl = BigQueryMongoETL('hourlyadstats_imps_clicks.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading imps and clicks aggregates to MongoDB')
        result = main_etl.run(start=args.start, end=args.end, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Primary ETL Complete' % name)
        else:
            logger.info('No rows to insert, Primary ETL complete.')

        #####################################
        # LOAD ACTION AGGREGATES TO MONGODB #
        #####################################
        actions_etl = BigQueryMongoETL('hourlyadstats_actions.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading matched action aggregates to MongoDB')
        result = actions_etl.run(start=args.start, end=args.end, error_callback=pd_error_callback)
        if result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Actions ETL Complete' % name)
        else:
            logger.info('No rows to insert, Actions ETL complete.')

        ##############################################
        # LOAD DEFAULT AUCTION AGGREGATES TO MONGODB #
        ##############################################
        defaults_etl = BigQueryMongoETL('auction_defaults.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
        logger.info('Now loading auction default aggregates to MongoDB')
        new_result = defaults_etl.run(start=args.start, end=args.end, error_callback=pd_error_callback)
        if new_result is not None:
            logger.info('Inserted %s documents into collection %s' %
                        (len(new_result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
            logger.info('%s Auction Defaults ETL Complete' % name)
        else:
            logger.info('No rows to insert, Auction Defaults ETL complete.')

    except:
        # Trigger incident in PagerDuty, then write out to log file
        stacktrace_to_pd_event(pd_subdomain, pd_api_key, pd_service_key)
        logger.exception('Uncaught exception while running ETL!')
        raise