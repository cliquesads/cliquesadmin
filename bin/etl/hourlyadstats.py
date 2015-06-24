from pymongo import MongoClient

from cliquesadmin import logger
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
                                                 end=args.end)
    logger.info('Done')

    ##############################
    # LOAD AGGREGATES TO MONGODB #
    ##############################
    HOURLY_ADSTAT_COLLECTION = client.exchange.hourlyadstat
    etl = BigQueryMongoETL('hourlyadstats.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
    logger.info('Now loading aggregates to MongoDB')
    result = etl.run(start=args.start, end=args.end)
    if result is not None:
        logger.info('Inserted %s documents into collection %s' %
                    (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
        logger.info('%s ETL Complete' % name)
    else:
        logger.info('No rows to insert, ETL complete.')