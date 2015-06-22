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

client = MongoClient(mongo_host, mongo_port)
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)

if __name__ == '__main__':
    name = 'HourlyAdStats'
    args = parse_hourly_etl_args(name)
    logger.info('Beginning %s ETLs for interval %s to %s' % (name, args.start, args.end))

    # IMP_MATCHED_ACTIONS ETL
    query_opts = {
        'destinationTable':
            {
                'datasetId': 'ad_events',
                'projectId': cliques_bq_settings.PROJECT_ID,
                'tableId': 'imp_matched_actions'
            },
            'createDisposition': 'CREATE_AS_NEEDED',
            'writeDisposition': 'WRITE_APPEND'
    }
    VIEW_ACTION_LOOKBACK = 30
    imp_matched_actions_etl = BigQueryIntermediateETL('imp_matched_actions.sql',
                                                      cliques_bq_settings,
                                                      query_options=query_opts)
    logger.info('Now matching imps to actions, storing in BigQuery')
    imp_matched_result = imp_matched_actions_etl.run(start=args.start,
                                                     end=args.end,
                                                     lookback=VIEW_ACTION_LOOKBACK)
    logger.info('Done')

    # LOAD AGGREGATES TO MONGODB
    HOURLY_ADSTAT_COLLECTION = client.exchange.test
    etl = BigQueryMongoETL('hourlyadstats.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
    logger.info('Now loading aggregates to MongoDB')
    result = etl.run(start=args.start, end=args.end)
    if result is not None:
        logger.info('Inserted %s documents into collection %s' %
                    (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
        logger.info('%s ETL Complete' % name)
    else:
        logger.info('No rows to insert, ETL complete.')