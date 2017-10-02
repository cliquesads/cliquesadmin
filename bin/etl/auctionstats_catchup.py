"""
!!!!!! DO NOT RUN THIS IN PRODUCTION !!!!!
SHOULD ONLY BE RUN ON AN AS_NEEDED BASIS
"""

from pymongo import MongoClient
import os
from datetime import datetime, timedelta
from cliquesadmin import logger
from cliquesadmin.pagerduty_utils import stacktrace_to_pd_event, create_pd_event_wrapper
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.gce_utils.bigquery import BigQueryMongoETL, cliques_bq_settings

config = JsonConfigParser()

mongo_host = config.get('ETL', 'mongodb', 'host')
mongo_port = config.get('ETL', 'mongodb', 'port')
mongo_user = config.get('ETL', 'mongodb', 'user')
mongo_pwd = config.get('ETL', 'mongodb', 'pwd')
mongo_source_db = config.get('ETL', 'mongodb', 'db')

pd_api_key = config.get('PagerDuty', 'api_key')
pd_subdomain = config.get('PagerDuty', 'subdomain')
pd_service_key = config.get('PagerDuty', 'service_key')
pd_error_callback = create_pd_event_wrapper(pd_subdomain, pd_api_key, pd_service_key)

client = MongoClient(mongo_host, mongo_port)
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)

# get dataset from config
dataset = config.get('ETL', 'bigquery', 'adEventDataset')

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
name = 'Hourly Ad Stats Catchup'

if __name__ == '__main__':

    # args = parse_hourly_etl_args(name)
    datetimes = [datetime(2017, 10, n, h) for h in range(0, 24) for n in range(1,2)]
    # datetimes += [datetime(2017, 3, 3, h) for h in range(0, 6)]

    for dt in datetimes:
        start = dt
        end = dt + timedelta(hours=1)

        logger.info('Beginning %s ETLs for interval %s to %s' % (name, start, end))

        # Wrap whole thing in blanket exception handler to write to log
        try:
           ##########################################
            # LOAD IMP & CLICK AGGREGATES TO MONGODB #
            ##########################################
            HOURLY_ADSTAT_COLLECTION = destination_db.hourlyadstats
            main_etl = BigQueryMongoETL('hourlyadstats/hourlyadstats_imps_clicks.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
            logger.info('Now loading imps and clicks aggregates to MongoDB')
            result = main_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
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
            result = actions_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
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
            new_result = defaults_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
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
            geo_main_etl = BigQueryMongoETL('geoadstats/geoadstats_imps_clicks.sql', cliques_bq_settings,
                                            GEO_ADSTAT_COLLECTION)
            logger.info('Now loading GEO imps and clicks aggregates to MongoDB')
            result = geo_main_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
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
            result = actions_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
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
            new_result = geo_defaults_etl.run(start=start, end=end, dataset=dataset, error_callback=pd_error_callback)
            if new_result is not None:
                logger.info('Inserted %s documents into collection %s' %
                            (len(new_result.inserted_ids), GEO_ADSTAT_COLLECTION.full_name))
                logger.info('%s Auction Defaults ETL Complete' % GEO_ADSTAT_COLLECTION.full_name)
            else:
                logger.info('No rows to insert, GEO Auction Defaults ETL complete.')
        except:
            # Trigger incident in PagerDuty, then write out to log file
            stacktrace_to_pd_event(pd_subdomain, pd_api_key, pd_service_key)
            logger.exception('Uncaught exception while running ETL!')
            raise