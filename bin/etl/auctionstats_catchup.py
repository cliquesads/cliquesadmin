"""
!!!!!! DO NOT RUN THIS IN PRODUCTION !!!!!
SHOULD ONLY BE RUN ON AN AS_NEEDED BASIS
"""

from pymongo import MongoClient
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

GLOBAL_QUERY_OPTS = {
    'destinationTable':
        {
            'datasetId': 'ad_events',
            'projectId': cliques_bq_settings.PROJECT_ID,
        },
        'createDisposition': 'CREATE_AS_NEEDED',
        'writeDisposition': 'WRITE_APPEND'
}
name = 'AuctionStats Catchup'

if __name__ == '__main__':

    # args = parse_hourly_etl_args(name)
    datetimes = [datetime(2016, 3, n, h) for h in range(0, 24) for n in range(1, 3)]
    datetimes += [datetime(2016, 3, 3, h) for h in range(0, 6)]

    for dt in datetimes:
        start = dt
        end = dt + timedelta(hours=1)

        logger.info('Beginning %s ETLs for interval %s to %s' % (name, start, end))

        # Wrap whole thing in blanket exception handler to write to log
        try:
            #####################
            # AUCTION_STATS ETL #
            #####################
            # auction_query_opts = GLOBAL_QUERY_OPTS
            # auction_query_opts['destinationTable']['tableId'] = 'auction_stats'
            # auction_stats_etl = BigQueryIntermediateETL('auction_stats.sql',
            #                                             cliques_bq_settings,
            #                                             query_options=auction_query_opts)
            #
            # logger.info('Now creating auction stats, containing bid density & clearprice')
            # auction_stats_result = auction_stats_etl.run(start=start,
            #                                              end=end,
            #                                              error_callback=pd_error_callback)
            # logger.info('Done')

            ########################################
            # DELETE INVALID AGGREGATES TO MONGODB #
            ########################################
            # logger.info('Now deleting invalid aggregate results for this hour in MongoDB HourlyAdStats')
            # HOURLY_ADSTAT_COLLECTION = client.exchange.hourlyadstats
            # result = HOURLY_ADSTAT_COLLECTION.delete_many({'hour': start})
            # logger.info('Successfully deleted %s rows' % result.deleted_count)

            ##############################
            # LOAD AGGREGATES TO MONGODB #
            ##############################
            HOURLY_ADSTAT_COLLECTION = client.exchange.hourlyadstats
            etl = BigQueryMongoETL('hourlyadstats_actions.sql', cliques_bq_settings, HOURLY_ADSTAT_COLLECTION)
            logger.info('Now loading action aggregates to MongoDB')
            result = etl.run(start=start, end=end, error_callback=pd_error_callback)
            if result is not None:
                logger.info('Inserted %s documents into collection %s' %
                            (len(result.inserted_ids), HOURLY_ADSTAT_COLLECTION.full_name))
                logger.info('%s ETL Complete' % name)
            else:
                logger.info('No rows to insert, ETL complete.')
        except:
            # Trigger incident in PagerDuty, then write out to log file
            stacktrace_to_pd_event(pd_subdomain, pd_api_key, pd_service_key)
            logger.exception('Uncaught exception while running ETL!')
            raise