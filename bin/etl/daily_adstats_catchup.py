"""
!!!!!! DO NOT RUN THIS IN PRODUCTION !!!!!
SHOULD ONLY BE RUN ON AN AS_NEEDED BASIS
"""

from pymongo import MongoClient
import os
from cliquesadmin import logger
from datetime import datetime, timedelta
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.etl.query_templates.mongo.daily_ad_stats import daily_ad_stats_pipeline
from cliquesadmin.etl.mongo_etl import DailyMongoAggregationETL

config = JsonConfigParser()

mongo_host = config.get('ETL', 'mongodb', 'host')
mongo_port = config.get('ETL', 'mongodb', 'port')
mongo_user = config.get('ETL', 'mongodb', 'user')
mongo_pwd = config.get('ETL', 'mongodb', 'pwd')
mongo_source_db = config.get('ETL', 'mongodb', 'db')

client = MongoClient(mongo_host, mongo_port)
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)
client = MongoClient(mongo_host, mongo_port)
if os.environ.get('ENV', None) != 'production':
    destination_db = client.exchange_dev
else:
    destination_db = client.exchange
destination_db.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)

name = 'DailyAdStatsCatchup'

if __name__ == '__main__':
    logger.info('Environment "%s" loaded' % os.environ.get('ENV', None))
    dt = datetime(2017, 9, 28, 0, 0, 0)
    while dt < datetime(2018, 10, 17, 0, 0, 0):
        start = dt
        end = dt + timedelta(days=1)
        logger.info('Beginning %s ETLs for interval %s to %s' % (name, start, end))
        input_collection = destination_db.hourlyadstats
        output_collection = destination_db.dailyadstats
        update_keys = [
            "date",
            "advertiser",
            "campaign",
            "adv_clique",
            "publisher",
            "site",
            "pub_clique"
        ]
        logger.info('Now inserting MongoDB DailyAdStats with aggregation results from HourlyAdStats...')
        logger.info('Day interval is %s to (but not including) %s' % (start, end))
        etl = DailyMongoAggregationETL('date', daily_ad_stats_pipeline, input_collection, output_collection,
                                       upsert=False)
        result = etl.run(start_datetime=start,
                         end_datetime=end)
        logger.info('DailyAdStats ETL complete.')
        dt = end
