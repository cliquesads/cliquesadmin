from pymongo import MongoClient

from cliquesadmin import logger
from cliquesadmin.misc_utils import parse_hourly_etl_args
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.gce_utils.bigquery import BigQueryETL, cliques_bq_settings

config = JsonConfigParser()

mongo_host = config.get('ETL', 'mongodb', 'host')
mongo_port = config.get('ETL', 'mongodb', 'port')
mongo_user = config.get('ETL', 'mongodb', 'user')
mongo_pwd = config.get('ETL', 'mongodb', 'pwd')
mongo_source_db = config.get('ETL', 'mongodb', 'db')

client = MongoClient(mongo_host, mongo_port)
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)
collection = client.exchange.test

bigquery_template = 'hourlyadstats.sql'
name = 'HourlyAdStats'

if __name__ == '__main__':

    args = parse_hourly_etl_args(name)
    etl = BigQueryETL(bigquery_template, cliques_bq_settings, collection)
    logger.info('Beginning %s ETL for interval %s to %s' % (name, args.start, args.end))

    result = etl.run(start=args.start, end=args.end)
    if result is not None:
        logger.info('Inserted %s documents into collection %s' %
                    (len(result.inserted_ids), collection.full_name))
        logger.info('%s ETL Complete' % name)
    else:
        logger.info('No rows to insert, ETL complete.')