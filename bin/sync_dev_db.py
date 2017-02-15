__author__ = 'bliang'

##################################
# ========= SYNC CONFIG ======== #
##################################

# Collections to sync (i.e. drop & bulk write)
SYNC = [
    'advertisers',
    'publishers',
    'organizations',
    'users',
    'payments',
    'insertionorders',
    'accesscodes',
    'termsandconditions',
    'demohourlyadstats',
    'users',
    'screenshots'
]

# Aggregates will only have latest 2000 documents synced
# each aggregate is tuple of collection name & sort field
AGGREGATES = [
    ('hourlyadstats', 'hour')
]

# number of documents to sync for aggregates
AGGREGATES_LIMIT = 5000

# These collections will only be inserted to dev once on first sync,
# and will not be overwritten on subsequent runs
SYNC_ONCE = [
    'cliques'
]

##############################
# ====== BEGIN Script ====== #
##############################


from cliquesadmin import logger
from pymongo import MongoClient, DESCENDING
from cliquesadmin.pagerduty_utils import stacktrace_to_pd_event
from cliquesadmin.jsonconfig import JsonConfigParser

config = JsonConfigParser()

mongo_host = config.get('ETL', 'mongodb', 'host')
mongo_port = config.get('ETL', 'mongodb', 'port')
mongo_user = config.get('ETL', 'mongodb', 'user')
mongo_pwd = config.get('ETL', 'mongodb', 'pwd')
mongo_source_db = config.get('ETL', 'mongodb', 'db')

pd_api_key = config.get('PagerDuty', 'api_key')
pd_subdomain = config.get('PagerDuty', 'subdomain')
pd_service_key = config.get('PagerDuty', 'service_key')

client = MongoClient(mongo_host, mongo_port)

# Authenticate on exchange & exchange_dev
# Needs read access to exchange & RW to dev
client.exchange.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)
client.exchange_dev.authenticate(mongo_user, mongo_pwd, source=mongo_source_db)


def insert_to_dev_from_cursor(curs, collection):
    """
    Shortcut to insert docs into named dev collection from cursor object

    :param curs: cursor object
    :param collection: collection name
    :return:
    """
    logger.info('Inserting prod documents into dev...')
    results = client.exchange_dev[collection].insert_many([doc for doc in curs])
    logger.info('DONE. %s documents inserted into exchange_dev.%s' % (len(results.inserted_ids), collection))


if __name__ == '__main__':

    logger.info('============ BEGIN Prod to Dev MongoDB Sync ===========')

    try:
        # first sync collections
        logger.info('Starting sync of SYNC collections. '
                    'All collections in exchange_dev will be dropped & replaced with prod collection')
        for col in SYNC:
            logger.info('Dropping dev collection: %s' % col)
            client.exchange_dev.drop_collection(col)
            logger.info('Getting new prod documents...')
            cursor = client.exchange[col].find()
            insert_to_dev_from_cursor(cursor, col)
        logger.info(' ============= DONE syncing SYNC collections ============= ')

        # Now sync aggregates
        logger.info('Starting sync of AGGREGATES collections. '
                    'Latest %s records in each collection will be cloned into dev, & old dev collection will be dropped'
                    % AGGREGATES_LIMIT)
        for col in AGGREGATES:
            logger.info('Dropping dev collection: %s' % col[0])
            client.exchange_dev.drop_collection(col[0])
            logger.info('Getting latest %s prod documents...' % AGGREGATES_LIMIT)
            cursor = client.exchange[col[0]].find(sort=[(col[1], DESCENDING)], limit=AGGREGATES_LIMIT)
            insert_to_dev_from_cursor(cursor, col[0])
        logger.info(' ============= DONE syncing AGGREGATES collections ============= ')

        # Now sync SYNC_ONCE collections
        logger.info('Checking SYNC_ONCE collections & cloning if not in dev')
        for col in SYNC_ONCE:
            if client.exchange_dev[col].count():
                # skip if collection exists & is non-empty
                logger.info('SYNC_ONCE collection %s already exists in dev, skipping' % col)
                continue
            else:
                logger.info('SYNC_ONCE collection %s does not yet exist in dev, will create now' % col)
                cursor = client.exchange[col].find()
                insert_to_dev_from_cursor(cursor, col)
        logger.info(' ============= DONE syncing SYNC_ONCE collections ============= ')

        logger.info('\n')
        logger.info('ALL DONE WITH DEV SYNC!')

    except:
        # Trigger incident in PagerDuty, then write out to log file
        stacktrace_to_pd_event(pd_subdomain, pd_api_key, pd_service_key)
        logger.exception('Uncaught exception while running ETL!')
        raise