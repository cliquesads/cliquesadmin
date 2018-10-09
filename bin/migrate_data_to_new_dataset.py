import os
from cliquesadmin import logger
from cliquesadmin.jsonconfig import JsonConfigParser
from cliquesadmin.gce_utils.bigquery import BigQueryETL, cliques_bq_settings

config = JsonConfigParser()

GLOBAL_QUERY_OPTS = {
    'destinationTable':
        {
            'projectId': cliques_bq_settings.PROJECT_ID,
        },
    'createDisposition': 'CREATE_AS_NEEDED',
    'writeDisposition': 'WRITE_APPEND',
    'useLegacySQL': False
}

if __name__ == '__main__':

    logger.info('Environment "%s" loaded' % os.environ.get('ENV', None))
    TEMPLATE = 'migrate_data.sql'
    AD_EVENTS_TABLES = [
        'actions',
        'auction_stats',
        'auction_defaults',
        'auctions',
        'bid_responses',
        'bids',
        'click_matched_actions',
        'clicks',
        'imp_matched_actions'
        'impressions',
        # 's2s',
        'win_notices'
    ]
    HTTP_EVENTS_TABLES = [
        'http_requests',
        'http_responses'
    ]

    TARGET_DATASET = 'smartertravel_ad_events'
    DESTINATION_DATASET = 'smartertravel_ad_events_pt'
    for table in AD_EVENTS_TABLES:
        queryOpts = GLOBAL_QUERY_OPTS
        queryOpts['destinationTable']['datasetId'] = DESTINATION_DATASET
        queryOpts['destinationTable']['tableId'] = table
        etl = BigQueryETL(TEMPLATE, cliques_bq_settings, query_options=queryOpts)
        etl.extract(dataset=TARGET_DATASET, table=table)
        logger.info('Finished migrating table %s' % table)

    TARGET_HTTP_DATASET = 'smartertravel_http_events'
    DESTINATION_HTTP_DATASET = 'smartertravel_http_events_pt'
    for table in HTTP_EVENTS_TABLES:
        queryOpts = GLOBAL_QUERY_OPTS
        queryOpts['destinationTable']['datasetId'] = DESTINATION_HTTP_DATASET
        queryOpts['destinationTable']['tableId'] = table
        etl = BigQueryETL(TEMPLATE, cliques_bq_settings, query_options=queryOpts)
        etl.extract(dataset=TARGET_HTTP_DATASET, table=table)
        logger.info('Finished migrating table %s' % table)




