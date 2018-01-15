import logging
from time import sleep
import numpy as np
import pandas as pd
from jinja2 import Environment, PackageLoader
from datetime import datetime
from cliquesadmin.gce_utils import authenticate_and_build_jwt_client, CliquesGCESettings

logger = logging.getLogger(__name__)


class CliquesBigQuerySettings(CliquesGCESettings):
    API_VERSION = 'v2'
    SCOPE = 'https://www.googleapis.com/auth/bigquery'
    API_NAME = 'bigquery'


cliques_bq_settings = CliquesBigQuerySettings()
jinja_bq_env = Environment(loader=PackageLoader('cliquesadmin', 'bigquery'))


# BigQuery Datatype reference:
# https://cloud.google.com/bigquery/preparing-data-for-bigquery#datatypes
BQ_NP_TYPE_MAPPING = {
    'STRING': np.dtype(object),
    'INTEGER': np.dtype(int),
    'FLOAT': np.dtype(float),
    'BOOLEAN': np.dtype(bool),
    'TIMESTAMP': np.dtype(float),
}


def query_response_to_dataframe(query_response):
    """
    Loads query response to pandas dataframe

    Automatically handles type conversions as well, including
    timestamp to datetime64

    :param query_response: query response from BigQuery API
    :return: pandas DataFrame
    """
    rows = [tuple(map(lambda field: field['v'], row['f']))
            for row in query_response['rows']]

    # Now create dtype list of tuples for np.array
    dtypes = []
    # save timestamp columns to convert in DF to datetime64 once complete
    tstamp_cols = []
    for field in query_response['schema']['fields']:
        dtype_tuple = (str(field['name']),
                       BQ_NP_TYPE_MAPPING[field['type']])
        dtypes.append(dtype_tuple)
        if field['type'] == 'TIMESTAMP':
            tstamp_cols.append(field['name'])

    arr = np.array(rows, dtype=dtypes)
    df = pd.DataFrame(arr)

    # now go back over dataframe and convert temporary float columns
    # into datetime64's.
    for col in tstamp_cols:
        df[col] = df[col].astype('datetime64[s]')
    return df


class BigQueryETL(object):
    """
    Basic ETL Job moving data from Google BigQuery to MongoDB.

    - Extracts from BigQuery using BigQuery Query template (Jinja2)
    - Stores query result into Dataframe
    - Transforms resulting Dataframe as necessary
    - Loads transformed Dataframe into MongoDB

    Whole process can be accessed via the `BigQueryMongoETL.run` method

    Base class doesn't transform resulting data at all but provides
    hook for custom subclasses to perform their own custom transforms
    """
    def __init__(self, template, gce_settings, query_options=None):
        self.gce_settings = gce_settings
        self.gce_service = authenticate_and_build_jwt_client(gce_settings)
        self.query_options = query_options
        self.template = jinja_bq_env.get_template(template)

    def run_query(self, rendered_template, query_request, error_callback=None, **kwargs):
        """
        Hits BigQuery ETL w/ query and returns result

        :param kwargs: passed to template
        :return:
        """
        # TODO: This isn't a comprehensive parsing of query options
        if self.query_options is not None:
            # Need to use 'insert' method if destinationTable is specified
            query_data = self.query_options
            query_data['query'] = rendered_template
        else:
            query_data = {'query': rendered_template}
        query_data = {'configuration': {'query': query_data}}

        # Insert job and then wait for it to be complete
        job_response = query_request.insert(projectId=self.gce_settings.PROJECT_ID,
                                            body=query_data).execute()
        query_response = query_request.getQueryResults(projectId=self.gce_settings.PROJECT_ID,
                                                       jobId=job_response['jobReference']['jobId']).execute()

        while not query_response['jobComplete']:
            query_response = query_request.getQueryResults(projectId=self.gce_settings.PROJECT_ID,
                                                           jobId=job_response['jobReference']['jobId']).execute()
            sleep(1)

        # Handle any errors in job
        if query_response.has_key('errors'):
            error_msg = 'ERRORS encountered in BigQuery JobId %s: \n %s' % \
                        (job_response['jobReference']['jobId'], query_response['errors'])
            logger.error(error_msg)
            if error_callback:
                error_callback(error_msg)

        logger.info('Query completed, %s rows returned by jobId %s' %
                    (query_response['totalRows'],
                     query_response['jobReference']['jobId']))
        return query_response

    def extract(self, **kwargs):
        """
        Run BigQuery query and load response into dataframe

        :param kwargs: all kwargs passed directly into template as template vars
        :return:
        """
        # first format any datetime keyword args
        for kw in kwargs:
            if isinstance(kwargs[kw], datetime):
                kwargs[kw] = kwargs[kw].strftime('%Y-%m-%d %H:%M:%S')

        # parse query template with provided kwargs
        rendered_template = self.template.render(**kwargs)
        # TODO: Save response to Cloud Storage as backup?
        query_request = self.gce_service.jobs()
        query_response = self.run_query(rendered_template, query_request, **kwargs)

        # Only parse response to dataframe if it is non-null, and
        # if it is a queryResponse vs. job resource (i.e. when returning
        # asynchronous job).
        #
        # If you want to parse async job result into dataframe, use
        # getQueryResult in runQuery subclass method
        if query_response['kind'] == 'bigquery#getQueryResultsResponse':
            if int(query_response['totalRows']) > 0:
                # load into dataframe
                dataframe = query_response_to_dataframe(query_response)
                logger.info('Loaded query result to DataFrame')
                return dataframe
        return None

    def transform(self, dataframe):
        """
        Hook for subclasses to do any necessary transformation
        of raw query output before inserting into MongoDB.

        Base class just passes dataframe right through.

        :param dataframe:
        :return:
        """
        # For keyword adstats, have to transform the keywords field,
        # a string with comma separated keywords, to an string array
        is_keyword_dataframe = False
        for c in dataframe.columns:
            if c == 'keywords':
                is_keyword_dataframe = True
                break

        if is_keyword_dataframe == True:
            for index, row in dataframe.iterrows():
                row['keywords'] = row['keywords'].split(',')
                dataframe.set_value(index, 'keywords', row['keywords'])

        return dataframe

    def load(self, dataframe):
        """
        Hook for subclasses to load data into external datastore.

        Base class just passes dataframe right through.
        :param dataframe:
        :return:
        """
        return dataframe

    def run(self, **kwargs):
        dataframe = self.extract(**kwargs)
        if dataframe is not None:
            dataframe = self.transform(dataframe)
            result = self.load(dataframe)
            return result
        else:
            return None


class BigQueryMongoETL(BigQueryETL):

    def __init__(self, template, gce_settings, mongo_collection, **kwargs):
        self.mongo_collection = mongo_collection
        super(BigQueryMongoETL, self).__init__(template, gce_settings, **kwargs)

    def load(self, dataframe):
        """
        Loads a pandas dataframe object into MongoDB collection

        :param dataframe:
        :return:
        """
        records = dataframe.to_dict(orient='records')

        # Not proud of this, would love to figure out a way
        # around this natively in Pandas
        for row in records:
            for k in row:
                if isinstance(row[k], pd.tslib.Timestamp):
                    row[k] = row[k].to_datetime()

        return self.mongo_collection.insert_many(records)


class BqMongoKeywordETL(BigQueryMongoETL):

    def __init__(self, template, gce_settings, mongo_collection, **kwargs):
        self.mongo_collection = mongo_collection
        super(BigQueryMongoETL, self).__init__(template, gce_settings, **kwargs)

    def load(self, dataframe):
        """
        Loads a pandas dataframe object into MongoDB collection

        :param dataframe:
        :return:
        """
        records = dataframe.to_dict(orient='records')

        # Not proud of this, would love to figure out a way
        # around this natively in Pandas
        for row in records:
            for k in row:
                if isinstance(row[k], pd.tslib.Timestamp):
                    row[k] = row[k].to_datetime()

        return self.mongo_collection.insert_many(records)

    def transform(self, dataframe):
        """
        Transforms keywords column of comma separated strings in dataframe to
        array to store in Mongo collection

        :param dataframe:
        :return:
        """
        # For keyword adstats, have to transform the keywords field,
        # a string with comma separated keywords, to an string array
        for index, row in dataframe.iterrows():
            if row['keywords']:
                row['keywords'] = row['keywords'].split(',')
            dataframe.set_value(index, 'keywords', row['keywords'])

        return dataframe


class BigQueryIntermediateETL(BigQueryETL):

    def run_query(self, rendered_template, query_request, error_callback=None, **kwargs):
        """
        Runs query and stores results in a destination table.

        Job is run asynchronously, so
        :param kwargs: passed to template
        :return:
        """
        SECONDS_TO_SLEEP_BETWEEN_JOB_CALLS = 3

        query_data = self.query_options
        query_data['query'] = rendered_template
        # For insert jobs, need to nest options in 'query' sub-object under 'configuration'
        query_data = {'configuration': {'query': query_data}}
        job = query_request.insert(projectId=self.gce_settings.PROJECT_ID,
                                   body=query_data).execute()

        # You can call getQueryResults which will only return when job
        # is complete, but the results here could be very large so
        # it's better to just check on the job resource periodically
        while job['status']['state'] != 'DONE':
            sleep(SECONDS_TO_SLEEP_BETWEEN_JOB_CALLS)
            job = self.gce_service.jobs().get(projectId=job['jobReference']['projectId'],
                                              jobId=job['jobReference']['jobId']).execute()

        # logging stuff
        statistics = job['statistics']
        status = job['status']
        jobReference = job['jobReference']
        execution_time = (float(statistics['endTime']) - float(statistics['startTime']))/float(1000)

        logger.info('JobId %s status: %s' % (jobReference['jobId'], status['state']))
        logger.info('Total time to execute: %s' % execution_time)

        # Handle any errors in job
        if status.has_key('errorResult'):
            error_msg = 'ERRORS encountered in BigQuery JobId %s: \n %s' % (jobReference['jobId'], status['errorResult'])
            logger.error(error_msg)
            if error_callback:
                error_callback(error_msg)
        else:
            if statistics.has_key('query'):
                logger.info('totalBytesProcessed: %s ' % statistics['query']['totalBytesProcessed'])
                logger.info('cacheHit: %s ' % statistics['query']['cacheHit'])
        return job



# if __name__ == '__main__':
    # from cliquesadmin.jsonconfig import JsonConfigParser
    # from pymongo import MongoClient

    # config = JsonConfigParser()
    # client = MongoClient(config.get('ETL', 'mongodb', 'host'), config.get('ETL', 'mongodb', 'port'))
    # client.exchange.authenticate(config.get('ETL', 'mongodb', 'user'),
    #                              config.get('ETL', 'mongodb', 'pwd'),
    #                              source=config.get('ETL', 'mongodb', 'db'))
    # collection = client.exchange.test
    # opts = {'destinationTable':
    #             {
    #                 'datasetId': 'ad_events',
    #                 'projectId': cliques_bq_settings.PROJECT_ID,
    #                 'tableId': 'imp_matched_actions'
    #             },
    #         'createDisposition': 'CREATE_AS_NEEDED',
    #         'writeDisposition': 'WRITE_APPEND'
    #     }
    # etl = BigQueryIntermediateETL('imp_matched_actions.sql', cliques_bq_settings, query_options=opts)
    # result = etl.run(start=datetime(2015, 6, 20, 21, 0, 0), end=datetime(2015, 6, 20, 22, 0, 0), lookback=30)
    # print result