import logging
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
    def __init__(self, template, gce_settings, mongo_collection):
        self.gce_settings = gce_settings
        self.gce_service = authenticate_and_build_jwt_client(gce_settings)
        self.template = jinja_bq_env.get_template(template)
        self.mongo_collection = mongo_collection

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

        rendered = self.template.render(**kwargs)
        query_data = {'query': rendered}
        # TODO: Save response to Cloud Storage as backup?
        query_request = self.gce_service.jobs()
        query_response = query_request.query(projectId=cliques_bq_settings.PROJECT_ID,
                                             body=query_data).execute()

        logger.info('Query completed, %s rows returned by jobId %s' %
                    (query_response['totalRows'], query_response['jobReference']['jobId']))

        if int(query_response['totalRows']) > 0:
            # load into dataframe
            dataframe = query_response_to_dataframe(query_response)
            logger.info('Loaded query result to DataFrame')
            return dataframe
        else:
            return None

    def transform(self, dataframe):
        """
        Hook for subclasses to do any necessary transformation
        of raw query output before inserting into MongoDB.

        Base class just passes dataframe right through.

        :param dataframe:
        :return:
        """
        return dataframe

    def load_to_mongo(self, dataframe):
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

        result = self.mongo_collection.insert_many(records)
        return result

    def run(self, **kwargs):
        dataframe = self.extract(**kwargs)
        if dataframe is not None:
            dataframe = self.transform(dataframe)
            result = self.load_to_mongo(dataframe)
            return result
        else:
            return None

# if __name__ == '__main__':
#     from cliquesadmin.jsonconfig import JsonConfigParser
#     from pymongo import MongoClient
#
#     config = JsonConfigParser()
#     client = MongoClient(config.get('ETL', 'mongodb', 'host'), config.get('ETL', 'mongodb', 'port'))
#     client.exchange.authenticate(config.get('ETL', 'mongodb', 'user'),
#                                  config.get('ETL', 'mongodb', 'pwd'),
#                                  source=config.get('ETL', 'mongodb', 'db'))
#     collection = client.exchange.test
#     etl = BigQueryMongoETL('hourlyadstats.sql', cliques_bq_settings, collection)
#     result = etl.run(start=datetime(2015, 6, 1, 0, 0, 0), end=datetime(2015, 6, 15, 0, 0, 0))
#     print result