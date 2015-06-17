import logging
import numpy as np
import pandas as pd
from jinja2 import Environment, PackageLoader
from datetime import datetime
# from time import mktime
# from feedparser import _parse_date as parse_date
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


class BigQueryMongoETL(object):

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
        # load into dataframe
        dataframe = query_response_to_dataframe(query_response)
        return dataframe

    def transform(self, dataframe):
        return dataframe

    def load(self, dataframe):
        # bulk insert into Mongo here
        pass

    def run(self, **kwargs):
        dataframe = self.extract(**kwargs)
        dataframe = self.transform(dataframe)
        self.load(dataframe)

if __name__ == '__main__':
    from cliquesadmin.jsonconfig import JsonConfigParser
    from pymongo import MongoClient

    config = JsonConfigParser()
    client = MongoClient(config.get('ETL', 'mongodb', 'host'), config.get('ETL', 'mongodb', 'port'))
    client.exchange.authenticate(config.get('ETL', 'mongodb', 'user'),
                                 config.get('ETL', 'mongodb', 'pwd'),
                                 source=config.get('ETL', 'mongodb', 'db'))
    collection = client.exchange.advertisers
    etl = BigQueryMongoETL('hourlyadstats.sql', cliques_bq_settings, collection)
    df = etl.extract(start=datetime(2015, 6, 1, 0, 0, 0), end=datetime(2015, 6, 15, 0, 0, 0))
    print df