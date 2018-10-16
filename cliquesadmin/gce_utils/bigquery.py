import numpy as np
from cliquesadmin.gce_utils import CliquesGCESettings
import pandas as pd


class CliquesBigQuerySettings(CliquesGCESettings):
    API_VERSION = 'v2'
    SCOPE = 'https://www.googleapis.com/auth/bigquery'
    API_NAME = 'bigquery'


cliques_bq_settings = CliquesBigQuerySettings()

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


