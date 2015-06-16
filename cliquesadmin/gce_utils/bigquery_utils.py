import sys
import logging
from jinja2 import Environment, PackageLoader
# from datetime import datetime
# from time import mktime
# from feedparser import _parse_date as parse_date
from cliquesadmin.gce_utils import authenticate_and_build_with_service_account, cliques_bq_settings


env = Environment(loader=PackageLoader('cliquesadmin', 'bigquery'))

if __name__ == '__main__':
    gce_service = authenticate_and_build_with_service_account(cliques_bq_settings)
    template = env.get_template('hourlyadstats.sql')
    rendered = template.render()
    query_data = {'query': rendered}
    query_request = gce_service.jobs()
    query_response = query_request.query(projectId=cliques_bq_settings.PROJECT_ID,
                                         body=query_data).execute()
    print 'Query Results:'
    for row in query_response['rows']:
        result_row = []
        for field in row['f']:
            result_row.append(field['v'])
            print ('\t').join(result_row)



