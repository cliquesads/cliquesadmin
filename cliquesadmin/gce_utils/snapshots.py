__author__ = 'bliang'
from cliquesadmin.gce_utils import *


def make_snapshot(gce_service, auth_http):

    # List instances
    request = gce_service.disks().list(project=PROJECT_ID, filter=None, zone=DEFAULT_ZONE)
    response = request.execute(http=auth_http)
    if response and 'items' in response:
        instances = response['items']
        for instance in instances:
            print instance['name']
    else:
        print 'No instances to list.'