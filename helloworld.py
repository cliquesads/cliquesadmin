#!/usr/bin/env python

import logging
import sys
import argparse
import httplib2
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.tools import run_flow

from googleapiclient.discovery import build

DEFAULT_ZONE = 'us-central1-a'
API_VERSION = 'v1'
GCE_URL = 'https://www.googleapis.com/compute/%s/projects/' % (API_VERSION)
PROJECT_ID = 'mimetic-codex-781'
CLIENT_SECRETS = 'client_secrets.json'
OAUTH2_STORAGE = 'oauth2.dat'
GCE_SCOPE = 'https://www.googleapis.com/auth/compute'

def main(argv):
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter,
    parents=[tools.argparser])

    # Parse the command-line flags.
    flags = parser.parse_args(argv[1:])

    # Perform OAuth 2.0 authorization.
    flow = flow_from_clientsecrets(CLIENT_SECRETS, scope=GCE_SCOPE)
    storage = Storage(OAUTH2_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, flags)
    http = httplib2.Http()
    auth_http = credentials.authorize(http)

    # Build the service
    gce_service = build('compute', API_VERSION)
    project_url = '%s%s' % (GCE_URL, PROJECT_ID)

    # List instances
    request = gce_service.instances().list(project=PROJECT_ID, filter=None, zone=DEFAULT_ZONE)
    response = request.execute(http=auth_http)
    if response and 'items' in response:
        instances = response['items']
        for instance in instances:
            print instance['name']
    else:
        print 'No instances to list.'


def _blocking_call(gce_service, auth_http, response):
    """Blocks until the operation status is done for the given operation."""

    status = response['status']
    while status != 'DONE' and response:
        operation_id = response['name']

    # Identify if this is a per-zone resource
    if 'zone' in response:
      zone_name = response['zone'].split('/')[-1]
      request = gce_service.zoneOperations().get(
          project=PROJECT_ID,
          operation=operation_id,
          zone=zone_name)
    else:
      request = gce_service.globalOperations().get(
           project=PROJECT_ID, operation=operation_id)

    response = request.execute(http=auth_http)
    if response:
      status = response['status']
    return response

if __name__ == '__main__':
    main(sys.argv)