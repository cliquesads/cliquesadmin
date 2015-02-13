#!/usr/bin/env python
import logging
import os
import argparse
import httplib2
from time import sleep
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.tools import run_flow
from googleapiclient.discovery import build
from functools import wraps
from cliquesadmin import REPOSITORY_PATH

class GCESettings:
    ZONE = 'us-central1-a'
    API_VERSION = 'v1'
    GCE_URL = 'https://www.googleapis.com/compute/%s/projects/' % (API_VERSION)
    GCE_SCOPE = 'https://www.googleapis.com/auth/compute'
    PROJECT_ID = None
    CLIENT_SECRETS = None
    OAUTH2_STORAGE = None

class CliquesGCESettings(GCESettings):
    PROJECT_ID = 'mimetic-codex-781'
    CLIENT_SECRETS = os.path.join(REPOSITORY_PATH,'client_secrets.json')
    OAUTH2_STORAGE = os.path.join(REPOSITORY_PATH,'oauth2.dat')

cliques_gce_settings = CliquesGCESettings()

def authenticate_and_build(argv, gce_settings=cliques_gce_settings):
    """
    Authenticates with OAuth 2.0 credentials, if present, and builds GCE API Service obj

    :return: auth_http, gce_service
    """
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])

    # Parse the command-line flags.
    flags = parser.parse_args(argv[1:])

    # Perform OAuth 2.0 authorization.
    flow = flow_from_clientsecrets(gce_settings.CLIENT_SECRETS, scope=gce_settings.GCE_SCOPE)
    storage = Storage(gce_settings.OAUTH2_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, flags)
    http = httplib2.Http()
    auth_http = credentials.authorize(http)

    # Build the service
    gce_service = build('compute', gce_settings.API_VERSION)
    # project_url = '%s%s' % (GCE_URL, PROJECT_ID)

    return auth_http, gce_service

def blocking_call(func):
    """
    Blocks until the operation status is done for the given operation.

    Functions decorated must have the following signature::

    def some_api_call(auth_http, gce_service, *args, gce_settings=cliques_gce_settings, **kwargs)
        ...

    :param auth_http: Oauth2.0 authorized HTTP object
    :param gce_service: built Google Compute Engine API service
    :param gce_settings: GCESettings object
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_http = args[0]
        gce_service = args[1]
        if kwargs:
            gce_settings = kwargs['gce_settings']
        else:
            gce_settings = filter(lambda k: issubclass(k.__class__, GCESettings), func.func_defaults)
            if not gce_settings:
                raise Exception("You must specify a GCESettings object to use as function kwarg")
            elif len(gce_settings) > 1:
                raise TypeError("Found two GCESettings kwarg defaults, you can only specify one")
            else:
                gce_settings = gce_settings[0]
        response = func(*args, **kwargs)
        status = response['status']
        while status != 'DONE' and response:
            operation_id = response['name']

            # Identify if this is a per-zone resource
            if 'zone' in response:
              zone_name = response['zone'].split('/')[-1]
              request = gce_service.zoneOperations().get(
                  project=gce_settings.PROJECT_ID,
                  operation=operation_id,
                  zone=zone_name)
            else:
              request = gce_service.globalOperations().get(
                   project=gce_settings.PROJECT_ID, operation=operation_id)

            response = request.execute(http=auth_http)
            if response:
              status = response['status']
            sleep(5) #sleep for 5 seconds to avoid unnecessary API calls
        return status
    return wrapper

# if __name__ == '__main__':
#     main(sys.argv)
