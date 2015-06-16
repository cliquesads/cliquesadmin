#!/usr/bin/env python
import logging
import os
import argparse
import httplib2
import json
from time import sleep
from oauth2client.client import flow_from_clientsecrets, SignedJwtAssertionCredentials
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.tools import run_flow
from googleapiclient.discovery import build
from functools import wraps
from cliquesadmin import REPOSITORY_PATH, CONFIG_PATH

logger = logging.getLogger(__name__)


class CliquesGCESettings:
    # TODO: Zone is not static, need to handle multiple zones
    ZONE = 'us-central1-a'
    PROJECT_ID = 'mimetic-codex-781'
    JWT_SECRETS = os.path.join(CONFIG_PATH, 'google', 'jwt.json')
    API_VERSION = None
    API_NAME = None
    SCOPE = None
    CLIENT_SECRETS = None
    OAUTH2_STORAGE = None


def authenticate_and_build_jwt_client(gce_settings):
    f = file(gce_settings.JWT_SECRETS, 'rb')
    secrets = json.load(f)
    f.close()
    key = secrets['private_key']
    email = secrets['client_email']
    credentials = SignedJwtAssertionCredentials(
        email,
        key,
        scope=gce_settings.SCOPE
    )
    http = httplib2.Http()
    http = credentials.authorize(http)
    service = build(gce_settings.API_NAME, gce_settings.API_VERSION, http=http)
    return service

def authenticate_and_build_oauth(argv, gce_settings):
    """
    Authenticates with OAuth 2.0 credentials, if present, and builds GCE API Service obj

    Provides HTTP client to service client, so specifying http=auth_http in request.execute()
    is unnecessary.

    :return: gce_service
    """
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])

    # Parse the command-line flags.
    flags = parser.parse_args(argv[1:])

    # Perform OAuth 2.0 authorization.
    flow = flow_from_clientsecrets(gce_settings.CLIENT_SECRETS, scope=gce_settings.SCOPE)
    storage = Storage(gce_settings.OAUTH2_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage, flags)
    http = httplib2.Http()
    auth_http = credentials.authorize(http)

    # Build the service
    gce_service = build(gce_settings.API_NAME, gce_settings.API_VERSION, http=auth_http)

    return gce_service


def blocking_call(func):
    """
    Blocks until the operation status is done for the given operation.

    Functions decorated must have the following signature::

    def some_api_call(gce_service, *args, gce_settings=cliques_gce_settings, **kwargs)
        ...

    :param gce_service: built Google Compute Engine API service
    :param gce_settings: GCESettings object
    """
    @wraps(func)
    def wrapper(*args, **kwargs):

        # wrapper stuff
        # auth_http = args[0]
        gce_service = args[0]
        if kwargs:
            gce_settings = kwargs['gce_settings']
        else:
            gce_settings = filter(lambda k: issubclass(k.__class__, CliquesGCESettings), func.func_defaults)
            if not gce_settings:
                raise Exception("You must specify a GCESettings object to use as function kwarg")
            elif len(gce_settings) > 1:
                raise TypeError("Found two GCESettings kwarg defaults, you can only specify one")
            else:
                gce_settings = gce_settings[0]
        response = func(*args, **kwargs)
        status = response['status']

        # wait for status to be DONE, check every 5 seconds
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

            # response = request.execute(http=auth_http)
            response = request.execute()
            if response:
                status = response['status']
            sleep(5) # sleep for 5 seconds to avoid unnecessary API calls

        # log any errors that came back
        if response.has_key('error'):
            logger.warn('Operation completed with errors:')
            for error in response['error']['errors']:
                logger.warn('Error Code: %s -- %s' % (error['code'], error['message']))
        else:
            logger.info('Successfully completed API operation with no errors.')
        return response
    return wrapper



# if __name__ == '__main__':
#     main(sys.argv)
