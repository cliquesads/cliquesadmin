import sys
import json
from datetime import datetime
from cliquesadmin.gce_utils import CliquesGCESettings, authenticate_and_build, _blocking_call

def get_all_disks(auth_http, gce_service, gce_settings=CliquesGCESettings):
    """
    Gets a list of all disk names for a project
    """
    # List disks
    request = gce_service.disks().list(project=gce_settings.PROJECT_ID,
                                       filter=None,
                                       zone=gce_settings.ZONE)
    response = request.execute(http=auth_http)
    if response and 'items' in response:
        all_disks = [o['name'] for o in response['items']]
        return all_disks

def make_snapshot(auth_http, gce_service, disk_name, gce_settings=CliquesGCESettings):

    snapshot_name = "%s-%s" % (disk_name, datetime.utcnow().strftime("%Y-%m-%d-%H-%M"))
    body = {'sourceDisk': disk_name,
            'name': snapshot_name}
    request = gce_service.disks().createSnapshot(project=gce_settings.PROJECT_ID,
                                                 disk=disk_name,
                                                 zone=gce_settings.ZONE,
                                                 body=body)
    response = request.execute(http=auth_http)
    return response


if __name__ == "__main__":
    auth_http, gce_service = authenticate_and_build(sys.argv)
    k = get_all_disks(auth_http, gce_service)
    resp = make_snapshot(auth_http, gce_service, k[0])
    response = _blocking_call(gce_service, auth_http, resp)
    print response