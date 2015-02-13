import sys
from datetime import datetime
from . import cliques_gce_settings, authenticate_and_build, blocking_call

def get_all_disks(auth_http, gce_service, gce_settings=cliques_gce_settings):
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

@blocking_call
def make_snapshot(auth_http, gce_service, disk_name, gce_settings=cliques_gce_settings):

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
    for d in k:
        resp = make_snapshot(auth_http, gce_service, d)
        print resp