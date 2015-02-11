import sys
from cliquesadmin.gce_utils import CliquesGCESettings, authenticate_and_build

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

if __name__ == "__main__":
    auth_http, gce_service = authenticate_and_build(sys.argv)
    k = get_all_disks(auth_http, gce_service)
    print k