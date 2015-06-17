import logging
from datetime import datetime
from time import mktime

from feedparser import _parse_date as parse_date
from cliquesadmin.gce_utils.compute import compute_settings
from cliquesadmin.gce_utils import blocking_call


logger = logging.getLogger(__name__)


def get_all_disks(gce_service, gce_settings=compute_settings):
    """
    Gets a list of all disk names for a project
    """
    # List disks
    request = gce_service.disks().list(project=gce_settings.PROJECT_ID,
                                       filter=None,
                                       zone=gce_settings.ZONE)
    response = request.execute()
    if response and 'items' in response:
        all_disks = [o['name'] for o in response['items']]
        return all_disks

@blocking_call
def make_snapshot(gce_service, disk_name, gce_settings=compute_settings):

    snapshot_name = "%s-%s" % (disk_name, datetime.utcnow().strftime("%Y-%m-%d-%H-%M"))
    body = {'sourceDisk': disk_name,
            'name': snapshot_name}
    logger.info('Creating snapshot for disk name %s...' % disk_name)
    request = gce_service.disks().createSnapshot(project=gce_settings.PROJECT_ID,
                                                 disk=disk_name,
                                                 zone=gce_settings.ZONE,
                                                 body=body)
    # logger.info('Successfully created snapshot for disk name %s' % disk_name)
    response = request.execute()
    return response


def _rfc3339_to_datetime(timestamp):
    stime = parse_date(timestamp)
    tfloat = mktime(stime)
    return datetime.fromtimestamp(tfloat)

@blocking_call
def delete_snapshot(gce_service, snapshot_name, gce_settings=compute_settings):
    logger.warn('DELETING SNAPSHOT %s, this is not reversible' % snapshot_name)
    request = gce_service.snapshots().delete(project=gce_settings.PROJECT_ID,
                                             snapshot=snapshot_name)
    response = request.execute()
    logger.info('Successfully deleted %s' % snapshot_name)
    return response


def purge_old_snapshots(gce_service, disk_name, snapshots_to_keep=3, gce_settings=compute_settings):

    assert snapshots_to_keep >= 1

    logger.info("Checking if any stale snapshots for disk %s exist (snapshots_to_keep=%s)"
                 % (disk_name, snapshots_to_keep))

    snapshot_name_re= "name eq '%s.*'" % disk_name
    request = gce_service.snapshots().list(project=gce_settings.PROJECT_ID,
                                           filter=snapshot_name_re)
    response = request.execute()
    snapshots = response['items']

    # prune older snapshots
    if len(snapshots) > snapshots_to_keep:
        logger.info('Found %s stale snapshots for disk %s, will now try to delete them' %
                    (len(snapshots)-snapshots_to_keep, disk_name))
        key = lambda k: _rfc3339_to_datetime(k['creationTimestamp'])
        snapshots = sorted(snapshots, key=key, reverse=True)
        old_snapshots = snapshots[snapshots_to_keep:]
        for old_snap in old_snapshots:
            delete_snapshot(gce_service, old_snap['name'], gce_settings=gce_settings)
    return True

# if __name__ == "__main__":
#     auth_http, gce_service = authenticate_and_build(sys.argv)
#     # purge_old_snapshots(auth_http, gce_service, 'mysql-server-a1')