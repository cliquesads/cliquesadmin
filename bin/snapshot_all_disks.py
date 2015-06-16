from datetime import datetime

from cliquesadmin import logger
from cliquesadmin.gce_utils import authenticate_and_build_jwt_client
from cliquesadmin.gce_utils.compute import compute_settings
from cliquesadmin.gce_utils.compute.disk import get_all_disks, make_snapshot, purge_old_snapshots


SNAPSHOTS_TO_KEEP = 5

if __name__ == "__main__":

    start = datetime.utcnow()
    logger.info('Starting to create snapshots for all GCE disks at %s' % start)

    gce_service = authenticate_and_build_jwt_client(compute_settings)
    k = get_all_disks(gce_service)
    for d in k:
        resp = make_snapshot(gce_service, d)
        logger.info('Snapshot Complete for disk %s ' % d)
        purge_old_snapshots(gce_service, d, snapshots_to_keep=SNAPSHOTS_TO_KEEP)

    end = datetime.utcnow()
    logger.info('Finished creating snapshots for all GCE disks at %s' % end)
    logger.info('='*80)
