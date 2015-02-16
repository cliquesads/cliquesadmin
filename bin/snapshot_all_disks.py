import sys
import logging
import logging.config
from datetime import datetime
from cliquesadmin import logger
from cliquesadmin.gce_utils import authenticate_and_build
from cliquesadmin.gce_utils.disk_utils import get_all_disks, make_snapshot, purge_old_snapshots

SNAPSHOTS_TO_KEEP = 5

if __name__ == "__main__":

    start = datetime.utcnow()
    logger.info('Starting to create snapshots for all GCE disks at %s' % start)

    auth_http, gce_service = authenticate_and_build(sys.argv)
    k = get_all_disks(auth_http, gce_service)
    for d in k:
        resp = make_snapshot(auth_http, gce_service, d)
        logger.info('Snapshot Complete for disk %s ' % d)
        purge_old_snapshots(auth_http, gce_service,d,snapshots_to_keep=SNAPSHOTS_TO_KEEP)

    end = datetime.utcnow()
    logger.info('Finished creating snapshots for all GCE disks at %s' % end)
    logger.info('='*80)
