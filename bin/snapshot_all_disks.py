import sys
import logging
import logging.config
import os
from datetime import datetime
from cliquesadmin import logger
from cliquesadmin.gce_utils import authenticate_and_build
from cliquesadmin.gce_utils.disk_utils import get_all_disks, make_snapshot, purge_old_snapshots

# logging.config.fileConfig(LOGGING_CONFIG_FILE,
#                           defaults={'logfilename': os.path.expanduser('~/logs/gce_snapshot.log')})

# logfile = os.path.expanduser('~/logs/gce_snapshot.log')
# logging.basicConfig(filename=logfile, level=logging.DEBUG)

if __name__ == "__main__":
    # logging.basicConfig(filename=logfile, level=logging.DEBUG)

    start = datetime.utcnow()
    logger.info('Starting to create snapshots for all GCE disks at %s' % start)

    auth_http, gce_service = authenticate_and_build(sys.argv)
    k = get_all_disks(auth_http, gce_service)
    for d in k:
        resp = make_snapshot(auth_http, gce_service, d)
        logging.info('Snapshot Complete for disk %s ' % d)
        purge_old_snapshots(auth_http, gce_service,d)

    end = datetime.utcnow()
    logger.info('Finished creating snapshots for all GCE disks at %s' % end)
    logger.info('='*80)
