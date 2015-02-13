import sys
import logging
import os
from datetime import datetime
from cliquesadmin.gce_utils import authenticate_and_build
from cliquesadmin.gce_utils.disk_utils import get_all_disks, make_snapshot, purge_old_snapshots

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        filename=os.path.expanduser('~/logs/gce_snapshot.log'))
    start = datetime.utcnow()
    logging.info('Starting to create snapshots for all GCE disks at %s' % start)

    auth_http, gce_service = authenticate_and_build(sys.argv)
    k = get_all_disks(auth_http, gce_service)
    for d in k:
        resp = make_snapshot(auth_http, gce_service, d)
        logging.info('Snapshot Complete')
        purge_old_snapshots(auth_http, gce_service,d)

    end = datetime.utcnow()
    logging.info('Finished creating snapshots for all GCE disks at %s' % datetime.utcnow())
    logging.info('Time elapsed: %s' % end-start)
    logging.info('')
