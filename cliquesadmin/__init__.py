import os
import sys
import logging
from datetime import datetime

this_dir = os.path.dirname(__file__)
sys.path.extend([this_dir])
REPOSITORY_PATH = os.path.abspath(os.path.join(this_dir,os.pardir))
CONFIG_PATH = os.path.join(REPOSITORY_PATH, 'config')
# LOGGING_CONFIG_FILE = os.path.join(REPOSITORY_PATH,'logging.conf')

#module-level logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.FileHandler(os.path.expanduser('~/logs/cliquesadmin_%s.log' %
                                            datetime.utcnow().strftime('%Y-%m-%d')))
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)