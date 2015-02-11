import os
import sys

this_dir = os.path.dirname(__file__)
sys.path.extend([this_dir])
REPOSITORY_PATH = os.path.abspath(os.path.join(this_dir,os.pardir))