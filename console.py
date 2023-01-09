import sys
sys.path.append('lib/python2.7/site-packages')
sys.path.append('thrift/gen-py')
sys.path.append('.')
from client.client import lsd_client
from lsdtypes.ttypes import *
txn = lsd_client()
