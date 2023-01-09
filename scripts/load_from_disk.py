import sys
sys.path.append('lib/python2.7/site-packages')
sys.path.append('thrift/gen-py')
sys.path.append('.')
from client.client import lsd_client

filename = sys.argv[1]
lsd = lsd_client()
n = lsd.tm.n_servers
print 'loading from {}_{}s{}.bin'.format(filename, n, range(1, n + 1))
print '->',
for sid in range(0, n):
  idx = sid + 1
  suffix = '_{}s{}.bin'.format(n, idx)
  lsd.tm.clients[sid].send_load_from_disk(filename + suffix)
  print '{}'.format(idx),
print ''
print '<-',
for sid in range(0, n):
  idx = sid + 1
  lsd.tm.clients[sid].recv_load_from_disk()
  print '{}'.format(idx),
