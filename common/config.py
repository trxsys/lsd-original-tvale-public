from ConfigParser import RawConfigParser
from sharding import *

class config:
  def __init__(self):
    self.clist = []
    self.cthreads = 0
    self.slist = []
    self.bhost = None
    self.bport = None
    parser = RawConfigParser()
    parser.read('config.ini')
    # barrier
    self.bhost = parser.get('barrier', 'host')
    self.bport = parser.get('barrier', 'port')
    # servers
    servers = parser.get('server', 'instances').split()
    for s in servers:
      host, port = s.split(':')
      self.slist.append((host, port))
    # clients
    clients = parser.get('ycsb', 'instances').split()
    for c in clients:
      host, threads = c.split(':')
      self.clist.append((host, threads))
      self.cthreads += int(threads)
    # sharding
    sharding = parser.get('python', 'sharding')
    if sharding == 'hash':
      self.sharding_impl = sharding_hash()
    elif sharding == 'tpcc':
      self.sharding_impl = sharding_tpcc()
    else:
      raise Exception('Unknown sharding policy')
    # concurrency control
    ccontrol = parser.get('python', 'ccontrol')
    if ccontrol == 'occ':
      self.twopl = False
    elif ccontrol == '2pl':
      self.twopl = True
    else:
      raise Exception('Unknown concurrency control protocol')
    # lsd interface
    assume_predicates = parser.get('python', 'assume_predicates')
    if assume_predicates in ('yes', 'Yes', 'y', 'Y', '1', 'true', 'True'):
      self.assume_predicates = True
    else:
      self.assume_predicates = False
    twopc_sequential = parser.get('python', '2pc_sequential')
    if twopc_sequential in ('yes', 'Yes', 'y', 'Y', '1', 'true', 'True'):
      self.twopc_sequential = True
    else:
      self.twopc_sequential = False

  def clients(self):
    return self.clist

  def threads(self, client_id):
    return self.clist[client_id][1]

  def total_threads(self):
    return self.cthreads

  def global_id(self, client_id, thread_id):
    id = 0
    for i in range(0, client_id):
      id += self.clist[i][1]
    return id

  def servers(self):
    return self.slist

  def barrier_host(self):
    return self.bhost

  def barrier_port(self):
    return self.bport

  def sharding_policy(self):
    return self.sharding_impl

  def is_twopl(self):
    return self.twopl

  def is_2pc_sequential(self):
    return self.twopc_sequential
