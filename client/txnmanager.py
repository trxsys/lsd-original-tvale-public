from lsdtypes.ttypes import *
from txn_manager import txn_manager as tm
from data_manager import data_manager as dm
from thrift.transport.THttpClient import THttpClient
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

class tm_handler(tm.Iface):
  def __init__(self, servers, sharding, twopl, twopc_sequential):
    self.sharding = sharding
    self.twopl = twopl
    self.twopc_sequential = twopc_sequential
    self.clients = []
    self.transports = []
    self.n_servers = len(servers)
    for (host, port) in servers:
      uri = 'http://{}:{}/'.format(host, port)
      transport = THttpClient(uri)
      protocol = TBinaryProtocol(transport)
      client = dm.Client(protocol)
      self.clients.append(client)
      transport.open()
      self.transports.append(transport)
    # for
    assert(len(self.clients) == self.n_servers)
    assert(len(self.transports) == self.n_servers)
    # setup operations vtable
    self.ops = [None] * ftype_t.TOTAL
    def read_op(self, txn, future, kr_map, transient):
      fread = future.u.rdata
      if fread.resolved:
        rread = fread.data
        if rread.existed:
          return rread.value
        else:
          return None
      key = fread.key
      assert key in kr_map
      rread = kr_map[key]
      fread.data = rread
      if not transient:
        fread.resolved = True
      if rread.existed:
        return rread.value
      else:
        return None
    self.ops[ftype_t.READ] = read_op
    def pointer_op(self, txn, future, kr_map, transient):
      fi = future.u.fi
      pointee = txn.futures.fmap[fi]
      return self.ops[pointee.type](self, txn, pointee, kr_map, transient)
    self.ops[ftype_t.POINTER] = pointer_op
    def add_fi_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = int(left) + int(right)
        bin_op.value = str(result)
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.ADD_FI] = add_fi_op
    def add_fd_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = float(left) + float(right)
        bin_op.value = str(result)
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.ADD_FD] = add_fd_op
    def sub_fi_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = int(left) - int(right)
        bin_op.value = str(result)
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.SUB_FI] = sub_fi_op
    def sub_fd_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = float(left) - float(right)
        bin_op.value = str(result)
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.SUB_FD] = sub_fd_op
    def concat_fs_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = left + right
        bin_op.value = result
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.CONCAT_FS] = concat_fs_op
    def concat_sf_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left = bin_op.left.value
        right_f = fmap[bin_op.right.fi]
        right = self.ops[right_f.type](self, txn, right_f, kr_map, transient)
        result = left + right
        bin_op.value = result
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.CONCAT_SF] = concat_sf_op
    def gteq_fi_op(self, txn, future, kr_map, transient):
      bin_op = future.u.binaryop
      if bin_op.resolved:
        return bin_op.value
      else:
        fmap = txn.futures.fmap
        left_f = fmap[bin_op.left.fi]
        left = self.ops[left_f.type](self, txn, left_f, kr_map, transient)
        right = bin_op.right.value
        result = int(left) >= int(right)
        bin_op.value = str(result)
        if not transient:
          bin_op.resolved = True
        return bin_op.value
    self.ops[ftype_t.GTEQ_FI] = gteq_fi_op
    def none_op(self, txn, future, kr_map, transient):
      raise NotImplementedError('resolve future type {}'.format(ftype))
    self.ops[ftype_t.NONE] = none_op
  # def
  def get_notxn(self, key):
    """
    Parameters:
     - key
    """
    server_id = self.sharding.get_server(key, self.n_servers)
    rread = self.clients[server_id].get_notxn(key)
    return rread
  # def
  def get(self, tid, key, for_update):
    """
    Parameters:
     - tid
     - key
    """
    if not self.twopl:
      for_update = False
    server_id = self.sharding.get_server(key, self.n_servers)
    rread = self.clients[server_id].get(tid, key, for_update)
    return rread
  # def
  def multiget_notxn(self, klist):
    """
    Parameters:
     - klist
    """
    kr_map = {}
    server_keys = {}
    klist.sort()
    for key in klist:
      server_id = self.sharding.get_server(key, self.n_servers)
      exists = server_id in server_keys
      if not exists:
        server_keys[server_id] = []
      server_keys[server_id].append(key)
    for server_id, keys in server_keys.iteritems():
      self.clients[server_id].send_multiget_notxn(keys)
    for server_id in server_keys.iterkeys():
      mg_return = self.clients[server_id].recv_multiget_notxn()
      kr_map.update(mg_return.mgmap)
    return kr_map
  # def
  def multiget(self, tid, klist, for_update):
    """
    Parameters:
     - tid
     - klist
    """
    if not self.twopl:
      for_update = False
    global_ok = True
    kr_map = {}
    server_keys = {}
    klist.sort()
    for key in klist:
      server_id = self.sharding.get_server(key, self.n_servers)
      exists = server_id in server_keys
      if not exists:
        server_keys[server_id] = []
      server_keys[server_id].append(key)
    for server_id, keys in server_keys.iteritems():
      self.clients[server_id].send_multiget(tid, keys, for_update)
    for server_id in server_keys.iterkeys():
      mg_return = self.clients[server_id].recv_multiget()
      kr_map.update(mg_return.mgmap)
      global_ok = global_ok and mg_return.status == vote_result_t.OK
    return (global_ok, kr_map)
  # def
  def is_true(self, txn, predicate):
    """
    Parameters:
     - tid
     - predicate
     - fmap
    """
    assert self.twopl
    server_ids = set()
    for key in predicate.func.keys:
      server_id = self.sharding.get_server(key, self.n_servers)
      server_ids.add(server_id)
    assert len(server_ids) == 1
    result = self.clients[server_id].opc_assert(txn.tid, predicate, txn.futures.fmap)
    if result.status == vote_result_t.OK:
      return (True, result.result)
    else:
      return (False, None)
  # def
  def commit(self, txn):
    """
    Parameters:
     - txn
    """
    if len(txn.rwset) == 0:
      assert len(txn.future_wset) == 0
      return vote_t(vote_result_t.OK, {})
    servers = {}
    fwset = []
    for key, access in txn.rwset.iteritems():
      server_id = self.sharding.get_server(key, self.n_servers)
      exists = server_id in servers
      if not exists:
        servers[server_id] = txn_t(rwset = {}, future_wset = [], pset = [],
                                   futures = txn.futures)
      servers[server_id].rwset[key] = access
    for entry in txn.future_wset:
      key_func = entry.func
      key_f = txn.futures.fmap[key_func.future_fi]
      server_id = self.sharding.get_server(key_f, self.n_servers, txn.futures.fmap)
      server_known = server_id is not None
      if server_known:
        klist = key_func.keys
        server_can_resolve = True
        for k in klist:
          server_dep = self.sharding.get_server(k, self.n_servers)
          server_can_resolve = server_can_resolve and server_id == server_dep
        if server_can_resolve:
          if server_id not in servers:
            servers[server_id] = txn_t(rwset = {}, future_wset = [], pset = [],
                                       futures = txn.futures)
          servers[server_id].future_wset.append(entry)
        else:
          fwset.append(entry)
      else:
        fwset.append(entry)
    assert len(servers) >= 1
    single_partition = len(servers) == 1
    server_id = servers.keys()[0]
    server_txn = servers[server_id]
    resolvable_fwset = len(server_txn.future_wset) == len(txn.future_wset)
    if single_partition and resolvable_fwset:
      # one-phase commit, single-partition, transaction (no unknown wset keys)
      assert len(fwset) == 0
      vote = self.clients[server_id].opc_commit(txn)
    else:
      # two-phase commit, potentially multi-partition, transaction
      # sort servers
      server_ids = sorted(servers)
      # prepare
      if self.twopl:
        prepared = server_ids
      else:
        prepared = []
      vote = vote_t(vote_result_t.OK, {})
      if not self.twopc_sequential:
        for server_id in server_ids:
          server_txn = servers[server_id]
          server_txn.tid = txn.tid
          self.clients[server_id].send_tpc_prepare(server_txn)
        for server_id in server_ids:
          server_vote = self.clients[server_id].recv_tpc_prepare()
          if server_vote.result == vote_result_t.OK:
            if not self.twopl:
              prepared.append(server_id)
            if vote.result == vote_result_t.OK:
              vote.rfmap.update(server_vote.rfmap)
          else:
            vote.result = server_vote.result
      else:
        for server_id in server_ids:
          server_txn = servers[server_id]
          server_txn.tid = txn.tid
          server_vote = self.clients[server_id].tpc_prepare(server_txn)
          if server_vote.result == vote_result_t.OK:
            if not self.twopl:
              prepared.append(server_id)
            vote.rfmap.update(server_vote.rfmap)
          else:
            vote.result = server_vote.result
            break
      # if
      if vote.result == vote_result_t.OK:
        self.__resolve_reads(txn, vote.rfmap)
        # prepare ok
        if len(fwset) > 0:
          # prepare2
          prepared = set(prepared)
          servers_fwset = {}
          for entry in fwset:
            key_func = entry.func
            key_f = txn.futures.fmap[key_func.future_fi]
            key = self.resolve(txn, key_f)
            server_id = self.sharding.get_server(key, self.n_servers)
            assert server_id is not None
            if server_id not in servers_fwset:
              servers_fwset[server_id] = txn_t(rwset = {}, future_wset = [],
                                               pset = [],
                                               futures = txn.futures)
            servers_fwset[server_id].future_wset.append(entry)
          for server_id, server_txn in servers_fwset.iteritems():
            server_txn.tid = txn.tid
            self.clients[server_id].send_tpc_prepare2(server_txn, vote.rfmap)
          for server_id in servers_fwset.iterkeys():
            server_vote = self.clients[server_id].recv_tpc_prepare2()
            if server_vote.result == vote_result_t.OK:
              prepared.add(server_id)
            else:
              vote.result = server_vote.result
          if vote.result != vote_result_t.OK:
            # prepare ok, prepare2 fail, abort
            for server_id in prepared:
              self.clients[server_id].send_tpc_abort(txn.tid)
            for server_id in prepared:
              self.clients[server_id].recv_tpc_abort()
            return vote
        # prepare ok, (prepare2 ok)
        if len(txn.pset) > 0:
          for pred in txn.pset:
            fi = pred.func.future_fi
            future = txn.futures.fmap[fi]
            expected = pred.expected
            result = self.resolve(txn, future)
            pred_ok = result == str(expected)
            if not pred_ok:
              vote.result = vote_result_t.PREDICATE_VALIDATION
              break
          if vote.result != vote_result_t.OK:
            # prepare ok, (prepare2 ok,) predicates fail, abort
            for server_id in prepared:
              self.clients[server_id].send_tpc_abort(txn.tid)
            for server_id in prepared:
              self.clients[server_id].recv_tpc_abort()
            return vote
        # prepare ok, (prepare2 ok, predicates ok,) commit
        for server_id in prepared:
          self.clients[server_id].send_tpc_commit(txn.tid, vote.rfmap)
        for server_id in prepared:
          self.clients[server_id].recv_tpc_commit()
      else:
        # prepare fail, abort
        for server_id in prepared:
          self.clients[server_id].send_tpc_abort(txn.tid)
        for server_id in prepared:
          self.clients[server_id].recv_tpc_abort()
    return vote
  # def
  def rollback(self, txn):
    assert self.twopl
    if len(txn.rwset) == 0:
      assert len(txn.future_wset) == 0
      return
    servers = set()
    for key, access in txn.rwset.iteritems():
      atype = access.type
      if atype == atype_t.READ or atype == atype_t.READ_WRITE:
        server_id = self.sharding.get_server(key, self.n_servers)
        servers.add(server_id)
    for server_id in servers:
      self.clients[server_id].send_tpc_abort(txn.tid)
    for server_id in servers:
      self.clients[server_id].recv_tpc_abort()
  # def
  def __resolve_reads(self, txn, rfmap):
    fmap = txn.futures.fmap
    for fi, rread in rfmap.iteritems():
      future_read = fmap[fi]
      assert future_read.type == ftype_t.READ
      fread = future_read.u.rdata
      assert not fread.resolved
      fread.data = rread
      fread.resolved = True
  # def
  def resolve(self, txn, future, kr_map = {}, transient = False):
    ftype = future.type
    return self.ops[ftype](self, txn, future, kr_map, transient)
  # def
# class
