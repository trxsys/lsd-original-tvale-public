from lsdtypes.ttypes import *

class sharding_policy:
  def get_server(self, key, n_servers, fmap = {}):
    raise NotImplementedError

class sharding_hash(sharding_policy):
  def get_server(self, key, n_servers, fmap = {}):
    if n_servers == 1:
      return 0
    elif isinstance(key, future_t):
      return None
    else:
      return hash(key) % n_servers

# shards everything by warehouse id (w_id) except item and history, which are
# sharded by item id (i_id) and client (h_c_id), resp.
class sharding_tpcc(sharding_policy):
  def get_server(self, key, n_servers, fmap = {}):
    if n_servers == 1:
      return 0
    if isinstance(key, future_t):
      #             __ + __
      #            /       \
      #        __ + __    'suffix'
      #       /       \
      #  'table-'   {future}
      assert key.type == ftype_t.CONCAT_FS
      bin_op = key.u.binaryop
      tail = bin_op.right.value
      future = fmap[bin_op.left.fi]
      assert future.type == ftype_t.CONCAT_SF
      bin_op = future.u.binaryop
      table = bin_op.left.value
      table, s, t = table.partition('-')
    else:
      table, sep, tail = key.partition('-')
    if table == 'w':
      # tail = wid-field
      wid, s, t = tail.partition('-')
      return int(wid) % n_servers
    elif table == 'd':
      # tail = did_wid-field
      h, s, t = tail.partition('-')
      did, s, wid = h.partition('_')
      return int(wid) % n_servers
    elif table == 'c':
      # tail = cid_cdid_cwid-field
      h, s, t = tail.partition('-')
      h, s, wid = h.rpartition('_')
      return int(wid) % n_servers
    elif table == 'ci':
      # tail = cdid_cwid_clast
      did, s, t = tail.partition('_')
      wid, s, last = t.partition('_')
      return int(wid) % n_servers
    elif table == 'h':
      # tail = huuid_hcid_hcwid_hwid-field
      h, s, t = tail.rpartition('-')
      h, s, wid = h.rpartition('_')
      return int(wid) % n_servers
    elif table == 'no':
      # tail = (nooid)_nodid_nowid-field
      h, s, field = tail.partition('-')
      h, s, wid = h.rpartition('_')
      return int(wid) % n_servers
    elif table == 'noi':
      # tail = nodid_nowid
      did, s, wid = tail.partition('_')
      return int(wid) % n_servers
    elif table == 'o':
      # tail = (oid)_odid_owid-field
      h, s, field = tail.partition('-')
      h, s, wid = h.rpartition('_')
      return int(wid) % n_servers
    elif table == 'oi':
      # tail = odid_owid_ocid
      did, s, t = tail.partition('_')
      wid, s, cid = t.partition('_')
      return int(wid) % n_servers
    elif table == 'ol':
      # tail = (olnum_oloid)_oldid_olwid-field
      h, s, field = tail.partition('-')
      h, s, wid = h.rpartition('_')
      return int(wid) % n_servers
    elif table == 'i':
      # tail = iid-field
      iid, s, t = tail.partition('-')
      return int(iid) % n_servers
    elif table == 's':
      # tail = siid_swid-field
      h, s, t = tail.partition('-')
      iid, s, wid = h.partition('_')
      return int(wid) % n_servers
    else:
      raise Exception('Unknown key')
