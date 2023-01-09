from lsdtypes.ttypes import *
from txnmanager import tm_handler
from common.config import config
import uuid

class TransactionException(Exception):
  pass

class fwrapper_t:
  def __init__(self):
    self.f = future_t(ftype_t.POINTER, fstate_t(fi = -1))

class lsd_client:
  NULL_FUTURE = fwrapper_t()

  def __init__(self):
    self.cid = uuid.uuid4()
    self.tid_ctr = 0
    cfg = config()
    self.twopl = cfg.is_twopl()
    self.slist = cfg.servers()
    self.tm = tm_handler(self.slist, cfg.sharding_policy(), self.twopl,
                         cfg.is_2pc_sequential())
    self.txn = txn_t(rwset = {}, future_wset = [], pset = [],
                     futures = futures_t(0, []))
    self.assume_predicates = cfg.assume_predicates
    self.assume_predicates_default = cfg.assume_predicates
  # def
  def begin(self):
    self.txn.rwset.clear()
    self.txn.future_wset = []
    self.txn.pset = []
    self.txn.futures.num_fi = 0
    del self.txn.futures.fmap[:]
    self.txn.tid = '{}-{}'.format(str(self.tid_ctr), self.cid) # - because it's ASCII value is < that numbers
  # def
  def get_notxn(self, key):
      rread = self.tm.get_notxn(key)
      return (rread.existed, rread.value)
  # def
  def get(self, key, lsd_api, for_update = False):
    assert lsd_api is True or lsd_api is False
    rwset = self.txn.rwset
    # simplify code by assuming:
    # a. no more than one get for a specific key per transaction
    # b. no get for a specific key if put before
    exists = key in rwset
    assert exists is False
    if lsd_api:
      # create future read
      fread = fread_t(False, key)
      future_read = future_t(ftype_t.READ, fstate_t(rdata = fread))
      res = self.make_future(future_read)
      # create future read pointer
      rstate = rstate_t(future_fi = res.u.fi)
      read = read_t(True, rstate)
      rwdata = rwdata_t(r = read)
      access = access_t(atype_t.READ, rwdata)
      rwset[key] = access
      return res
    else:
      rread = self.tm.get(self.txn.tid, key, for_update)
      if rread.status != vote_result_t.OK:
        assert self.twopl
        self.__retry('{}:get({},{},{})'.format(self.txn.tid, key, str(lsd_api),
                                               str(for_update)))
      rstate = rstate_t(concrete = rread)
      read = read_t(False, rstate)
      if self.twopl and for_update:
        wstate = wstate_t(value = rread.value)
        if rread.existed:
          wtype = wtype_t.PUT
        else:
          wtype = wtype_t.REMOVE
        write = write_t(wtype, False, wstate)
        readwrite = readwrite_t(read, write)
        rwdata = rwdata_t(rw = readwrite)
        access = access_t(atype_t.READ_WRITE, rwdata)
      else:
        rwdata = rwdata_t(r = read)
        access = access_t(atype_t.READ, rwdata)
      rwset[key] = access
      return (rread.existed, rread.value)
  # def
  def multiget_notxn(self, klist):
    kr_map = self.tm.multiget_notxn(klist)
    res = {}
    for key, rread in kr_map.iteritems():
      res[key] = (rread.existed, rread.value)
    return res
  # def
  def multiget(self, klist, lsd_api, for_update = False):
    assert lsd_api is True or lsd_api is False
    rwset = self.txn.rwset
    res = {}
    # simplify code by assuming:
    # a. no more than one get for a specific key per transaction
    # b. no get for a specific key if put before
    for k in klist:
      assert k not in rwset
    if lsd_api:
      for key in klist:
        res[key] = self.get(key, True)
    else:
      ok, kr_map = self.tm.multiget(self.txn.tid, klist, for_update)
      for key, rread in kr_map.iteritems():
        rstate = rstate_t(concrete = rread)
        read = read_t(False, rstate)
        if self.twopl and for_update:
          wstate = wstate_t(value = rread.value)
          if rread.existed:
            wtype = wtype_t.PUT
          else:
            wtype = wtype_t.REMOVE
          write = write_t(wtype, False, wstate)
          readwrite = readwrite_t(read, write)
          rwdata = rwdata_t(rw = readwrite)
          access = access_t(atype_t.READ_WRITE, rwdata)
        else:
          rwdata = rwdata_t(r = read)
          access = access_t(atype_t.READ, rwdata)
        rwset[key] = access
        res[key] = (rread.existed, rread.value)
      if not ok:
        assert self.twopl
        self.__retry('{}:multiget({},{},{})'.format(self.txn.tid, str(klist),
                                                    str(lsd_api),
                                                    str(for_update)))
    # if
    return res
  # def
  def is_true(self, predicate, assume = None):
    pset = self.txn.pset
    fmap = self.txn.futures.fmap
    assert predicate not in pset
    if self.twopl:
      ok, result = self.tm.is_true(self.txn, predicate)
      if not ok:
        self.__retry('{}:is_true'.format(self.txn.tid))
      return result
    else:
      if assume is not None and self.assume_predicates:
        assert not self.twopl
        assert assume is True or assume is False
        predicate.expected = assume
      else:
        klist = predicate.func.keys
        ok, kr_map = self.tm.multiget(self.txn.tid, klist, False)
        assert ok
        res = self.tm.resolve(self.txn, fmap[predicate.func.future_fi], kr_map,
                              transient = True)
        predicate.expected = res == 'True'
      pset.append(predicate)
      return predicate.expected
  # def
  def put(self, key, value, lsd_api, lsd_key = False, lsd_value = False):
    assert lsd_api is True or lsd_api is False
    assert lsd_api is (lsd_key or lsd_value)
    if lsd_key:
      assert lsd_api
      self.__write_lsd_key(key, wtype_t.PUT, value, lsd_value)
    else:
      self.__write(key, wtype_t.PUT, value, lsd_value)
  # def
  def remove(self, key, lsd_api):
    assert lsd_api is True or lsd_api is False
    value = '<removed>'
    if lsd_api:
      self.__write_lsd_key(key, wtype_t.REMOVE, value, False)
    else:
      self.__write(key, wtype_t.REMOVE, value, False)
  # def
  def __write(self, key, wtype, value, lsd_api):
    # simplify code by assuming no more than one put for a specific key
    # per transaction
    rwset = self.txn.rwset
    exists = key in rwset
    if not exists:
      if lsd_api:
        assert value.type == ftype_t.POINTER
        wstate = wstate_t(func_fi = value.u.fi)
        write = write_t(wtype, True, wstate)
      else:
        wstate = wstate_t(value = value)
        write = write_t(wtype, False, wstate)
      rwdata = rwdata_t(w = write)
      access = access_t(atype_t.WRITE, rwdata)
      rwset[key] = access
    else:
      access = rwset[key]
      rwdata = access.u
      atype = access.type
      if atype == atype_t.READ:
        access.type = atype_t.READ_WRITE
        read = rwdata.r
        if lsd_api:
          assert value.type == ftype_t.POINTER
          wstate = wstate_t(func_fi = value.u.fi)
          write = write_t(wtype, True, wstate)
        else:
          wstate = wstate_t(value = value)
          write = write_t(wtype, False, wstate)
        readwrite = readwrite_t(read, write)
        access.u = rwdata_t(rw = readwrite)
      else:
        assert atype == atype_t.READ_WRITE and self.twopl
        rwdata.rw.w.type = wtype
        if lsd_api:
          rwdata.rw.w.u.func_fi = value.u.fi
        else:
          rwdata.rw.w.u.value = value
    # if
  # def
  def __write_lsd_key(self, key_func, wtype, value, lsd_api):
    assert (lsd_api and value.type == ftype_t.POINTER) or not lsd_api
    if lsd_api:
      wstate = wstate_t(func_fi = value.u.fi)
      write = write_t(wtype, True, wstate)
    else:
      wstate = wstate_t(value = value)
      write = write_t(wtype, False, wstate)
    entry = fwset_entry_t(key_func, write)
    self.txn.future_wset.append(entry)
  # if
  def commit(self):
    vote = self.tm.commit(self.txn)
    if vote.result == vote_result_t.OK:
      self.__done()
      self.assume_predicates = self.assume_predicates_default
      fmap = self.txn.futures.fmap
      for fi, rread in vote.rfmap.iteritems():
        future_read = fmap[fi]
        assert future_read.type == ftype_t.READ
        fread = future_read.u.rdata
        if not fread.resolved:
          fread.data = rread
          fread.resolved = True
      for future in fmap:
        self.tm.resolve(self.txn, future)
    else:
      if vote.result == vote_result_t.PREDICATE_VALIDATION:
        self.assume_predicates = False
      self.__retry('{}:commit'.format(self.txn.tid), rollback = False)
  # def
  def abort(self):
    if self.twopl:
      self.tm.rollback(self.txn)
    self.__done()
  # def
  def __alloc_future(self):
    futures = self.txn.futures
    fi = futures.num_fi
    futures.num_fi += 1
    return future_t(ftype_t.POINTER, fstate_t(fi = fi))
  # def
  def make_future(self, future):
    futures = self.txn.futures
    res = self.__alloc_future()
    fi = res.u.fi
    futures.fmap.append(future)
    assert len(futures.fmap) == futures.num_fi
    return res
  # def
  def __retry(self, why, rollback = True):
    if rollback:
      assert self.twopl
      self.tm.rollback(self.txn)
    raise TransactionException(why)
  # def
  def __done(self):
    self.tid_ctr += 1
  # def
# class
