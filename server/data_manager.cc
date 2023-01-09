//
//  data_manager.cc
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "data_manager.hh"
#include "../thrift/gen-cpp/data_manager.h"
#include "../thrift/gen-cpp/types_types.h"
#include <cassert>
#include <cinttypes>
#include <cstdlib>
#include <fstream>
#include <iostream>

#ifdef USE_BATCH_WRITES
#include <vector>
#endif

namespace novalincs {
namespace cs {
namespace lsd {

dm_handler::dm_handler(std::string& storage_impl, std::string& id)
  : storage(StorageFactory::create_storage(storage_impl, id))
{
  // setup operations vtable
  ops[ftype_t::READ] = [this](fmap_t& fmap, future_t& future,
                              bool const transient,
                              env_t const& env) -> value_t const* {
    auto& fread = future.u.rdata;
    auto& rread = fread.data;
    auto env_pair = env.find(fread.key);
    if (env_pair != env.end()) {
      DASSERT(transient);
      return &env_pair->second;
    } else if (fread.resolved) {
      if (rread.existed) {
        return &rread.value;
      } else {
        std::cerr
          << "ABORT: Attempting to resolve future of a non-existant key = "
          << fread.key << std::endl;
        std::abort();
        return nullptr;
      }
    } else {
      DASSERT(transient);
      Result res = storage->get(fread.key);
      DASSERT(res.exists);
      rread.value = res.value;
      return &rread.value;
    }
  };
  ops[ftype_t::POINTER] = [this](fmap_t& fmap, future_t& future,
                                 bool const transient,
                                 env_t const& env) -> value_t const* {
    auto const& fi = future.u.fi;
    auto& pointee = fmap[fi];
    return ops[pointee.type](fmap, pointee, transient, env);
  };
  ops[ftype_t::ADD_FI] = [this](fmap_t& fmap, future_t& future,
                                bool const transient,
                                env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      int const result = std::stoi(*left) + std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::ADD_FD] = [this](fmap_t& fmap, future_t& future,
                                bool const transient,
                                env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      double const result = std::stod(*left) + std::stod(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::SUB_FI] = [this](fmap_t& fmap, future_t& future,
                                bool const transient,
                                env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      int const result = std::stoi(*left) - std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::SUB_FD] = [this](fmap_t& fmap, future_t& future,
                                bool const transient,
                                env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      double const result = std::stod(*left) - std::stod(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::CONCAT_FS] = [this](fmap_t& fmap, future_t& future,
                                   bool const transient,
                                   env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      bin_op.value = *left + *right;
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::CONCAT_SF] = [this](fmap_t& fmap, future_t& future,
                                   bool const transient,
                                   env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto const* left = &bin_op.left.value;
      auto& right_f = fmap[bin_op.right.fi];
      auto const* right = ops[right_f.type](fmap, right_f, transient, env);
      bin_op.value = *left + *right;
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::GTEQ_FI] = [this](fmap_t& fmap, future_t& future,
                                 bool const transient,
                                 env_t const& env) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](fmap, left_f, transient, env);
      auto const* right = &bin_op.right.value;
      bool const result = std::stoi(*left) >= std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::NONE] = [](fmap_t& fmap, future_t& future, bool const transient,
                          env_t const& env) -> value_t const* {
    DASSERT(false);
    value_t* p = nullptr;
    *p = 1;
    return p;
  };
}

void
dm_handler::get_notxn(tcxx::function<void(rread_t const&)> cob,
                      key_t const& key)
{
  DPRINTF("get_notxn(%s):\n", key.c_str());
  Result res = storage->get(key);
  rread_t _return;
  _return.existed = res.exists;
  _return.value = res.value;
  DPRINTF("  ok   exists=%d value=%s\n", _return.existed,
          _return.value.c_str());
  cob(_return);
}

void
dm_handler::get(tcxx::function<void(rread_t const&)> cob, tid_t const& tid,
                key_t const& key, bool const for_update)
{
#if !defined(TWOPL)
  DASSERT(!for_update);
#else
  auto& txnw = txns[tid];
  txnw.txn.tid = tid;
  txnw.local_txn.tid = tid;
#endif
  get_cob(cob, tid, key, for_update);
}

void
dm_handler::get_cob(tcxx::function<void(rread_t const&)> cob, tid_t const& tid,
                    key_t const& key, bool const for_update)
{
  DPRINTF("get(%s,%s,%s):\n", tid.c_str(), key.c_str(),
          std::to_string(for_update).c_str());
  auto& lock = vlocks[key];
  rread_t _return;
  _return.status = vote_result_t::OK;
#if defined TWOPL
  auto& txnw = txns[tid];
  txn_queue_t pending_txns;
#endif
#if defined DEADLOCK_WOUNDWAIT
#if defined TWOPL
  if (txnw.state == txn_state::ABORTED || txnw.state == txn_state::PREEMPTED) {
    _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
    DPRINTF("  kill %s %s\n", (for_update ? "rw" : "r "),
            lock.to_string().c_str());
    cob(_return);
    return;
  }
#else // OCC
  DASSERT(txns.find(tid) == txns.end());
#endif
#endif
#ifdef TWOPL
  bool const can_lock =
    (for_update && lock.can_wlock(tid)) || (!for_update && lock.can_rlock(tid));
  if (can_lock) {
    auto& txn = txns[tid].local_txn;
    auto& rwset = txn.rwset;
    DASSERT(rwset.find(key) == rwset.end());
    auto& access = rwset[key];
    read_t* read;
    if (for_update) {
      access.type = atype_t::READ_WRITE;
      read = &access.u.rw.r;
      lock.wlock(tid);
    } else {
      access.type = atype_t::READ;
      read = &access.u.r;
      lock.rlock(tid);
    }
    read->future = false;
#endif
    Result res = storage->get(key);
    _return.existed = res.exists;
    _return.value = res.value;
    _return.version = res.exists ? lock.version() : -1;
#ifdef TWOPL
    DPRINTF("  ok   %s exists=%d version=%" PRId64 " value=%s %s\n",
            (for_update ? "rw" : "r "), _return.existed, _return.version,
            _return.value.c_str(), lock.to_string().c_str());
#endif
#if !defined TWOPL
    DPRINTF("  ok   exists=%d version=%" PRId64 " value=%s %s\n",
            _return.existed, _return.version, _return.value.c_str(),
            lock.to_string().c_str());
#endif
#ifdef TWOPL
  } else {
    if (lock.can_block(for_update, tid, txns, pending_txns, this)) {
      auto txn_cob =
        tcxx::bind(&dm_handler::get_cob, this, cob, tid, key, for_update);
      lock.block(for_update, tid, txnw, txn_cob);
      DPRINTF("  wait %s %s\n", (for_update ? "rw" : "r "),
              lock.to_string().c_str());
      process(pending_txns);
      return;
    } else {
      _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
      DPRINTF("  fail %s %s\n", (for_update ? "rw" : "r "),
              lock.to_string().c_str());
    }
  }
#endif
  cob(_return);
}

void
dm_handler::multiget_notxn(tcxx::function<void(mget_t const&)> cob,
                           klist_t const& klist)
{
  DPRINTF("multiget_notxn(...):\n");
  mget_t _return;
  for (auto const key : klist) {
    rread_t& rread = _return.mgmap[key];
    Result res = storage->get(key);
    rread.existed = res.exists;
    rread.value = res.value;
    DPRINTF("  ok   key=%s exists=%d value=%s\n", key.c_str(), rread.existed,
            rread.value.c_str());
  }
  cob(_return);
}

void
dm_handler::multiget(tcxx::function<void(mget_t const&)> cob, tid_t const& tid,
                     klist_t const& klist, bool const for_update)
{
#if !defined(TWOPL)
  DASSERT(!for_update);
  auto const& keys = klist;
#endif
#ifdef TWOPL
  auto& txnw = txns[tid];
  txnw.txn.tid = tid;
  txnw.local_txn.tid = tid;
  txnw.klist = klist;
  auto const& keys = txnw.klist;
  txnw.mget_return.mgmap.clear();
#endif
  multiget_cob(cob, tid, keys, keys.begin(), for_update);
}

void
dm_handler::multiget_cob(tcxx::function<void(mget_t const&)> cob,
                         tid_t const& tid, klist_t const& klist,
                         klist_t::const_iterator kit, bool const for_update)
{
  DPRINTF("multiget(%s,...):\n", tid.c_str());
#ifdef TWOPL
  auto& txnw = txns[tid];
  auto& txn = txnw.local_txn;
  auto& rwset = txn.rwset;
  mget_t& _return = txnw.mget_return;
  txn_queue_t pending_txns;
#endif
#if !defined(TWOPL)
  mget_t _return;
#endif
  _return.status = vote_result_t::OK;
#if defined DEADLOCK_WOUNDWAIT
#if defined TWOPL
  if (txnw.state == txn_state::ABORTED || txnw.state == txn_state::PREEMPTED) {
    _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
    DPRINTF("  kill %s key=%s %s\n", (for_update ? "rw" : "r "), kit->c_str(),
            vlocks[*kit].to_string().c_str());
    cob(_return);
    return;
  }
#else // OCC
  DASSERT(txns.find(tid) == txns.end());
#endif
#endif
  for (; kit != klist.end(); ++kit) {
    auto const& key = *kit;
    auto& lock = vlocks[key];
#ifdef TWOPL
    bool const can_lock = (for_update && lock.can_wlock(tid)) ||
                          (!for_update && lock.can_rlock(tid));
    if (can_lock) {
      DASSERT(rwset.find(key) == rwset.end());
      auto& access = rwset[key];
      read_t* read;
      if (for_update) {
        access.type = atype_t::READ_WRITE;
        read = &access.u.rw.r;
        lock.wlock(tid);
      } else {
        access.type = atype_t::READ;
        read = &access.u.r;
        lock.rlock(tid);
      }
      read->future = false;
#endif
      rread_t& rread = _return.mgmap[key];
      Result res = storage->get(key);
      rread.existed = res.exists;
      rread.value = res.value;
      rread.version = res.exists ? lock.version() : -1;
#ifdef TWOPL
      DPRINTF("  ok   %s key=%s exists=%d version=%" PRId64 " value=%s %s\n",
              (for_update ? "rw" : "r "), key.c_str(), rread.existed,
              rread.version, rread.value.c_str(), lock.to_string().c_str());
#endif
#if !defined(TWOPL)
      DPRINTF("  ok   key=%s exists=%d version=%" PRId64 " value=%s %s\n",
              key.c_str(), rread.existed, rread.version, rread.value.c_str(),
              lock.to_string().c_str());
#endif
#ifdef TWOPL
    } else {
      if (lock.can_block(for_update, tid, txns, pending_txns, this)) {
        auto txn_cob = tcxx::bind(&dm_handler::multiget_cob, this, cob, tid,
                                  std::ref(klist), kit, for_update);
        lock.block(for_update, tid, txnw, txn_cob);
        DPRINTF("  wait %s key=%s %s\n", (for_update ? "rw" : "r "),
                key.c_str(), lock.to_string().c_str());
        process(pending_txns);
        return;
      } else {
        _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
        DPRINTF("  fail %s key=%s %s\n", (for_update ? "rw" : "r "),
                key.c_str(), lock.to_string().c_str());
        break;
      }
    }
#endif
  }
  cob(_return);
}

void
dm_handler::opc_commit(tcxx::function<void(vote_t const&)> cob,
                       txn_t const& txn)
{
  auto& txnw = txns[txn.tid];
  txnw.txn = txn;
#ifdef TWOPL
  txnw.local_txn.tid = txn.tid;
  auto pit = txnw.keys_to_validate.begin();
#else
  auto pit = txnw.txn.pset.begin();
#endif
  opc_commit_cob(cob, txnw, txnw.txn.rwset.begin(), false, pit);
}

void
dm_handler::opc_commit_cob(tcxx::function<void(vote_t const&)> cob,
                           txnw_t& txnw, aiterator_t ait, bool fwset_done,
                           piterator_t pit)
{
  auto& txn = txnw.txn;
  DPRINTF("opc_commit(%s):\n", txn.tid.c_str());
  vote_result_t::type result = vote_result_t::OK;
  bool done;
  txn_queue_t pending_txns;
#if defined DEADLOCK_WOUNDWAIT
#if defined TWOPL
  if (txnw.state == txn_state::ABORTED || txnw.state == txn_state::PREEMPTED) {
#else // OCC
  if (txnw.state == txn_state::ABORTED) {
#endif
    result = vote_result_t::DEADLOCK_AVOIDANCE;
    done = true;
  } else {
#endif
    // for 2PL, try-lock writes
    // for OCC, validate and lock reads, lock writes
    done = prepare(cob, txnw, ait, pit, pending_txns,
                   tcxx::bind(&dm_handler::opc_commit_cob, this,
                              tcxx::placeholders::_1, tcxx::placeholders::_2,
                              tcxx::placeholders::_3, tcxx::placeholders::_4,
                              tcxx::placeholders::_5),
                   result);
#if defined DEADLOCK_WOUNDWAIT
  }
#endif
  if (!done) {
    process(pending_txns);
    return;
  }
  if (result == vote_result_t::OK) {
    if (!fwset_done) {
      // try-lock writes
      result = trylock_fwset(txnw);
#if defined TWOPL
      pit = txnw.keys_to_validate.begin();
      DASSERT(txnw.plocked_keys.empty());
      txnw.env.clear();
#endif
    }
    if (result == vote_result_t::OK) {
      // validate predicates
      done = validate_predicates(
        cob, txnw, ait, pit, pending_txns,
        tcxx::bind(&dm_handler::opc_commit_cob, this, tcxx::placeholders::_1,
                   tcxx::placeholders::_2, tcxx::placeholders::_3,
                   tcxx::placeholders::_4, tcxx::placeholders::_5),
        result);
      if (!done) {
        process(pending_txns);
        return;
      }
    }
  }
  DPRINTF(
    "  reply %s\n",
    result == vote_result_t::OK
      ? "OK"
      : (result == vote_result_t::VERSION_VALIDATION
           ? "VERSION"
           : (result == vote_result_t::PREDICATE_VALIDATION
                ? "PREDICATE"
                : (result == vote_result_t::DEADLOCK_AVOIDANCE ? "DEADLOCK"
                                                               : "?"))));
  bool const success = result == vote_result_t::OK;
  // reply to txn manager
  vote_t vote;
  vote.result = result;
  if (success) {
    vote.rfmap = txnw.rfmap;
  }
  cob(vote);
  if (success) {
    commit(txnw, pending_txns);
  } else {
#if defined TWOPL
    auto begin = txnw.local_txn.rwset.begin();
#if defined DEADLOCK_WOUNDWAIT
    auto end =
      txnw.state == txn_state::PREEMPTED ? begin : txnw.local_txn.rwset.end();
#else
    auto end = txnw.local_txn.rwset.end();
#endif
    ait = end;
#else // OCC
    auto begin = txnw.txn.rwset.begin();
    auto end = txnw.txn.rwset.end();
#endif
    abort(txnw, begin, ait, end, pending_txns);
  }
  txns.erase(txn.tid);
  process(pending_txns);
}

void
dm_handler::tpc_prepare(tcxx::function<void(vote_t const&)> cob,
                        txn_t const& txn)
{
  auto& txnw = txns[txn.tid];
  txnw.txn = txn;
#ifdef TWOPL
  txnw.local_txn.tid = txn.tid;
  auto pit = txnw.keys_to_validate.begin();
#else
  auto pit = txnw.txn.pset.begin();
#endif
  tpc_prepare_cob(cob, txnw, txnw.txn.rwset.begin(), false, pit);
}

void
dm_handler::tpc_prepare_cob(tcxx::function<void(vote_t const&)> cob,
                            txnw_t& txnw, aiterator_t ait, bool fwset_done,
                            piterator_t pit)
{
  auto& txn = txnw.txn;
  DPRINTF("tpc_prepare(%s):\n", txn.tid.c_str());
  vote_result_t::type result = vote_result_t::OK;
  bool done;
  txn_queue_t pending_txns;
#if defined DEADLOCK_WOUNDWAIT
#if defined TWOPL
  if (txnw.state == txn_state::ABORTED || txnw.state == txn_state::PREEMPTED) {
#else // OCC
  if (txnw.state == txn_state::ABORTED) {
#endif
    result = vote_result_t::DEADLOCK_AVOIDANCE;
    done = true;
  } else {
#endif
    // for 2PL, try-lock writes
    // for OCC, validate and lock reads, lock writes
    done = prepare(cob, txnw, ait, pit, pending_txns,
                   tcxx::bind(&dm_handler::tpc_prepare_cob, this,
                              tcxx::placeholders::_1, tcxx::placeholders::_2,
                              tcxx::placeholders::_3, tcxx::placeholders::_4,
                              tcxx::placeholders::_5),
                   result);
#if defined DEADLOCK_WOUNDWAIT
  }
#endif
  if (!done) {
    process(pending_txns);
    return;
  }
  if (result == vote_result_t::OK) {
    if (!fwset_done) {
      // try-lock writes
      result = trylock_fwset(txnw);
#if defined TWOPL
      pit = txnw.keys_to_validate.begin();
      txnw.env.clear();
#endif
    }
#if defined TWOPL
    if (result == vote_result_t::OK) {
      DASSERT(txnw.plocked_keys.empty());
      // validate predicates
      done = validate_predicates(
        cob, txnw, ait, pit, pending_txns,
        tcxx::bind(&dm_handler::tpc_prepare_cob, this, tcxx::placeholders::_1,
                   tcxx::placeholders::_2, tcxx::placeholders::_3,
                   tcxx::placeholders::_4, tcxx::placeholders::_5),
        result);
      if (!done) {
        process(pending_txns);
        return;
      }
    }
#endif
  }
  DPRINTF(
    "  reply %s\n",
    result == vote_result_t::OK
      ? "OK"
      : (result == vote_result_t::VERSION_VALIDATION
           ? "VERSION"
           : (result == vote_result_t::PREDICATE_VALIDATION
                ? "PREDICATE"
                : (result == vote_result_t::DEADLOCK_AVOIDANCE ? "DEADLOCK"
                                                               : "?"))));
  bool const success = result == vote_result_t::OK;
  // reply to txn manager
  vote_t vote;
  vote.result = result;
  if (success) {
#if defined DEADLOCK_WOUNDWAIT
    txnw.state = txn_state::PREPARED;
#endif
    vote.rfmap = txnw.rfmap;
  }
  cob(vote);
  if (!success) {
#if defined TWOPL
    auto begin = txnw.local_txn.rwset.begin();
#if defined DEADLOCK_WOUNDWAIT
    auto end =
      txnw.state == txn_state::PREEMPTED ? begin : txnw.local_txn.rwset.end();
#else
    auto end = txnw.local_txn.rwset.end();
#endif
    ait = end;
#else // OCC
    auto begin = txnw.txn.rwset.begin();
    auto end = txnw.txn.rwset.end();
#endif
    abort(txnw, begin, ait, end, pending_txns);
    txns.erase(txn.tid);
  }
  process(pending_txns);
}

void
dm_handler::tpc_prepare2(tcxx::function<void(vote_t const&)> cob,
                         txn_t const& _txn, rfmap_t const& rfmap)
{
  bool const exists = txns.find(_txn.tid) != txns.end();
  auto& txnw = txns[_txn.tid];
  if (!exists) {
    txnw.txn = _txn;
#if defined TWOPL
    txnw.local_txn.tid = _txn.tid;
#endif
  }
  auto& txn = txnw.txn;
  if (exists) {
    txn.future_wset = _txn.future_wset;
    DASSERT(txn.future_wset.size() == _txn.future_wset.size());
    resolve_remote_futures(txnw, rfmap);
  }
  DPRINTF("tpc_prepare2(%s):\n", txn.tid.c_str());
  vote_result_t::type result;
#if defined DEADLOCK_WOUNDWAIT
#if defined TWOPL
  if (txnw.state == txn_state::ABORTED || txnw.state == txn_state::PREEMPTED) {
#else // OCC
  if (txnw.state == txn_state::ABORTED) {
#endif
    result = vote_result_t::DEADLOCK_AVOIDANCE;
  } else {
#endif
    result = trylock_fwset(txnw);
#if defined DEADLOCK_WOUNDWAIT
  }
#endif
  DPRINTF(
    "  reply %s\n",
    result == vote_result_t::OK
      ? "OK"
      : (result == vote_result_t::VERSION_VALIDATION
           ? "VERSION"
           : (result == vote_result_t::PREDICATE_VALIDATION
                ? "PREDICATE"
                : (result == vote_result_t::DEADLOCK_AVOIDANCE ? "DEADLOCK"
                                                               : "?"))));
  bool const success = result == vote_result_t::OK;
  // reply to txn manager
  vote_t vote;
  vote.result = result;
#if defined DEADLOCK_WOUNDWAIT
  if (success) {
    txnw.state = txn_state::PREPARED;
  }
#endif
  cob(vote);
  // transactions that can proceed due to this transaction's commit/abort
  txn_queue_t pending_txns;
  if (!success) {
#if defined TWOPL
    auto begin = txnw.local_txn.rwset.begin();
#if defined DEADLOCK_WOUNDWAIT
    auto end =
      txnw.state == txn_state::PREEMPTED ? begin : txnw.local_txn.rwset.end();
#else
    auto end = txnw.local_txn.rwset.end();
#endif
#else // OCC
    auto begin = txnw.txn.rwset.begin();
    auto end = txnw.txn.rwset.end();
#endif
    abort(txnw, begin, end, end, pending_txns);
    txns.erase(txn.tid);
    process(pending_txns);
  }
}

void
dm_handler::tpc_commit(tcxx::function<void()> cob, tid_t const& tid,
                       rfmap_t const& rfmap)
{
  DASSERT(txns.find(tid) != txns.end());
  DPRINTF("tpc_commit(%s):\n", tid.c_str());
  DPRINTF("  reply %s\n", "OK");
  // resolve remote futures BEFORE cob() because I think it frees the memory
  // where the parameters reside
  auto& txnw = txns[tid];
  resolve_remote_futures(txnw, rfmap);
  // reply to txn manager
  cob();
  // transactions that can proceed due to this transaction's commit/abort
  txn_queue_t pending_txns;
  commit(txnw, pending_txns);
  txns.erase(tid);
  process(pending_txns);
}

void
dm_handler::tpc_abort(tcxx::function<void()> cob, tid_t const& tid)
{
#if !defined TWOPL
  DASSERT(txns.find(tid) != txns.end());
#endif
  DPRINTF("tpc_abort(%s):\n", tid.c_str());
  DPRINTF("  reply %s\n", "OK");
  // reply to txn manager
  cob();
#if defined TWOPL
  if (txns.find(tid) != txns.end()) {
#endif
    // transactions that can proceed due to this transaction's commit/abort
    txn_queue_t pending_txns;
    auto& txnw = txns[tid];
#if defined TWOPL
    txnw.txn.tid = tid;
    txnw.local_txn.tid = tid;
    auto begin = txnw.local_txn.rwset.begin();
#if defined DEADLOCK_WOUNDWAIT
    auto end =
      txnw.state == txn_state::PREEMPTED ? begin : txnw.local_txn.rwset.end();
#else
    auto end = txnw.local_txn.rwset.end();
#endif
#else // OCC
  auto begin = txnw.txn.rwset.begin();
  auto end = txnw.txn.rwset.end();
#endif
    abort(txnw, begin, end, end, pending_txns);
    txns.erase(tid);
    process(pending_txns);
#if defined TWOPL
  }
#endif
}

void
dm_handler::opc_assert(tcxx::function<void(assert_t const&)> cob,
                       tid_t const& tid, predicate_t const& predicate,
                       fmap_t const& fmap)
{
#if defined TWOPL
  auto& txnw = txns[tid];
  txnw.txn.tid = tid;
  txnw.local_txn.tid = tid;
  txnw.predicate = predicate;
  txnw.local_txn.futures.fmap = fmap;
  opc_assert_cob(cob, tid, txnw.predicate, txnw.local_txn.futures.fmap,
                 txnw.predicate.func.keys.begin());
#else
  DASSERT(false);
#endif
}

void
dm_handler::opc_assert_cob(tcxx::function<void(assert_t const&)> cob,
                           tid_t const& tid, predicate_t const& predicate,
                           fmap_t& fmap, klist_t::const_iterator kit)
{
#if defined TWOPL
  auto& future_fi = predicate.func.future_fi;
  DPRINTF("opc_assert(%s,%s):\n", tid.c_str(),
          future_to_string(fmap, fmap.at(future_fi)).c_str());
  auto& txnw = txns[tid];
  auto& local_txn = txnw.local_txn;
  auto& rwset = local_txn.rwset;
  auto& keys = predicate.func.keys;
  auto& pset = local_txn.pset;
  assert_t _return;
  _return.status = vote_result_t::OK;
  txn_queue_t pending_txns;
  for (; kit != keys.end(); ++kit) {
    auto const& key = *kit;
    auto& lock = vlocks[key];
    DASSERT(rwset.find(key) == rwset.end());
    if (lock.can_rlock(tid)) {
      auto& access = rwset[key];
      access.type = atype_t::READ;
      access.u.r.future = false;
      lock.rlock(tid);
      DPRINTF("  ok   r  key=%s %s\n", key.c_str(), lock.to_string().c_str());
    } else {
      if (lock.can_block(false, tid, txns, pending_txns, this)) {
        auto txn_cob = tcxx::bind(&dm_handler::opc_assert_cob, this, cob, tid,
                                  std::ref(predicate), std::ref(fmap), kit);
        lock.block(false, tid, txnw, txn_cob);
        DPRINTF("  wait r  key=%s %s\n", key.c_str(), lock.to_string().c_str());
        process(pending_txns);
        return;
      } else {
        _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
        DPRINTF("  fail r  key=%s %s\n", key.c_str(), lock.to_string().c_str());
        break;
      }
    }
  }
  bool const ok = _return.status == vote_result_t::OK;
  if (ok) {
    DASSERT(kit == keys.end());
    pset.push_back(predicate);
    auto& pred = pset.back();
    auto const* const result = resolve(fmap, fmap.at(future_fi), true);
    pred.expected = *result == std::to_string(true);
    _return.result = pred.expected;
    for (auto const& key : keys) {
      vlocks[key].plock(tid, txnw, pred);
      txnw.plocked_keys.insert(key);
    }
  } else {
    DASSERT(kit != keys.end());
  }
  DPRINTF("  reply %s, %d\n", ok ? "OK" : "ABORT", _return.result);
  cob(_return);
  DASSERT(pending_txns.empty());
  for (auto it = keys.begin(); it != kit; ++it) {
    auto const& key = *it;
    auto& lock = vlocks[key];
    lock.runlock(tid);
    DPRINTF("  ok   %s  key=%s %s\n", ok ? "p" : "r", key.c_str(),
            lock.to_string().c_str());
    rwset.erase(key);
    // pending readers or writer
    auto const rpending = lock.rpending();
    DASSERT(!rpending);
    auto const wpending = !lock.rlocked() && lock.wpending();
    if (rpending || wpending) {
#if defined NDEBUG
      lock.unblock(pending_txns);
#else
      auto const n = lock.unblock(pending_txns);
      DASSERT((wpending && n == 1) || (rpending && n > 0));
#endif
    }
  }
  process(pending_txns);
#else
  DASSERT(false);
#endif
}

bool
dm_handler::prepare(
  tcxx::function<void(vote_t const&)> cob, txnw_t& txnw, aiterator_t& ait,
  piterator_t& pit, txn_queue_t& pending_txns,
  tcxx::function<void(tcxx::function<void(vote_t const&)>, txnw_t&, aiterator_t,
                      bool, piterator_t)>
    caller,
  vote_result_t::type& result)
{
  auto& txn = txnw.txn;
#if defined TWOPL
  auto& local_txn = txnw.local_txn;
#endif
  for (; ait != txn.rwset.end(); ++ait) {
    auto& entry = *ait;
    auto const& key = entry.first;
    auto& access = entry.second;
    auto const& type = access.type;
    auto& lock = vlocks[key];
    if (type == atype_t::READ) {
      auto& read = access.u.r;
#if defined TWOPL
      if (read.future) {
        bool const can_rlock = lock.can_rlock(txn.tid);
        if (can_rlock) {
          // read, lock, and add to local_txn
          prepare_read(txnw, key, access, type, pending_txns);
          // next iteration
        } else {
          bool const can_block =
            lock.can_block(false, txn.tid, txns, pending_txns, this);
          if (can_block) {
            auto txn_cob =
              tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
            lock.block(false, txn.tid, txnw, txn_cob);
            DPRINTF("  wait r  %s %s\n", key.c_str(), lock.to_string().c_str());
            return false;
          } else {
            DPRINTF("  fail r  %s %s\n", key.c_str(), lock.to_string().c_str());
            result = vote_result_t::DEADLOCK_AVOIDANCE;
            return true;
          }
        }
      } else {
        // already locked on read
        DASSERT(!lock.wlocked() && lock.rlocked() &&
                lock._readers.find(txn.tid) != lock._readers.end());
        DASSERT(local_txn.rwset.find(key) != local_txn.rwset.end());
        DASSERT(local_txn.rwset[key].type == atype_t::READ);
        DASSERT(txnw.plocked_keys.find(key) == txnw.plocked_keys.end());
      }
#else
      bool const can_rlock = lock.can_rlock(txn.tid);
      if (can_rlock) {
        if (read.future) {
          // read and lock
          prepare_read(txnw, key, access, type, pending_txns);
        } else {
          // validate and lock
          vote_result_t::type const ok = prepare_read(txn.tid, key, read, type);
          if (ok != vote_result_t::OK) {
            result = ok;
            return true;
          }
        }
        // next iteration
      } else {
        bool const can_block =
          lock.can_block(false, txn.tid, txns, pending_txns, this);
        if (can_block) {
          auto txn_cob =
            tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
          lock.block(false, txn.tid, txnw, txn_cob);
          DPRINTF("  wait r  %s %s\n", key.c_str(), lock.to_string().c_str());
          return false;
        } else {
          DPRINTF("  fail r  %s %s\n", key.c_str(), lock.to_string().c_str());
          result = vote_result_t::DEADLOCK_AVOIDANCE;
          return true;
        }
      }
#endif
    } else if (type == atype_t::WRITE) {
#if defined TWOPL
      DASSERT(local_txn.rwset.find(key) == local_txn.rwset.end());
      DASSERT(txnw.plocked_keys.find(key) == txnw.plocked_keys.end());
      if (lock.can_wlock(txn.tid)) {
        local_txn.rwset[key] = access;
        lock.wlock(txn.tid);
        if (lock.plocked()) {
          txnw.keys_to_validate.insert(key);
          DPRINTF("  okp  w  %s %s\n", key.c_str(), lock.to_string().c_str());
        } else {
          DPRINTF("  ok   w  %s %s\n", key.c_str(), lock.to_string().c_str());
        }
        // next iteration
      } else {
        bool const can_block =
          lock.can_block(true, txn.tid, txns, pending_txns, this);
        if (can_block) {
          auto txn_cob =
            tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
          lock.block(true, txn.tid, txnw, txn_cob);
          DPRINTF("  wait w  %s %s\n", key.c_str(), lock.to_string().c_str());
          return false;
        } else {
          DPRINTF("  fail w  %s %s\n", key.c_str(), lock.to_string().c_str());
          result = vote_result_t::DEADLOCK_AVOIDANCE;
          return true;
        }
      }
#else
      if (lock.can_wlock(txn.tid)) {
        lock.wlock(txn.tid);
        DPRINTF("  ok   w  %s %s\n", key.c_str(), lock.to_string().c_str());
        // next iteration
      } else {
        bool const can_block =
          lock.can_block(true, txn.tid, txns, pending_txns, this);
        if (can_block) {
          auto txn_cob =
            tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
          lock.block(true, txn.tid, txnw, txn_cob);
          DPRINTF("  wait w  %s %s\n", key.c_str(), lock.to_string().c_str());
          return false;
        } else {
          DPRINTF("  fail w  %s %s\n", key.c_str(), lock.to_string().c_str());
          result = vote_result_t::DEADLOCK_AVOIDANCE;
          return true;
        }
      }
#endif
    } else {
      DASSERT(type == atype_t::READ_WRITE);
      auto& read = access.u.rw.r;
#if defined TWOPL
      if (read.future) {
        if (lock.can_wlock(txn.tid)) {
          prepare_read(txnw, key, access, type, pending_txns);
        } else {
          bool const can_block =
            lock.can_block(true, txn.tid, txns, pending_txns, this);
          if (can_block) {
            auto txn_cob =
              tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
            lock.block(true, txn.tid, txnw, txn_cob);
            DPRINTF("  wait rw %s %s\n", key.c_str(), lock.to_string().c_str());
            return false;
          } else {
            DPRINTF("  fail rw %s %s\n", key.c_str(), lock.to_string().c_str());
            result = vote_result_t::DEADLOCK_AVOIDANCE;
            return true;
          }
        }
      } else {
        DASSERT(lock.wlocked() && lock._writer == txn.tid);
        DASSERT(local_txn.rwset.find(key) != local_txn.rwset.end());
        DASSERT(local_txn.rwset[key].type == atype_t::READ_WRITE);
        DASSERT(txnw.plocked_keys.find(key) == txnw.plocked_keys.end());
        local_txn.rwset[key] = access;
      }
#else
      if (lock.can_wlock(txn.tid)) {
        if (read.future) {
          prepare_read(txnw, key, access, type, pending_txns);
        } else {
          vote_result_t::type const ok = prepare_read(txn.tid, key, read, type);
          if (ok != vote_result_t::OK) {
            result = ok;
            return true;
          }
        }
      } else {
        bool const can_block =
          lock.can_block(true, txn.tid, txns, pending_txns, this);
        if (can_block) {
          auto txn_cob =
            tcxx::bind(caller, cob, std::ref(txnw), ait, false, pit);
          lock.block(true, txn.tid, txnw, txn_cob);
          DPRINTF("  wait rw %s %s\n", key.c_str(), lock.to_string().c_str());
          return false;
        } else {
          DPRINTF("  fail rw %s %s\n", key.c_str(), lock.to_string().c_str());
          result = vote_result_t::DEADLOCK_AVOIDANCE;
          return true;
        }
      }
#endif
    }
  }
  return true;
}

void
dm_handler::resolve_remote_futures(txnw_t& txnw, rfmap_t const& rfmap)
{
  auto& txn = txnw.txn;
  auto& fmap = txn.futures.fmap;
  for (auto it = rfmap.begin(); it != rfmap.end(); it++) {
    auto const& fi = it->first;
    auto const& rread = it->second;
    auto& future_read = fmap.at(fi);
    DASSERT(future_read.type == ftype_t::READ);
    auto& fread = future_read.u.rdata;
    if (!fread.resolved) {
      DASSERT(txnw.rfmap.find(fi) == txnw.rfmap.end());
      fread.data = rread;
      fread.resolved = true;
    }
  }
}

vote_result_t::type
dm_handler::trylock_fwset(txnw_t& txnw)
{
  /* TODO use deadlock prevention policy */
  auto& txn = txnw.txn;
  auto& fwset = txn.future_wset;
  auto& rwset = txn.rwset;
#if defined TWOPL
  auto& local_rwset = txnw.local_txn.rwset;
#endif
  for (auto fwit = fwset.begin(); fwit != fwset.end(); fwit++) {
    auto const& key_fi = fwit->func.future_fi;
    auto const& write = fwit->w;
    auto& fmap = txn.futures.fmap;
    auto& key_f = fmap[key_fi];
    auto const* const key_p = resolve(fmap, key_f);
    DASSERT(key_p != nullptr);
    auto& lock = vlocks[*key_p];
    /* what happens if I already locked the key for:
     *  (a) read, and
     *    (1) am sole reader
     *    (2) there are multiple readers
     *  (b) write
     *
     * (a.1) upgrade to write lock
     * (a.2) abort
     * (b) which of the writes is the latest?
     */
    bool const wlocked = lock.wlocked();
    bool const rlocked = lock.rlocked();
    bool const by_txn = rwset.find(*key_p) != rwset.end();
    bool const sole_owner = lock.readers() == 1;
    if (!wlocked && (!rlocked || (rlocked && by_txn && sole_owner))) {
      auto& access = rwset[*key_p];
      auto& atype = access.type;
      auto& rwdata = access.u;
      if (!rlocked) {
        DASSERT(!by_txn);
        atype = atype_t::WRITE;
        rwdata.w = write;
      } else {
        DASSERT(rlocked && by_txn && sole_owner);
        DASSERT(atype == atype_t::READ);
        atype = atype_t::READ_WRITE;
        DASSERT(rwdata.r.future);
        auto const& fi = rwdata.r.u.future_fi;
        DASSERT(fmap[fi].type == ftype_t::READ);
        DASSERT(!fmap[fi].u.rdata.resolved);
        DASSERT(fmap[fi].u.rdata.key == *key_p);
        auto& read = rwdata.rw.r;
        read.future = true;
        read.u.future_fi = fi;
        rwdata.rw.w = write;
        DASSERT(lock._readers.find(txn.tid) == lock._readers.end());
        lock.runlock(txn.tid);
        DASSERT(!lock.rlocked());
        DASSERT(lock.readers() == 0);
      }
      lock.wlock(txn.tid);
      if (!rlocked) {
        DPRINTF("  ok   w  %s %s\n", key_p->c_str(), lock.to_string().c_str());
      } else {
        DPRINTF("  ok   rw %s %s\n", key_p->c_str(), lock.to_string().c_str());
      }
#if defined TWOPL
      local_rwset[*key_p] = access;
      if (lock.plocked()) {
        std::cerr
          << "ABORT: future write-key %s is plocked (not implemented yet)"
          << *key_p << std::endl;
        std::abort();
      }
#endif
    } else {
      // TODO if wlocked and the lock owner is the transaction, we simply need
      //      to know which of the write functions is the latest and squash
      //      the other
      DASSERT(!(wlocked && lock._writer == txn.tid));
      DASSERT(wlocked || (rlocked && !by_txn));
      DPRINTF("  fail ?? %s %s\n", key_p->c_str(), lock.to_string().c_str());
      return vote_result_t::DEADLOCK_AVOIDANCE;
    }
  }
  return vote_result_t::OK;
}

bool
dm_handler::validate_predicates(
  tcxx::function<void(vote_t const&)> cob, txnw_t& txnw, aiterator_t& ait,
  piterator_t& pit, txn_queue_t& pending_txns,
  tcxx::function<void(tcxx::function<void(vote_t const&)>, txnw_t&, aiterator_t,
                      bool, piterator_t)>
    caller,
  vote_result_t::type& result)
{
#if defined TWOPL
  DASSERT(txnw.plocked_keys.empty());
  auto& txn = txnw.local_txn;
  auto& plocks = txnw.keys_to_validate;
  // validate predicates
  for (; pit != plocks.end(); ++pit) {
    auto& key = *pit;
    auto& lock = vlocks[key];
    auto& plist = lock.plist;
    for (auto& entry : plist) {
      DASSERT(entry.owner != txn.tid);
      auto& predicate = entry.predicate;
      auto& fi = predicate.func.future_fi;
      auto& fmap = entry.txnw.local_txn.futures.fmap;
      // add writes to env
      for (auto const& key : predicate.func.keys) {
        if (txnw.env.find(key) == txnw.env.end()) {
          auto access_pair = txn.rwset.find(key);
          if (access_pair != txn.rwset.end()) {
            auto& access = access_pair->second;
            auto const& atype = access.type;
            if (atype == atype_t::WRITE || atype == atype_t::READ_WRITE) {
              write_t* write = nullptr;
              if (atype == atype_t::WRITE) {
                write = &access.u.w;
              } else {
                write = &access.u.rw.w;
              }
              DASSERT(write->type == wtype_t::PUT);
              value_t const* value;
              if (write->future) {
                auto& writer_fmap = txnw.txn.futures.fmap;
                value = resolve(writer_fmap, writer_fmap[write->u.func_fi]);
              } else {
                value = &write->u.value;
              }
              txnw.env.emplace(key, *value);
            }
          }
        }
      }
      // resolve and check
      auto const* const resolved = resolve(fmap, fmap[fi], true, txnw.env);
      if (predicate.expected != (*resolved == std::to_string(true))) {
        if (entry.can_block(txn.tid, txns, pending_txns, this)) {
          auto txn_cob =
            tcxx::bind(caller, cob, std::ref(txnw), ait, true, pit);
          entry.block(txn.tid, txnw, txn_cob);
          DPRINTF("  wait p  %s = %s, expected %s\n",
                  future_to_string(fmap, fmap[fi], txnw.env).c_str(),
                  resolved->c_str(),
                  std::to_string(predicate.expected).c_str());
          return false;
        } else {
          DPRINTF("  fail p  %s = %s, expected %s\n",
                  future_to_string(fmap, fmap[fi], txnw.env).c_str(),
                  resolved->c_str(),
                  std::to_string(predicate.expected).c_str());
          result = vote_result_t::DEADLOCK_AVOIDANCE;
          return true;
        }
      } else {
        DPRINTF("  ok   p  %s = %s, expected %s\n",
                future_to_string(fmap, fmap[fi], txnw.env).c_str(),
                resolved->c_str(), std::to_string(predicate.expected).c_str());
      }
    }
  }
#else
  auto& txn = txnw.txn;
  auto& fmap = txn.futures.fmap;
  auto& pset = txn.pset;
  for (; pit != pset.end(); pit++) {
    auto& pred = *pit;
    auto const& fi = pred.func.future_fi;
    auto& future = fmap[fi];
    auto& expected = pred.expected;
    auto const* resolved = resolve(fmap, future);
    auto const success = *resolved == std::to_string(expected);
    if (success) {
      DPRINTF("  ok   p  %s = %s, expected %s\n",
              future_to_string(fmap, future).c_str(), resolved->c_str(),
              std::to_string(expected).c_str());
    } else {
      DPRINTF("  fail p  %s = %s, expected %s\n",
              future_to_string(fmap, future).c_str(), resolved->c_str(),
              std::to_string(expected).c_str());
      result = vote_result_t::PREDICATE_VALIDATION;
      return true;
    }
  }
#endif
  return true;
}

void
dm_handler::commit(txnw_t& txnw, txn_queue_t& pending_txns)
{
#if defined TWOPL
  DASSERT(txnw.plocked_keys.empty());
#endif
#if defined USE_BATCH_WRITES
  std::vector<std::tuple<key_t, wtype_t::type, value_t>> batch;
#endif
  auto& txn = txnw.txn;
  auto& fmap = txn.futures.fmap;
  for (auto& entry : txn.rwset) {
    auto const& key = entry.first;
    auto& access = entry.second;
    auto const& type = access.type;
    auto& lock = vlocks[key];
    if (type == atype_t::READ) {
      lock.runlock(txn.tid);
      DPRINTF("  ok   r  %s unlock %s\n", key.c_str(),
              lock.to_string().c_str());
      // pending readers or writer
      auto const rpending = lock.rpending();
      auto const wpending = !lock.rlocked() && lock.wpending();
      if (rpending || wpending) {
#if defined NDEBUG
        lock.unblock(pending_txns);
#else
        auto const n = lock.unblock(pending_txns);
        DASSERT((wpending && n == 1) || (rpending && n > 0));
#endif
      }
    } else {
      auto const new_version = lock.version() + 1;
      write_t* write = nullptr;
      if (type == atype_t::WRITE) {
        write = &access.u.w;
      } else {
        DASSERT(type == atype_t::READ_WRITE);
        write = &access.u.rw.w;
      }
      lock.set_version(new_version);
      lock.wunlock(txn.tid);
      if (write->type == wtype_t::PUT) {
        value_t const* value;
        if (write->future) {
          value = resolve(fmap, fmap[write->u.func_fi]);
        } else {
          value = &write->u.value;
        }
        DPRINTF("  ok   %s %s unlock v=%" PRId64 " value=%s %s\n",
                (type == atype_t::WRITE ? "w " : "rw"), key.c_str(),
                new_version, value->c_str(), lock.to_string().c_str());
#if defined USE_BATCH_WRITES
        batch.push_back(std::make_tuple(key, write->type, *value));
#else
        storage->put(key, *value);
#endif
      } else {
        DASSERT(write->type == wtype_t::REMOVE);
        DPRINTF("  ok   %s %s unlock v=%" PRId64 " delete %s\n",
                (type == atype_t::WRITE ? "w " : "rw"), key.c_str(),
                new_version, lock.to_string().c_str());
#if defined USE_BATCH_WRITES
        batch.push_back(std::make_tuple(key, write->type, "<removed>"));
#else
        storage->del(key);
#endif
      }
      // pending readers or writer
      auto const rpending = lock.rpending();
      auto const wpending = lock.wpending();
      if (rpending || wpending) {
#if defined NDEBUG
        lock.unblock(pending_txns);
#else
        auto const n = lock.unblock(pending_txns);
        DASSERT((wpending && n == 1) || (rpending && n > 0));
#endif
      }
    }
  }
#if defined USE_BATCH_WRITES
  storage->put_batch(batch);
#endif
}

#if defined TWOPL && defined DEADLOCK_WOUNDWAIT
void
dm_handler::preempt(tid_t const& tid)
{
  txn_queue_t pending_txns;
  auto& txnw = txns.at(tid);
  DASSERT(txnw.state == txn_state::ABORTED);
  DPRINTF("preempt(%s):\n", tid.c_str());
  auto begin = txnw.local_txn.rwset.begin();
  auto end = txnw.local_txn.rwset.end();
  abort(txnw, begin, end, end, pending_txns);
  txnw.state = txn_state::PREEMPTED;
  process(pending_txns);
}
#endif

void
dm_handler::abort(txnw_t& txnw, const_aiterator_t begin,
                  const_aiterator_t up_to, const_aiterator_t end,
                  txn_queue_t& pending_txns)
{
  auto const lastit = up_to;
  if (up_to != end) {
    ++up_to;
  }
  for (auto it = begin; it != up_to; ++it) {
    auto const& entry = *it;
    auto const& key = entry.first;
    auto const& access = entry.second;
    auto const& type = access.type;
    auto& lock = vlocks[key];
    bool const unlock = it != lastit;
    if (type == atype_t::READ) {
      if (unlock) {
        lock.runlock(txnw.txn.tid);
        DPRINTF("  ok   r  %s unlock %s\n", key.c_str(),
                lock.to_string().c_str());
      }
      // pending readers or writer
      auto const rpending = lock.rpending();
      auto const wpending = !lock.rlocked() && lock.wpending();
      if (rpending || wpending) {
#if defined NDEBUG
        lock.unblock(pending_txns);
#else
        auto const n = lock.unblock(pending_txns);
        DASSERT((wpending && n == 1) || (rpending && n > 0));
#endif
      }
    } else {
      if (unlock) {
        lock.wunlock(txnw.txn.tid);
        DPRINTF("  ok   %s %s unlock %s\n",
                (type == atype_t::WRITE ? "w " : "rw"), key.c_str(),
                lock.to_string().c_str());
      }
      // pending readers or writer
      bool const rpending = lock.rpending();
      bool const wpending = lock.wpending();
      if (rpending || wpending) {
#if defined NDEBUG
        lock.unblock(pending_txns);
#else
        auto const n = lock.unblock(pending_txns);
        DASSERT((wpending && n == 1) || (rpending && n > 0));
#endif
      }
    }
  }
#if defined TWOPL
  // these keys were never upgraded from plock to r/wlock
  // we resume writers that are blocked on plocks we had
  auto& plocked = txnw.plocked_keys;
  auto it = plocked.begin();
  while (it != plocked.end()) {
    auto& key = *it;
    auto& lock = vlocks[key];
    lock.punlock(txnw.txn.tid, pending_txns);
    DPRINTF("  ok   p  %s unlock %s\n", key.c_str(), lock.to_string().c_str());
    it = plocked.erase(it);
  }
  DASSERT(plocked.empty());
#endif
}

void
dm_handler::process(txn_queue_t& pending_txns)
{
  while (!pending_txns.empty()) {
    auto txn = pending_txns.front();
    pending_txns.pop_front();
#if defined DEADLOCK_WOUNDWAIT
    DASSERT(txn.txnw.state == txn_state::QUEUED ||
            txn.txnw.state == txn_state::ABORTED);
    if (txn.txnw.state == txn_state::QUEUED) {
      txn.txnw.state = txn_state::EXECUTING;
    }
#endif
    DPRINTF("resuming %s (%s)...\n", txn.tid.c_str(),
            (txn.writer ? "write" : "read"));
    txn.cob();
  }
}

void
dm_handler::prepare_read(txnw_t& txnw, key_t const& key, access_t& access,
                         atype_t::type const& atype, txn_queue_t& pending_txns)
{
  auto& txn = txnw.txn;
  read_t* read;
  if (atype == atype_t::READ) {
    read = &access.u.r;
  } else {
    DASSERT(atype == atype_t::READ_WRITE);
    read = &access.u.rw.r;
  }
  DASSERT(read->future);
  auto& lock = vlocks[key];
  DASSERT((atype == atype_t::READ && !lock.wlocked()) ||
          (atype == atype_t::READ_WRITE && !lock.rlocked() && !lock.wlocked()));
  Result res = storage->get(key);
  auto const& fi = read->u.future_fi;
  auto& future = txn.futures.fmap[fi];
  DASSERT(future.type == ftype_t::READ);
  auto& fread = future.u.rdata;
  DASSERT(!fread.resolved);
  DASSERT(fread.key == key);
  auto& data = fread.data;
  data.existed = res.exists;
  data.value = res.value;
  data.version = res.exists ? lock.version() : -1;
  fread.resolved = true;
  DASSERT(txnw.rfmap.find(fi) == txnw.rfmap.end());
  txnw.rfmap[fi] = data;
  if (atype == atype_t::READ) {
    lock.rlock(txnw.txn.tid);
  } else {
    DASSERT(atype == atype_t::READ_WRITE);
    lock.wlock(txnw.txn.tid);
  }
#if defined TWOPL
  auto& local_txn = txnw.local_txn;
  DASSERT(local_txn.rwset.find(key) == local_txn.rwset.end());
  local_txn.rwset[key] = access;
  auto pit = txnw.plocked_keys.find(key);
  if (pit != txnw.plocked_keys.end()) {
    lock.punlock(txnw.txn.tid, pending_txns);
    txnw.plocked_keys.erase(pit);
  }
  if (lock.plocked()) {
    // TODO if predicate is of the same key, test it immediately instead of
    // postponing
    txnw.keys_to_validate.insert(key);
    DPRINTF("  okp  %s %s exists=%d version=%" PRId64 " value=%s %s\n",
            (atype == atype_t::READ ? "r " : "rw"), key.c_str(), data.existed,
            data.version, data.value.c_str(), lock.to_string().c_str());
  } else {
#endif
    DPRINTF("  ok   %s %s exists=%d version=%" PRId64 " value=%s %s\n",
            (atype == atype_t::READ ? "r " : "rw"), key.c_str(), data.existed,
            data.version, data.value.c_str(), lock.to_string().c_str());
#if defined TWOPL
  }
#endif
}

value_t const*
dm_handler::resolve(fmap_t& fmap, future_t& future, bool const transient,
                    env_t const& env)
{
  auto const& ftype = future.type;
  return ops[ftype](fmap, future, transient, env);
}

vote_result_t::type
dm_handler::prepare_read(tid_t const& tid, key_t const& key, read_t& read,
                         atype_t::type const& atype)
{
  DASSERT(!read.future);
  auto& lock = vlocks[key];
  DASSERT((atype == atype_t::READ && !lock.wlocked()) ||
          (atype == atype_t::READ_WRITE && !lock.rlocked() && !lock.wlocked()));
  // if existed, check version
  if (read.u.concrete.existed) {
    auto const& vread = read.u.concrete.version;
    if (lock.validate(vread)) {
      if (atype == atype_t::READ) {
        lock.rlock(tid);
      } else {
        DASSERT(atype == atype_t::READ_WRITE);
        lock.wlock(tid);
      }
      DPRINTF("  ok   %s %s v=%" PRId64 "=%" PRId64 " %s\n",
              (atype == atype_t::READ ? "r " : "rw"), key.c_str(),
              lock.version(), vread, lock.to_string().c_str());
    } else {
      DPRINTF("  fail %s %s v=%" PRId64 ">%" PRId64 " %s\n",
              (atype == atype_t::READ ? "r " : "rw"), key.c_str(),
              lock.version(), vread, lock.to_string().c_str());
      // will abort
      return vote_result_t::VERSION_VALIDATION;
    }
    // if didn't exist, check still doesn't exist
  } else {
    // TODO: check if there is a performance penalty, because we just
    // want to know if key exists
    Result res = storage->get(key);
    if (!res.exists) {
      if (atype == atype_t::READ) {
        lock.rlock(tid);
      } else {
        DASSERT(atype == atype_t::READ_WRITE);
        lock.wlock(tid);
      }
      DPRINTF("  ok   %s %s doesn't exist %s\n",
              (atype == atype_t::READ ? "r " : "rw"), key.c_str(),
              lock.to_string().c_str());
    } else {
      DPRINTF("  fail %s %s exists %s\n",
              (atype == atype_t::READ ? "r " : "rw"), key.c_str(),
              lock.to_string().c_str());
      // will abort
      return vote_result_t::VERSION_VALIDATION;
    }
  }
  return vote_result_t::OK;
}

std::string
dm_handler::future_to_string(fmap_t const& fmap, future_t const& future,
                             env_t const& env)
{
  auto const& ftype = future.type;
  if (ftype == ftype_t::READ) {
    auto& fread = future.u.rdata;
    auto env_pair = env.find(fread.key);
    if (env_pair != env.end()) {
      return "{w(" + fread.key + ")=" + env_pair->second + "}";
    } else if (fread.resolved) {
      auto const& rread = fread.data;
      std::string value;
      if (rread.existed) {
        value = rread.value;
      } else {
        value = "<n/a>";
      }
      return "{r(" + fread.key + ")=" + value + "}";
    } else {
      return "{" + fread.key + "}";
    }
  } else if (ftype == ftype_t::POINTER) {
    auto const& fi = future.u.fi;
    auto& pointee = fmap.at(fi);
    return future_to_string(fmap, pointee, env);
  } else if (ftype == ftype_t::GTEQ_FI) {
    auto& bin_op = future.u.binaryop;
    auto const left = future_to_string(fmap, fmap.at(bin_op.left.fi), env);
    auto const right = bin_op.right.value;
    return left + " >= " + right;
  } else if (ftype == ftype_t::NONE) {
    DASSERT(false);
    return "ERROR";
  } else {
    DASSERT(false);
    return "ERROR";
  }
}

void
dm_handler::store_to_disk(tcxx::function<void(bool const&)> cob,
                          std::string const& filename)
{
  storage->store_to_disk(filename);
  cob(true);
}

void
dm_handler::load_from_disk(tcxx::function<void(bool const&)> cob,
                           std::string const& filename)
{
  storage->load_from_disk(
    [&](key_t key, value_t value) {
      vlocks[key].set_version(1);
      DPRINTF("load_from_disk(%s): key=%s value=%s version=%" PRId64 " %s\n",
              filename.c_str(), key.c_str(), value.c_str(),
              vlocks[key].version(), vlocks[key].to_string().c_str());
    },
    filename);
  cob(true);
}
}
}
} // namespace
