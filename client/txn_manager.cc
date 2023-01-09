//
//  txn_manager.cc
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "txn_manager.hh"
#include "../common/definitions.hh"
#include "../common/sharding_policy.hh"
#include <algorithm>
#include <cstdlib>
#include <forward_list>
#include <future>
#include <iostream>
#include <set>

namespace novalincs {
namespace cs {
namespace lsd {

#if defined TWOPL && defined TWOPC_SEQUENTIAL
static_assert(
  false,
  "Sequential 2PC (TWOPC_SEQUENTIAL) only compatible without 2PL (TWOPL)");
#endif

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

tm_handler::tm_handler(servers_t const& servers)
  : n_servers(servers.size())
{
  for (auto const& server : servers) {
    auto const& host = server.first;
    auto const& port = server.second;
    shared_ptr<TTransport> transport(new THttpClient(host, port, "/"));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    clients.emplace_back(protocol);
    transport->open();
    transports.push_back(transport);
  }
  assert(clients.size() == n_servers);
  assert(transports.size() == n_servers);
  // setup operations vtable
  ops[ftype_t::READ] = [this](txn_t& txn, future_t& future,
                              mgmap_t const& kr_map,
                              bool const transient) -> value_t const* {
    auto& fread = future.u.rdata;
    if (fread.resolved) {
      auto& rread = fread.data;
      if (rread.existed) {
        return &rread.value;
      } else {
        return nullptr;
      }
    }
    auto& key = fread.key;
    auto it = kr_map.find(key);
    DASSERT(it != kr_map.end());
    auto& rread = it->second;
    if (!transient) {
      fread.resolved = true;
    }
    if (rread.existed) {
      return &rread.value;
    } else {
      return nullptr;
    }
  };
  ops[ftype_t::POINTER] = [this](txn_t& txn, future_t& future,
                                 mgmap_t const& kr_map,
                                 bool const transient) -> value_t const* {
    auto const& fi = future.u.fi;
    auto& pointee = txn.futures.fmap[fi];
    return ops[pointee.type](txn, pointee, kr_map, transient);
  };
  ops[ftype_t::ADD_FI] = [this](txn_t& txn, future_t& future,
                                mgmap_t const& kr_map,
                                bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      int const result = std::stoi(*left) + std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::ADD_FD] = [this](txn_t& txn, future_t& future,
                                mgmap_t const& kr_map,
                                bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      double const result = std::stod(*left) + std::stod(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::SUB_FI] = [this](txn_t& txn, future_t& future,
                                mgmap_t const& kr_map,
                                bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      int const result = std::stoi(*left) - std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::SUB_FD] = [this](txn_t& txn, future_t& future,
                                mgmap_t const& kr_map,
                                bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      double const result = std::stod(*left) - std::stod(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::CONCAT_FS] = [this](txn_t& txn, future_t& future,
                                   mgmap_t const& kr_map,
                                   bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      bin_op.value = *left + *right;
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::CONCAT_SF] = [this](txn_t& txn, future_t& future,
                                   mgmap_t const& kr_map,
                                   bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto const* left = &bin_op.left.value;
      auto& right_f = fmap[bin_op.right.fi];
      auto const* right = ops[right_f.type](txn, right_f, kr_map, transient);
      bin_op.value = *left + *right;
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::GTEQ_FI] = [this](txn_t& txn, future_t& future,
                                 mgmap_t const& kr_map,
                                 bool const transient) -> value_t const* {
    auto& bin_op = future.u.binaryop;
    if (bin_op.resolved) {
      return &bin_op.value;
    } else {
      auto& fmap = txn.futures.fmap;
      auto& left_f = fmap[bin_op.left.fi];
      auto const* left = ops[left_f.type](txn, left_f, kr_map, transient);
      auto const* right = &bin_op.right.value;
      bool const result = std::stoi(*left) >= std::stoi(*right);
      bin_op.value = std::to_string(result);
      bin_op.resolved = !transient;
      return &bin_op.value;
    }
  };
  ops[ftype_t::NONE] = [](txn_t& txn, future_t& future, mgmap_t const& kr_map,
                          bool const transient) -> value_t const* {
    DASSERT(false);
    value_t* p = nullptr;
    *p = 1;
    return p;
  };
}

void
tm_handler::get_notxn(rread_t& _return, key_t const& key)
{
  auto const& server_id = get_server(key, n_servers);
  clients[server_id].get_notxn(_return, key);
}

void
tm_handler::get(rread_t& _return, tid_t const& tid, key_t const& key,
                bool for_update)
{
#if !defined TWOPL
  for_update = false;
#endif
  auto const& server_id = get_server(key, n_servers);
  clients[server_id].get(_return, tid, key, for_update);
}

void
tm_handler::multiget_notxn(mget_t& _return, klist_t const& klist)
{
  std::unordered_map<std::size_t, std::vector<key_t>> server_keys;
  for (auto const key : klist) {
    auto const& server_id = get_server(key, n_servers);
    server_keys[server_id].push_back(key);
  }
  for (auto& entry : server_keys) {
    auto const& server_id = entry.first;
    auto& keys = entry.second;
    keys.shrink_to_fit();
    clients[server_id].send_multiget_notxn(keys);
  }
  for (auto& entry : server_keys) {
    auto const& server_id = entry.first;
    mget_t mget;
    clients[server_id].recv_multiget_notxn(mget);
    _return.mgmap.insert<mgmap_t::const_iterator>(mget.mgmap.cbegin(),
                                                  mget.mgmap.cend());
  }
}

void
tm_handler::multiget(mget_t& _return, tid_t const& tid, klist_t const& _klist,
                     bool for_update)
{
  klist_t& klist = const_cast<klist_t&>(_klist);
#if !defined TWOPL
  for_update = false;
#endif
  bool global_ok = true;
  _return.status = vote_result_t::OK;
  std::unordered_map<std::size_t, std::vector<key_t>> server_keys;
  std::sort(klist.begin(), klist.end());
  for (auto const key : klist) {
    auto const& server_id = get_server(key, n_servers);
    server_keys[server_id].push_back(key);
  }
  for (auto& entry : server_keys) {
    auto const& server_id = entry.first;
    auto& keys = entry.second;
    keys.shrink_to_fit();
    clients[server_id].send_multiget(tid, keys, for_update);
  }
  for (auto& entry : server_keys) {
    auto const& server_id = entry.first;
    mget_t mget;
    clients[server_id].recv_multiget(mget);
    _return.mgmap.insert<mgmap_t::const_iterator>(mget.mgmap.cbegin(),
                                                  mget.mgmap.cend());
    global_ok = global_ok && mget.status == vote_result_t::OK;
  }
#ifdef TWOPL
  if (!global_ok) {
    _return.status = vote_result_t::DEADLOCK_AVOIDANCE;
  }
#endif
#if !defined TWOPL
  DASSERT(global_ok);
#endif
}

void
tm_handler::is_true(assert_t& _return, txn_t const& txn,
                    predicate_t const& predicate)
{
#if !defined TWOPL
  DASSERT(false);
#endif
  std::unordered_set<std::size_t> server_ids;
  for (auto& key : predicate.func.keys) {
    auto const& server_id = get_server(key, n_servers);
    server_ids.insert(server_id);
  }
  DASSERT(server_ids.size() == 1);
  auto const& server_id = *server_ids.begin();
  clients[server_id].opc_assert(_return, txn.tid, predicate, txn.futures.fmap);
}

void
tm_handler::commit(vote_t& _return, txn_t const& _txn)
{
  txn_t& txn = const_cast<txn_t&>(_txn);
  if (txn.rwset.size() == 0) {
    DASSERT(txn.future_wset.size() == 0);
    _return.result = vote_result_t::OK;
    return;
  }
  std::map<std::size_t, txn_t> servers;
  fwset_t fwset;
  // partition-specific transaction
  for (auto const& entry : txn.rwset) {
    auto const& key = entry.first;
    auto const server_id = get_server(key, n_servers);
    auto it = servers.find(server_id);
    if (it == servers.end()) {
      auto& t = servers[server_id];
      t.futures = txn.futures;
    }
    servers[server_id].rwset.insert(entry);
  }
  _return.result = vote_result_t::OK;
  // handle future wset
  for (auto& entry : txn.future_wset) {
    auto& key_func = entry.func;
    auto& key_f = txn.futures.fmap.at(key_func.future_fi);
    std::size_t server_id = get_server(key_f, n_servers, txn.futures.fmap);
    bool const server_known = server_id != SHARDING_UNKNOWN;
    if (server_known) {
      auto& klist = key_func.keys;
      bool server_can_resolve = true;
      for (auto& k : klist) {
        auto const& server_dep = get_server(k, n_servers);
        server_can_resolve = server_can_resolve && server_id == server_dep;
      }
      if (server_can_resolve) {
        auto it = servers.find(server_id);
        if (it == servers.end()) {
          auto& t = servers[server_id];
          t.futures = txn.futures;
        }
        servers[server_id].future_wset.push_back(entry);
      } else {
        fwset.push_back(entry);
      }
    } else {
      fwset.push_back(entry);
    }
  }
  DASSERT(servers.size() >= 1);
  bool const single_partition = servers.size() == 1;
  auto& server_txn = servers.begin()->second;
  bool const resolvable_fwset =
    server_txn.future_wset.size() == txn.future_wset.size();
  if (single_partition && resolvable_fwset) {
    // one-phase commit, single-partition, transaction (no unknown wset keys)
    DASSERT(fwset.size() == 0);
    auto const& server_id = servers.cbegin()->first;
    clients[server_id].opc_commit(_return, txn);
  } else {
    // two-phase commit, potentially multi-partition, transaction
    // prepare
    ;
#if defined TWOPL
    auto& prepared = servers;
#endif
#if !defined TWOPL
    std::vector<std::size_t> prepared;
#endif
    _return.result = vote_result_t::OK;
#if !defined TWOPC_SEQUENTIAL
    for (auto& entry : servers) {
      auto const& server_id = entry.first;
      auto& server_txn = entry.second;
      server_txn.tid = txn.tid;
      clients[server_id].send_tpc_prepare(server_txn);
    }
    for (auto& entry : servers) {
      auto const& server_id = entry.first;
      vote_t server_vote;
      clients[server_id].recv_tpc_prepare(server_vote);
      if (server_vote.result == vote_result_t::OK) {
#if !defined TWOPL
        prepared.push_back(server_id);
#endif
        if (_return.result == vote_result_t::OK) {
          _return.rfmap.insert<rfmap_t::const_iterator>(
            server_vote.rfmap.begin(), server_vote.rfmap.end());
        }
      } else {
        _return.result = server_vote.result;
      }
    }
#else
    for (auto& entry : servers) {
      auto const& server_id = entry.first;
      auto& server_txn = entry.second;
      server_txn.tid = txn.tid;
      vote_t server_vote;
      clients[server_id].tpc_prepare(server_vote, server_txn);
      if (server_vote.result == vote_result_t::OK) {
        prepared.push_back(server_id);
        _return.rfmap.insert<rfmap_t::const_iterator>(server_vote.rfmap.begin(),
                                                      server_vote.rfmap.end());
      } else {
        _return.result = server_vote.result;
        break;
      }
    }
#endif
    if (_return.result == vote_result_t::OK) {
      resolve_reads(txn, _return.rfmap);
      std::unordered_set<std::size_t> prepared_set;
#ifdef TWOPL
      for (auto& entry : prepared) {
        prepared_set.insert(entry.first);
      }
#endif
#if !defined TWOPL
      prepared_set.insert<std::vector<std::size_t>::const_iterator>(
        prepared.cbegin(), prepared.cend());
#endif
      // prepare ok
      if (fwset.size() > 0) {
        // prepare2
        std::unordered_map<std::size_t, txn_t> servers_fwset;
        for (auto& entry : fwset) {
          auto& key_func = entry.func;
          auto& key_f = txn.futures.fmap[key_func.future_fi];
          auto* key = resolve(txn, key_f);
          auto const& server_id = get_server(*key, n_servers);
          DASSERT(server_id != SHARDING_UNKNOWN);
          auto sfwit = servers_fwset.find(server_id);
          auto sit = servers.find(server_id);
          if (sfwit == servers_fwset.end() && sit == servers.end()) {
            auto& t = servers_fwset[server_id];
            t.futures = txn.futures;
          }
          servers_fwset[server_id].future_wset.push_back(entry);
        }
        for (auto& entry : servers_fwset) {
          auto& server_id = entry.first;
          auto& server_txn = entry.second;
          server_txn.tid = txn.tid;
          clients[server_id].send_tpc_prepare2(server_txn, _return.rfmap);
        }
        for (auto& entry : servers_fwset) {
          auto& server_id = entry.first;
          vote_t server_vote;
          clients[server_id].recv_tpc_prepare2(server_vote);
          if (server_vote.result == vote_result_t::OK) {
            prepared_set.insert(server_id);
          } else {
            _return.result = server_vote.result;
          }
        }
        if (_return.result != vote_result_t::OK) {
          // prepare ok, prepare2 fail, abort
          for (auto& server_id : prepared_set) {
            clients[server_id].send_tpc_abort(txn.tid);
          }
          for (auto& server_id : prepared_set) {
            clients[server_id].recv_tpc_abort();
          }
          return;
        }
      }
      // prepare ok, (prepare2 ok)
      if (txn.pset.size() > 0) {
#ifdef TWOPL
        DASSERT(false);
#endif
        for (auto& pred : txn.pset) {
          auto& fi = pred.func.future_fi;
          auto& future = txn.futures.fmap[fi];
          auto& expected = pred.expected;
          auto* result = resolve(txn, future);
          bool const pred_ok = *result == std::to_string(expected);
          if (!pred_ok) {
            _return.result = vote_result_t::PREDICATE_VALIDATION;
            break;
          }
        }
        if (_return.result != vote_result_t::OK) {
          // prepare ok, (prepare2 ok,) predicates fail, abort
          for (auto& server_id : prepared_set) {
            clients[server_id].send_tpc_abort(txn.tid);
          }
          for (auto& server_id : prepared_set) {
            clients[server_id].recv_tpc_abort();
          }
          return;
        }
      }
      // prepare ok, (prepare2 ok, predicates ok,) commit
      for (auto& server_id : prepared_set) {
        clients[server_id].send_tpc_commit(txn.tid, _return.rfmap);
      }
      for (auto& server_id : prepared_set) {
        clients[server_id].recv_tpc_commit();
      }
    } else {
      // prepare fail, abort
      for (auto& entry : prepared) {
#ifdef TWOPL
        auto const& server_id = entry.first;
#endif
#if !defined TWOPL
        auto const& server_id = entry;
#endif
        clients[server_id].send_tpc_abort(txn.tid);
      }
      for (auto& entry : prepared) {
#ifdef TWOPL
        auto const& server_id = entry.first;
#endif
#if !defined TWOPL
        auto const& server_id = entry;
#endif
        clients[server_id].recv_tpc_abort();
      }
    }
  }
}

void
tm_handler::rollback(txn_t const& txn)
{
#if !defined TWOPL
  DASSERT(false);
#endif
  if (txn.rwset.size() == 0) {
    DASSERT(txn.future_wset.size() == 0);
    return;
  }
  std::unordered_set<std::size_t> servers;
  for (auto& entry : txn.rwset) {
    auto& key = entry.first;
    auto& access = entry.second;
    auto& atype = access.type;
    if (atype == atype_t::READ || atype == atype_t::READ_WRITE) {
      auto const& server_id = get_server(key, n_servers);
      servers.insert(server_id);
    }
  }
  for (auto& server_id : servers) {
    clients[server_id].send_tpc_abort(txn.tid);
  }
  for (auto& server_id : servers) {
    clients[server_id].recv_tpc_abort();
  }
}

void
tm_handler::resolve_reads(txn_t& txn, rfmap_t const& rfmap)
{
  auto& fmap = txn.futures.fmap;
  for (auto& entry : rfmap) {
    auto& fi = entry.first;
    auto& rread = entry.second;
    auto& future_read = fmap[fi];
    DASSERT(future_read.type == ftype_t::READ);
    auto& fread = future_read.u.rdata;
    DASSERT(!fread.resolved);
    fread.data = rread;
    fread.resolved = true;
  }
}

value_t const* const
tm_handler::resolve(txn_t& txn, future_t& future, mgmap_t const& kr_map,
                    bool const transient)
{
  auto& ftype = future.type;
  return ops[ftype](txn, future, kr_map, transient);
}
}
}
} // namespace
