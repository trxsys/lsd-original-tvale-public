//
//  data_manager.hh
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include "../common/definitions.hh"
#include "../thrift/gen-cpp/data_manager.h"
#include "../thrift/gen-cpp/types_types.h"
#include "storage.hh"
#include "vlock.hh"

namespace novalincs {
namespace cs {
namespace lsd {

/**
 * TODO
 */
typedef std::function<value_t const*(fmap_t&, future_t&, bool const,
                                     env_t const&)>
  lambda_op;
typedef std::function<value_t(value_t const* const)> lambda_unop;
typedef std::function<value_t(value_t const* const, value_t const* const)>
  lambda_binop;

#ifndef NDEBUG
typedef std::function<std::string(std::string const)> lambda_unop_print;
typedef std::function<std::string(std::string const, std::string const)>
  lambda_binop_print;
#endif // NDEBUG

class dm_handler : virtual public data_managerCobSvIf
{
protected:
  // storage backend
  std::unique_ptr<Storage> storage;
  // key -> vlock map
  std::unordered_map<key_t, vlock> vlocks;
  // tid -> transaction map
  std::unordered_map<tid_t, txnw_t> txns;
  lambda_op ops[ftype_t::TOTAL];

public:
  dm_handler(std::string& storage_impl, std::string& id);
  void get_notxn(tcxx::function<void(rread_t const&)> cob, key_t const& key);
  void get(tcxx::function<void(rread_t const&)> cob, tid_t const& tid,
           key_t const& key, bool const for_update);
  void multiget_notxn(tcxx::function<void(mget_t const&)> cob,
                      klist_t const& klist);
  void multiget(tcxx::function<void(mget_t const&)> cob, tid_t const& tid,
                klist_t const& klist, bool const for_update);
  void opc_commit(tcxx::function<void(vote_t const&)> cob, txn_t const& txn);
  void tpc_prepare(tcxx::function<void(vote_t const&)> cob, txn_t const& txn);
  void tpc_prepare2(tcxx::function<void(vote_t const&)> cob, txn_t const& txn,
                    rfmap_t const& rfmap);
  void tpc_commit(tcxx::function<void()> cob, tid_t const& tid,
                  rfmap_t const& rfmap);
  void tpc_abort(tcxx::function<void()> cob, tid_t const& tid);
  void opc_assert(tcxx::function<void(assert_t const&)> cob, tid_t const& tid,
                  predicate_t const& predicate, fmap_t const& fmap);
  void store_to_disk(tcxx::function<void(bool const&)> cob,
                     std::string const& filename);
  void load_from_disk(tcxx::function<void(bool const&)> cob,
                      std::string const& filename);
#if defined TWOPL && defined DEADLOCK_WOUNDWAIT
  void preempt(tid_t const& tid);
#endif

private:
  void get_cob(tcxx::function<void(rread_t const&)> cob, tid_t const& tid,
               key_t const& key, bool const for_update);
  void multiget_cob(tcxx::function<void(mget_t const&)> cob, tid_t const& tid,
                    klist_t const& klist, klist_t::const_iterator kit,
                    bool const for_update);
  void opc_commit_cob(tcxx::function<void(vote_t const&)> cob, txnw_t& txn,
                      aiterator_t ait, bool fwset_done, piterator_t pit);
  void opc_assert_cob(tcxx::function<void(assert_t const&)> cob,
                      tid_t const& tid, predicate_t const& predicate,
                      fmap_t& fmap, klist_t::const_iterator kit);
  void tpc_prepare_cob(tcxx::function<void(vote_t const&)> cob, txnw_t& txnw,
                       aiterator_t ait, bool fwset_done, piterator_t pit);
  bool prepare(tcxx::function<void(vote_t const&)> cob, txnw_t& txnw,
               aiterator_t& ait, piterator_t& pit, txn_queue_t& pending_txns,
               tcxx::function<void(tcxx::function<void(vote_t const&)>, txnw_t&,
                                   aiterator_t, bool, piterator_t)>
                 caller,
               vote_result_t::type& result);
  void resolve_remote_futures(txnw_t& txnw, rfmap_t const& rfmap);
  value_t const* resolve(fmap_t& fmap, future_t& future,
                         bool const transient = false,
                         env_t const& env = env_t());
  vote_result_t::type trylock_fwset(txnw_t& txnw);
  bool validate_predicates(
    tcxx::function<void(vote_t const&)> cob, txnw_t& txnw, aiterator_t& ait,
    piterator_t& pit, txn_queue_t& pending_txns,
    tcxx::function<void(tcxx::function<void(vote_t const&)>, txnw_t&,
                        aiterator_t, bool, piterator_t)>
      caller,
    vote_result_t::type& result);
  void commit(txnw_t& txnw, txn_queue_t& pending_txns);
  void abort(txnw_t& txnw, const_aiterator_t begin, const_aiterator_t up_to,
             const_aiterator_t end, txn_queue_t& pending_txns);
  void process(txn_queue_t& pending_txns);
  void prepare_read(txnw_t& txnw, key_t const& key, access_t& access,
                    atype_t::type const& atype, txn_queue_t& pending_txns);
  vote_result_t::type prepare_read(tid_t const& tid, key_t const& key,
                                   read_t& rinfo, atype_t::type const& atype);
  std::string future_to_string(fmap_t const& fmap, future_t const& future,
                               env_t const& env = env_t());
};
}
}
} // namespace
