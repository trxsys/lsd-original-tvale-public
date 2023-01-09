//
//  vlock.hh
//  lsd
//
//  Created by Tiago Vale on 21/01/16.
//  Copyright © 2016 Tiago Vale. All rights reserved.
//
#pragma once

#include "../common/definitions.hh"
#include "../thrift/gen-cpp/types_types.h"
#include <list>
#include <string>
#include <unordered_set>

namespace novalincs {
namespace cs {
namespace lsd {

class dm_handler;

struct tid_less
{
  bool operator()(const std::string& lhs, const std::string& rhs) const
  {
    auto lhs_sep = lhs.find_first_of("-");
    auto rhs_sep = rhs.find_first_of("-");
    auto lhs_tid = std::stoi(lhs.substr(0, lhs_sep));
    auto rhs_tid = std::stoi(rhs.substr(0, rhs_sep));
    if (lhs_tid < rhs_tid) {
      return true;
    } else if (lhs_tid > rhs_tid) {
      return false;
    } else {
      auto lhs_cid = lhs.substr(lhs_sep + 1);
      auto rhs_cid = rhs.substr(rhs_sep + 1);
      return lhs_cid < rhs_cid;
    }
  }
};

struct tid_greater
{
  bool operator()(const std::string& lhs, const std::string& rhs) const
  {
    auto lhs_sep = lhs.find_first_of("-");
    auto rhs_sep = rhs.find_first_of("-");
    auto lhs_tid = std::stoi(lhs.substr(0, lhs_sep));
    auto rhs_tid = std::stoi(rhs.substr(0, rhs_sep));
    if (lhs_tid > rhs_tid) {
      return true;
    } else if (lhs_tid < rhs_tid) {
      return false;
    } else {
      auto lhs_cid = lhs.substr(lhs_sep + 1);
      auto rhs_cid = rhs.substr(rhs_sep + 1);
      return lhs_cid > rhs_cid;
    }
  }
};

typedef struct vlq_entry_s
{
  bool writer;
  tid_t tid;
  struct txnw_s& txnw;
  txn_cob_t cob;

  vlq_entry_s(bool writer, tid_t tid, struct txnw_s& txnw, txn_cob_t cob)
    : writer(writer)
    , tid(tid)
    , txnw(txnw)
    , cob(cob)
  {
  }

  bool operator<(struct vlq_entry_s const& other) const
  {
    auto f = tid_less();
    return f(tid, other.tid);
  }

  bool operator>(struct vlq_entry_s const& other) const
  {
    auto f = tid_greater();
    return f(tid, other.tid);
  }
} vlq_entry_t;
typedef std::list<vlq_entry_t> txn_queue_t;

#if defined TWOPL
class plist_entry
{
public:
  tid_t const owner;
  txnw_t& txnw;
  predicate_t& predicate;
  txn_queue_t waiting;

  plist_entry(tid_t const& tid, txnw_t& txnw, predicate_t& predicate);
  void block(tid_t const& tid, txnw_t& txnw, txn_cob_t cob);
  size_t unblock(txn_queue_t& cobs);
  bool can_block(tid_t const& tid, std::unordered_map<tid_t, txnw_t>& txns,
                 txn_queue_t& pending_txns, dm_handler* dm);
};
#endif

/**
 * a versioned lock
 * when there is an active writer:
 *     writing = true and reading = 0
 * when there are active readers:
 *     reading > 0 and writing = false
 * waiting is a queue of transactions waiting for the lock, of the form:
 *     (wants to write?, continuation object)
 */
class vlock
{
  friend class plist_entry;
#ifdef TWOPL
  typedef std::list<plist_entry> plist_t;
#endif

protected:
  bool writing;
  std::size_t reading;
  version_t current_version;
  txn_queue_t waiting;

public:
  tid_t _writer;
  std::unordered_set<tid_t> _readers;
#ifdef TWOPL
  plist_t plist;
#endif

public:
  vlock();
  version_t version() const;
  void set_version(version_t const new_version);
  bool validate(version_t const& read_version) const;
  bool can_rlock(tid_t const& tid);
  bool can_wlock(tid_t const& tid);
  void rlock(tid_t const& tid);
  void wlock(tid_t const& tid);
  void runlock(tid_t const& tid);
  void wunlock(tid_t const& tid);
  bool rlocked() const;
  bool wlocked() const;
  bool rpending() const;
  bool wpending() const;
  int readers() const;
  void block(bool const writer, tid_t const& tid, txnw_t& txnw, txn_cob_t cob);
  size_t unblock(txn_queue_t& cobs);
  bool can_block(bool const writer, tid_t const& tid,
                 std::unordered_map<tid_t, txnw_t>& txns,
                 txn_queue_t& pending_txns, dm_handler* dm);
#ifdef TWOPL
  void plock(tid_t const& tid, txnw_t& txnw, predicate_t& predicate);
  void punlock(tid_t const& tid, txn_queue_t& cobs);
  bool plocked() const;
#endif
  std::string to_string() const;

#ifdef TWOPL
private:
  std::string future_to_string(fmap_t const& fmap,
                               future_t const& future) const;
#endif
};
}
}
} // namespace
