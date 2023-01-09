//
//  vlock.cc
//  lsd
//
//  Created by Tiago Vale on 21/01/16.
//  Copyright © 2016 Tiago Vale. All rights reserved.
//

#include "vlock.hh"
#include "../common/definitions.hh"
#include "data_manager.hh"
#include <functional>

namespace novalincs {
namespace cs {
namespace lsd {

vlock::vlock()
  : writing(false)
  , reading(0)
  , current_version(0)
  , waiting()
#ifdef TWOPL
  , plist()
#endif
{
}

version_t
vlock::version() const
{
  return current_version;
}

void
vlock::set_version(version_t const new_version)
{
  current_version = new_version;
}

bool
vlock::validate(version_t const& read_version) const
{
  return read_version == current_version;
}

bool
vlock::can_rlock(tid_t const& tid)
{
  if (wlocked()) {
    return false;
  }
#if defined DEADLOCK_WAITDIE || defined DEADLOCK_WOUNDWAIT
  if (rlocked()) {
#if defined DEADLOCK_WAITDIE
    auto f = tid_less();
#else // DEADLOCK_WOUNDWAIT
    auto f = tid_greater();
#endif
    for (auto const& w : waiting) {
      if (f(tid, w.tid) && w.writer) {
        return false;
      }
    }
  }
#endif
  return true;
}

bool
vlock::can_wlock(tid_t const& tid)
{
  return !wlocked() && !rlocked();
}

void
vlock::rlock(tid_t const& tid)
{
  DASSERT(!writing);
  reading++;
  DASSERT(_readers.find(tid) == _readers.end());
  _readers.insert(tid);
}

void
vlock::wlock(tid_t const& tid)
{
  DASSERT(!writing);
  DASSERT(reading == 0);
  writing = true;
  DASSERT(_readers.size() == reading);
  _writer = tid;
}

void
vlock::runlock(tid_t const& tid)
{
  DASSERT(!writing && reading > 0);
  reading--;
  DASSERT(_readers.find(tid) != _readers.end());
  _readers.erase(tid);
  DASSERT(_readers.size() == reading);
}

void
vlock::wunlock(tid_t const& tid)
{
  DASSERT(writing && reading == 0);
  writing = false;
  DASSERT(_readers.size() == reading);
  DASSERT(_writer == tid);
}

bool
vlock::rlocked() const
{
  return reading > 0;
}

bool
vlock::wlocked() const
{
  return writing;
}

bool
vlock::rpending() const
{
  return !waiting.empty() && !waiting.front().writer;
}

bool
vlock::wpending() const
{
  return !waiting.empty() && waiting.front().writer;
}

int
vlock::readers() const
{
  return reading;
}

void
vlock::block(bool const writer, tid_t const& tid, txnw_t& txnw, txn_cob_t cob)
{
  waiting.emplace_back(writer, tid, txnw, cob);
#if defined DEADLOCK_WAITDIE || defined DEADLOCK_WOUNDWAIT
#if defined DEADLOCK_WAITDIE
  auto f = std::greater<vlq_entry_t>();
#else // DEADLOCK_WOUNDWAIT
  auto f = std::less<vlq_entry_t>();
  DASSERT(txnw.state == txn_state::EXECUTING);
  txnw.state = txn_state::BLOCKED_LOCK;
  txnw.blocking_vlock = this;
#endif
  waiting.sort(f);
#endif
}

size_t
vlock::unblock(txn_queue_t& cobs)
{
  DASSERT(!waiting.empty());
  size_t n = 0;
  bool const first_reader = !waiting.front().writer;
  if (first_reader) {
    do {
#if defined DEADLOCK_WOUNDWAIT
      auto& txnw = waiting.front().txnw;
      DASSERT(txnw.state == txn_state::BLOCKED_LOCK ||
              txnw.state == txn_state::ABORTED);
      if (txnw.state == txn_state::BLOCKED_LOCK) {
        txnw.state = txn_state::QUEUED;
      }
#endif
      cobs.push_back(waiting.front());
      waiting.pop_front();
      n++;
    } while (!waiting.empty() && !waiting.front().writer);
  } else {
#if defined DEADLOCK_WOUNDWAIT
    auto& txnw = waiting.front().txnw;
    DASSERT(txnw.state == txn_state::BLOCKED_LOCK ||
            txnw.state == txn_state::ABORTED);
    if (txnw.state == txn_state::BLOCKED_LOCK) {
      txnw.state = txn_state::QUEUED;
    }
#endif
    cobs.push_back(waiting.front());
    waiting.pop_front();
    n++;
  }
#if defined DEADLOCK_WAITDIE || defined DEADLOCK_WOUNDWAIT
#if defined DEADLOCK_WAITDIE
  auto f = std::greater<vlq_entry_t>();
#else // DEADLOCK_WOUNDWAIT
  auto f = std::less<vlq_entry_t>();
#endif
  cobs.sort(f);
#endif
  return n;
}

bool
vlock::can_block(bool const writer, tid_t const& tid,
                 std::unordered_map<tid_t, txnw_t>& txns,
                 txn_queue_t& pending_txns, dm_handler* dm)
{
  DASSERT((wlocked() && !rlocked()) || (!wlocked() && rlocked()));
#if defined DEADLOCK_WAITDIE
  auto less_than = tid_less();
  if (wlocked()) {
    if (less_than(_writer, tid)) {
      return false;
    }
  }
  if (writer) {
    if (rlocked()) {
      for (tid_t const& reader : _readers) {
        DASSERT(tid != reader);
        if (less_than(reader, tid)) {
          return false;
        }
      }
    }
  }
  return true;
#elif defined DEADLOCK_WOUNDWAIT
  auto greater_than = tid_greater();
  if (wlocked()) {
    if (greater_than(_writer, tid)) {
      auto& writer_txnw = txns.at(_writer);
      if (writer_txnw.state != txn_state::PREPARED) {
        // wound
        auto const prev_state = writer_txnw.state;
        writer_txnw.state = txn_state::ABORTED;
        if (prev_state == txn_state::BLOCKED_LOCK) {
          auto& lock = *writer_txnw.blocking_vlock;
          auto it = lock.waiting.begin();
          for (; it->tid != _writer && it != lock.waiting.end(); ++it)
            ;
          DASSERT(it != lock.waiting.end());
          pending_txns.push_back(*it);
          lock.waiting.erase(it);
          pending_txns.sort(std::less<vlq_entry_t>());
        }
#if defined TWOPL
        else if (prev_state == txn_state::BLOCKED_PREDICATE) {
          auto& pred = *writer_txnw.blocking_pred;
          auto it = pred.waiting.begin();
          for (; it->tid != _writer && it != pred.waiting.end(); ++it)
            ;
          DASSERT(it != pred.waiting.end());
          pending_txns.push_back(*it);
          pred.waiting.erase(it);
          pending_txns.sort(std::less<vlq_entry_t>());
        } else if (prev_state == txn_state::EXECUTING) {
          pending_txns.emplace_back(true, _writer, writer_txnw,
                                    [dm, this] { dm->preempt(_writer); });
          pending_txns.sort(std::less<vlq_entry_t>());
        }
#else
        else {
          DASSERT(prev_state != txn_state::EXECUTING);
        }
#endif
        return true;
      } else {
        // can't wound, can't wait, must die
        return false;
      }
    }
  }
  if (writer) {
    if (rlocked()) {
      std::vector<tid_t> to_wound;
      for (tid_t const& reader : _readers) {
        DASSERT(tid != reader);
        if (greater_than(reader, tid)) {
          auto& reader_txnw = txns.at(reader);
          if (reader_txnw.state != txn_state::PREPARED) {
            // wound
            to_wound.push_back(reader);
          } else {
            // can't wound, can't wait, must die
            return false;
          }
        }
      }
      for (tid_t const victim : to_wound) {
        auto& victim_txnw = txns.at(victim);
        DASSERT(victim_txnw.state != txn_state::PREPARED);
        auto const prev_state = victim_txnw.state;
        victim_txnw.state = txn_state::ABORTED;
        if (prev_state == txn_state::BLOCKED_LOCK) {
          auto& lock = *victim_txnw.blocking_vlock;
          auto it = lock.waiting.begin();
          for (; it->tid != victim && it != lock.waiting.end(); ++it)
            ;
          DASSERT(it != lock.waiting.end());
          pending_txns.push_back(*it);
          lock.waiting.erase(it);
          pending_txns.sort(std::less<vlq_entry_t>());
        }
#if defined TWOPL
        else if (prev_state == txn_state::BLOCKED_PREDICATE) {
          auto& pred = *victim_txnw.blocking_pred;
          auto it = pred.waiting.begin();
          for (; it->tid != victim && it != pred.waiting.end(); ++it)
            ;
          DASSERT(it != pred.waiting.end());
          pending_txns.push_back(*it);
          pred.waiting.erase(it);
          pending_txns.sort(std::less<vlq_entry_t>());
        } else if (prev_state == txn_state::EXECUTING) {
          pending_txns.emplace_back(false, victim, victim_txnw,
                                    [dm, victim] { dm->preempt(victim); });
          pending_txns.sort(std::less<vlq_entry_t>());
        }
#else
        else {
          DASSERT(prev_state != txn_state::EXECUTING);
        }
#endif
      }
    }
  }
  return true;
#elif defined DEADLOCK_NOWAIT
  return false;
#else
  return true;
#endif
}

#ifdef TWOPL
void
vlock::plock(tid_t const& tid, txnw_t& txnw, predicate_t& predicate)
{
  plist.emplace_back(tid, txnw, predicate);
}

void
vlock::punlock(tid_t const& tid, txn_queue_t& cobs)
{
  auto it = plist.begin();
  while (it != plist.end()) {
    auto& pentry = *it;
    if (pentry.owner == tid) {
      pentry.unblock(cobs);
      it = plist.erase(it);
    } else {
      ++it;
    }
  }
}

bool
vlock::plocked() const
{
  return !plist.empty();
}
#endif

std::string
vlock::to_string() const
{
  std::string readers_str = "";
  for (auto const& tid : _readers) {
    readers_str.append(" " + tid);
  }
  std::string waiting_str = "";
  for (auto const& w : waiting) {
    waiting_str.append(" (");
    waiting_str.append((w.writer ? "w" : "r"));
    waiting_str.append(",");
    waiting_str.append(w.tid);
    waiting_str.append(")");
  }
  auto result = "wlocked=" + (writing ? _writer : "no") + " ";
  result.append("rlocked=[" + readers_str + " ] ");
  result.append("waiting=[" + waiting_str + " ] ");
#ifdef TWOPL
  std::string predicates_str = "";
  for (auto& pentry : plist) {
    predicates_str.append(" (");
    predicates_str.append(pentry.owner + ",{");
    auto& fmap = pentry.txnw.local_txn.futures.fmap;
    auto& fi = pentry.predicate.func.future_fi;
    predicates_str.append(future_to_string(fmap, fmap[fi]));
    predicates_str.append("},");
    std::string waiting_str = "";
    for (auto const& w : pentry.waiting) {
      waiting_str.append(" " + w.tid);
    }
    predicates_str.append("[" + waiting_str + " ])");
  }
  result.append("predicates=[" + predicates_str + " ]");
#endif
  return result;
}

#ifdef TWOPL
std::string
vlock::future_to_string(fmap_t const& fmap, future_t const& future) const
{
  auto const& ftype = future.type;
  if (ftype == ftype_t::READ) {
    auto& fread = future.u.rdata;
    if (fread.resolved) {
      auto const& rread = fread.data;
      std::string value;
      if (rread.existed) {
        value = rread.value;
      } else {
        value = "<n/a>";
      }
      return "(" + fread.key + "=" + value + ")";
    } else {
      return fread.key;
    }
  } else if (ftype == ftype_t::POINTER) {
    auto const& fi = future.u.fi;
    auto& pointee = fmap.at(fi);
    return future_to_string(fmap, pointee);
  } else if (ftype == ftype_t::GTEQ_FI) {
    auto& bin_op = future.u.binaryop;
    auto const left = future_to_string(fmap, fmap.at(bin_op.left.fi));
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
#endif

#if defined TWOPL
plist_entry::plist_entry(tid_t const& tid, txnw_t& txnw, predicate_t& predicate)
  : owner(tid)
  , txnw(txnw)
  , predicate(predicate)
  , waiting()
{
}

void
plist_entry::block(tid_t const& tid, txnw_t& txnw, txn_cob_t cob)
{
  waiting.emplace_back(true, tid, txnw, cob);
#if defined DEADLOCK_WAITDIE || defined DEADLOCK_WOUNDWAIT
#if defined DEADLOCK_WAITDIE
  auto f = std::greater<vlq_entry_t>();
#else // DEADLOCK_WOUNDWAIT
  auto f = std::less<vlq_entry_t>();
  txnw.state = txn_state::BLOCKED_PREDICATE;
  txnw.blocking_pred = this;
#endif
  waiting.sort(f);
#endif
}

size_t
plist_entry::unblock(txn_queue_t& cobs)
{
  size_t n = waiting.size();
  for (auto it = waiting.begin(); it != waiting.end(); it = waiting.erase(it)) {
#if defined DEADLOCK_WOUNDWAIT
    auto& txnw = it->txnw;
    DASSERT(txnw.state == txn_state::BLOCKED_PREDICATE ||
            txnw.state == txn_state::ABORTED);
    if (txnw.state == txn_state::BLOCKED_PREDICATE) {
      txnw.state = txn_state::QUEUED;
    }
#endif
    cobs.push_back(*it);
  }
#if defined DEADLOCK_WAITDIE || defined DEADLOCK_WOUNDWAIT
#if defined DEADLOCK_WAITDIE
  auto f = std::greater<vlq_entry_t>();
#else // DEADLOCK_WOUNDWAIT
  auto f = std::less<vlq_entry_t>();
#endif
  cobs.sort(f);
#endif
  return n;
}

bool
plist_entry::can_block(tid_t const& tid,
                       std::unordered_map<tid_t, txnw_t>& txns,
                       txn_queue_t& pending_txns, dm_handler* dm)
{
  DASSERT(owner != tid);
#if defined DEADLOCK_WAITDIE
  auto less_than = tid_less();
  return !less_than(owner, tid);
#elif defined DEADLOCK_WOUNDWAIT
  auto greater_than = tid_greater();
  if (greater_than(owner, tid)) {
    auto& owner_txnw = txns.at(owner);
    if (owner_txnw.state != txn_state::PREPARED) {
      // wound
      auto const prev_state = owner_txnw.state;
      owner_txnw.state = txn_state::ABORTED;
      if (prev_state == txn_state::BLOCKED_LOCK) {
        auto& lock = *owner_txnw.blocking_vlock;
        auto it = lock.waiting.begin();
        for (; it->tid != owner && it != lock.waiting.end(); ++it)
          ;
        DASSERT(it != lock.waiting.end());
        pending_txns.push_back(*it);
        lock.waiting.erase(it);
        pending_txns.sort(std::less<vlq_entry_t>());
      } else if (prev_state == txn_state::BLOCKED_PREDICATE) {
        auto& pred = *owner_txnw.blocking_pred;
        auto it = pred.waiting.begin();
        for (; it->tid != owner && it != pred.waiting.end(); ++it)
          ;
        DASSERT(it != pred.waiting.end());
        pending_txns.push_back(*it);
        pred.waiting.erase(it);
        pending_txns.sort(std::less<vlq_entry_t>());
      } else if (prev_state == txn_state::EXECUTING) {
        pending_txns.emplace_back(false, owner, owner_txnw,
                                  [dm, this] { dm->preempt(owner); });
        pending_txns.sort(std::less<vlq_entry_t>());
      }
      return true;
    } else {
      // can't wound, can't wait, must die
      return false;
    }
  } else {
    return true;
  }
#elif defined DEADLOCK_NOWAIT
  return false;
#else
  return true;
#endif
}
#endif
}
}
} // namespace
