//
//  definitions.hh
//  lsd
//
//  Created by Tiago Vale on 23/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include "../thrift/gen-cpp/types_types.h"
#include <list>
#include <map>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace novalincs {
namespace cs {
namespace lsd {

/* debugging
 */
#ifdef DEBUG
#include <cstdio>
#define DPRINTF(...) fprintf(stdout, __VA_ARGS__)
#define DASSERT(x) assert(x)
#else
#define DPRINTF(...)
#define DASSERT(x) assert(x)
#endif

/* common types
 */
typedef std::map<key_t, access_t>::iterator aiterator_t;
typedef std::map<key_t, access_t>::const_iterator const_aiterator_t;
#if defined TWOPL
typedef std::unordered_set<key_t>::iterator piterator_t;
#else
typedef pset_t::iterator piterator_t;
#endif
typedef tcxx::function<void()> txn_cob_t;

typedef std::unordered_set<key_t> key_set_t;
typedef std::unordered_map<key_t, value_t> env_t;

#if defined DEADLOCK_WOUNDWAIT
enum class txn_state
{
  EXECUTING,
  BLOCKED_LOCK,
#if defined TWOPL
  BLOCKED_PREDICATE,
  PREEMPTED,
#endif
  QUEUED,
  PREPARED,
  ABORTED,
};
#endif

#if defined DEADLOCK_WOUNDWAIT
class vlock;
#if defined TWOPL
class plist_entry;
#endif
#endif

typedef struct txnw_s
{
  txn_t txn;
  rfmap_t rfmap;
#if defined TWOPL
  txn_t local_txn;
  klist_t klist;
  mget_t mget_return;
  predicate_t predicate;
  key_set_t plocked_keys;
  key_set_t keys_to_validate;
  env_t env;
#endif
#if defined DEADLOCK_WOUNDWAIT
  txn_state state;
  vlock* blocking_vlock;
#if defined TWOPL
  plist_entry* blocking_pred;
#endif
#endif

  txnw_s()
    : txn()
    , rfmap()
#ifdef TWOPL
    , plocked_keys()
    , keys_to_validate()
#endif
#ifdef DEADLOCK_WOUNDWAIT
    , state(txn_state::EXECUTING)
#endif
  {
  }
} txnw_t;
}
}
}
