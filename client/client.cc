//
//  client.cc
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "client.hh"
#include "../common/definitions.hh"
#include <fstream>
#include <sstream>
#include <vector>

namespace novalincs {
namespace cs {
namespace lsd {

fwrapper_t lsd_client::NULL_FUTURE = {};

lsd_client::lsd_client(cid_t const id)
  : tm(nullptr)
  , cid(id)
  , tid_ctr(0)
{
  config cfg;
  auto const& slist = cfg.servers();
  tm.reset(new tm_handler(slist));
  assume_predicates = true;
}

void
lsd_client::begin()
{
  txn.rwset.clear();
  txn.future_wset.clear();
  txn.pset.clear();
  txn.futures.num_fi = 0;
  txn.futures.fmap.clear();
  txn.tid = std::to_string(tid_ctr) + "-" + cid;
}

result_t
lsd_client::get(key_t const& key, bool const for_update)
{
  auto& rwset = txn.rwset;
  // simplify code by assuming:
  // a. no more than one get for a specific key per transaction
  // b. no get for a specific key if put before
  DASSERT(rwset.find(key) == rwset.end());
  rread_t rread;
  tm->get(rread, txn.tid, key, for_update);
  if (rread.status != vote_result_t::OK) {
#if !defined TWOPL
    DASSERT(false);
#endif
    auto const why = "get(" + key + "," + std::to_string(for_update) + ")";
    retry(why);
  }
  auto& access = rwset[key];
  read_t* read;
#ifdef TWOPL
  if (for_update) {
    access.type = atype_t::READ_WRITE;
    wtype_t::type wtype = rread.existed ? wtype_t::PUT : wtype_t::REMOVE;
    auto& write = access.u.rw.w;
    write.type = wtype;
    write.future = false;
    write.u.value = rread.value;
    read = &access.u.rw.r;
  } else {
#endif
    access.type = atype_t::READ;
    read = &access.u.r;
#ifdef TWOPL
  }
#endif
  read->future = false;
  auto& concrete = read->u.concrete;
  concrete = rread;
  result_t res(concrete.existed, concrete.value);
  return res;
}
fptr_t
lsd_client::get_lsd(key_t const& key)
{
  auto& rwset = txn.rwset;
  // simplify code by assuming:
  // a. no more than one get for a specific key per transaction
  // b. no get for a specific key if put before
  DASSERT(rwset.find(key) == rwset.end());
  // create future read
  future_t future_read;
  future_read.type = ftype_t::READ;
  auto& fread = future_read.u.rdata;
  fread.resolved = false;
  fread.key = key;
  fptr_t res = make_future(future_read);
  // create future read pointer
  auto& access = rwset[key];
  access.type = atype_t::READ;
  auto& read = access.u.r;
  read.future = true;
  read.u.future_fi = res.u.fi;
  return res;
}

result_map_t
lsd_client::multiget(klist_t klist, bool const for_update)
{
  auto& rwset = txn.rwset;
  // simplify code by assuming:
  // a. no more than one get for a specific key per transaction
  // b. no get for a specific key if put before
  result_map_t res;
#ifndef NDEBUG
  for (auto& key : klist) {
    DASSERT(rwset.find(key) == rwset.end());
  }
#endif
  mget_t mget;
  tm->multiget(mget, txn.tid, klist, for_update);
  for (auto& entry : mget.mgmap) {
    auto& key = entry.first;
    auto& rread = entry.second;
    auto& access = rwset[key];
    read_t* read;
#ifdef TWOPL
    if (for_update) {
      access.type = atype_t::READ_WRITE;
      wtype_t::type wtype = rread.existed ? wtype_t::PUT : wtype_t::REMOVE;
      auto& write = access.u.rw.w;
      write.type = wtype;
      write.future = false;
      write.u.value = rread.value;
      read = &access.u.rw.r;
    } else {
#endif
      access.type = atype_t::READ;
      read = &access.u.r;
#ifdef TWOPL
    }
#endif
    read->future = false;
    auto& concrete = read->u.concrete;
    concrete = rread;
    result_t result(rread.existed, rread.value);
    res.emplace(key, result);
  }
  if (mget.status != vote_result_t::OK) {
#if !defined TWOPL
    DASSERT(false);
#endif
    std::string why = "multiget([";
    for (auto& key : klist) {
      why.append(" " + key);
    }
    why.append(" ],");
    why.append(std::to_string(for_update));
    why.append(")");
    retry(why);
  }
  return res;
}
fptr_map_t
lsd_client::multiget_lsd(klist_t klist)
{
  // simplify code by assuming:
  // a. no more than one get for a specific key per transaction
  // b. no get for a specific key if put before
  fptr_map_t res;
  for (auto& key : klist) {
    DASSERT(txn.rwset.find(key) == txn.rwset.end());
    res[key] = get_lsd(key);
  }
  return res;
}

bool
lsd_client::is_true(predicate_t& predicate, assumption const assume)
{
#ifdef TWOPL
  DASSERT(assume == assumption::NONE);
  assert_t assert_res;
  tm->is_true(assert_res, txn, predicate);
  if (assert_res.status != vote_result_t::OK) {
    retry("is_true");
  }
  return assert_res.result;
#endif
#if !defined TWOPL
  auto& pset = txn.pset;
  auto& fmap = txn.futures.fmap;
  if (assume != assumption::NONE && assume_predicates) {
    DASSERT(assume == assumption::TRUE || assume == assumption::FALSE);
    predicate.expected = assume == assumption::TRUE ? true : false;
  } else {
    auto& klist = predicate.func.keys;
    mget_t mget;
    tm->multiget(mget, txn.tid, klist, false);
    DASSERT(mget.status == vote_result_t::OK);
    auto* res =
      tm->resolve(txn, fmap[predicate.func.future_fi], mget.mgmap, true);
    predicate.expected = *res == std::to_string(true);
  }
  pset.push_back(predicate);
  return predicate.expected;
#endif
}

void
lsd_client::put(key_t const& key, value_t const& value)
{
  write(key, wtype_t::PUT, value);
}
void
lsd_client::put_lsd(key_t const& key, fptr_t& fp)
{
  write_lsd(key, wtype_t::PUT, fp);
}
void
lsd_client::put_lsd(function_t& key_func, value_t const& value)
{
  write_lsd(key_func, wtype_t::PUT, value);
}
void
lsd_client::put_lsd(function_t& key_func, fptr_t& fp)
{
  write_lsd(key_func, wtype_t::PUT, fp);
}

void
lsd_client::remove(key_t const& key)
{
  write(key, wtype_t::REMOVE, "<removed>");
}
void
lsd_client::remove_lsd(function_t& key_func)
{
  write_lsd(key_func, wtype_t::REMOVE, "<removed>");
}

void
lsd_client::write(key_t const& key, wtype_t::type const& wtype,
                  value_t const& value)
{
  // simplify code by assuming no more than one put for a specific key
  // per transaction
  auto& rwset = txn.rwset;
  auto const it = rwset.find(key);
  bool const exists = it != rwset.end();
  auto& access = rwset[key];
  auto& rwdata = access.u;
  if (!exists) {
    access.type = atype_t::WRITE;
    write_t& write = rwdata.w;
    write.type = wtype;
    write.future = false;
    write.u.value = value;
  } else {
    auto& atype = access.type;
    if (atype == atype_t::READ) {
      atype = atype_t::READ_WRITE;
      auto& write = rwdata.rw.w;
      write.type = wtype;
      write.future = false;
      write.u.value = value;
      rwdata.rw.r = rwdata.r;
    } else {
#if !defined TWOPL
      DASSERT(false);
#endif
      DASSERT(atype == atype_t::READ_WRITE);
      auto& write = rwdata.rw.w;
      write.type = wtype;
      write.u.value = value;
    }
  }
}

void
lsd_client::write_lsd(key_t const& key, wtype_t::type const& wtype, fptr_t& fp)
{
  // simplify code by assuming no more than one put for a specific key
  // per transaction
  auto& rwset = txn.rwset;
  auto const it = rwset.find(key);
  bool const exists = it != rwset.end();
  auto& access = rwset[key];
  auto& rwdata = access.u;
  if (!exists) {
    access.type = atype_t::WRITE;
    write_t& write = rwdata.w;
    write.type = wtype;
    DASSERT(fp.type == ftype_t::POINTER);
    write.future = true;
    write.u.func_fi = fp.u.fi;
  } else {
    auto& atype = access.type;
    if (atype == atype_t::READ) {
      atype = atype_t::READ_WRITE;
      auto& write = rwdata.rw.w;
      write.type = wtype;
      DASSERT(fp.type == ftype_t::POINTER);
      write.future = true;
      write.u.func_fi = fp.u.fi;
      rwdata.rw.r = rwdata.r;
    } else {
#if !defined TWOPL
      DASSERT(false);
#endif
      DASSERT(atype == atype_t::READ_WRITE);
      auto& write = rwdata.rw.w;
      write.type = wtype;
      write.u.func_fi = fp.u.fi;
    }
  }
}
void
lsd_client::write_lsd(function_t& key_func, wtype_t::type const& wtype,
                      value_t const& value)
{
  fwset_entry_t entry;
  entry.func = key_func;
  write_t& write = entry.w;
  write.type = wtype;
  write.future = false;
  write.u.value = value;
  txn.future_wset.push_back(entry);
}
void
lsd_client::write_lsd(function_t& key_func, wtype_t::type const& wtype,
                      fptr_t& fp)
{
  fwset_entry_t entry;
  entry.func = key_func;
  write_t& write = entry.w;
  write.type = wtype;
  write.future = true;
  write.u.func_fi = fp.u.fi;
  txn.future_wset.push_back(entry);
}

void
lsd_client::commit()
{
  vote_t vote;
  tm->commit(vote, txn);
  if (vote.result == vote_result_t::OK) {
    ++tid_ctr;
    assume_predicates = true;
    auto& fmap = txn.futures.fmap;
    for (auto& entry : vote.rfmap) {
      auto& fi = entry.first;
      auto& rread = entry.second;
      auto& future_read = fmap[fi];
      DASSERT(future_read.type == ftype_t::READ);
      auto& fread = future_read.u.rdata;
      if (!fread.resolved) {
        fread.data = rread;
        fread.resolved = true;
      }
    }
    for (auto& future : fmap) {
      tm->resolve(txn, future);
    }
  } else {
    if (vote.result == vote_result_t::PREDICATE_VALIDATION) {
      assume_predicates = false;
    }
    retry("commit", false);
  }
}

void
lsd_client::abort()
{
#ifdef TWOPL
  tm->rollback(txn);
#endif
  ++tid_ctr;
}

fptr_t
lsd_client::alloc_future()
{
  auto& futures = txn.futures;
  auto fi = futures.num_fi;
  ++futures.num_fi;
  future_t res;
  res.type = ftype_t::POINTER;
  res.u.fi = fi;
  return res;
}

fptr_t const
lsd_client::make_future(future_t& future)
{
  auto& futures = txn.futures;
  auto res = alloc_future();
  futures.fmap.push_back(future);
  DASSERT(futures.fmap.size() == static_cast<std::size_t>(futures.num_fi));
  return res;
}

void
lsd_client::retry(std::string const why, bool const rollback)
{
  if (rollback) {
#if !defined TWOPL
    DASSERT(false);
#endif
    tm->rollback(txn);
  }
  throw txn_exception(why);
}
}
}
} // namespace
