//
//  client.hh
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include "../common/definitions.hh"
#include "../thrift/gen-cpp/types_types.h"
#include "txn_manager.hh"
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace novalincs {
namespace cs {
namespace lsd {

class txn_exception : public std::runtime_error
{
public:
  txn_exception(std::string why)
    : runtime_error(why)
  {
  }
};

struct result_s
{
  bool const exists;
  value_t const& value;

  result_s(bool const exists, value_t const& value)
    : exists(exists)
    , value(value)
  {
  }
};
typedef struct result_s result_t;
typedef std::unordered_map<key_t, result_t> result_map_t;

struct fwrapper_s
{
  future_t f;

  fwrapper_s()
  {
    f.type = ftype_t::POINTER;
    f.u.fi = -1;
  }
};
typedef struct fwrapper_s fwrapper_t;

typedef future_t fptr_t;
typedef std::unordered_map<key_t, fptr_t> fptr_map_t;

typedef std::string cid_t;

enum class assumption
{
  NONE,
  TRUE,
  FALSE
};

class lsd_client
{
protected:
  fwrapper_t static NULL_FUTURE;
  std::unique_ptr<tm_handler> tm;
  cid_t const cid;
  uint64_t tid_ctr;
  txn_t txn;
  bool assume_predicates;

public:
  lsd_client(cid_t const id);
  void begin();
  result_t get(key_t const& key, bool const for_update = false);
  fptr_t get_lsd(key_t const& key);
  result_map_t multiget(klist_t klist, bool const for_update = false);
  fptr_map_t multiget_lsd(klist_t klist);
  bool is_true(predicate_t& predicate,
               assumption const assume = assumption::NONE);
  void put(key_t const& key, value_t const& value);
  void put_lsd(key_t const& key, fptr_t& fp);
  void put_lsd(function_t& key_func, value_t const& value);
  void put_lsd(function_t& key_func, fptr_t& value_fp);
  fptr_t const make_future(future_t& future);
  void remove(key_t const& key);
  void remove_lsd(function_t& func);
  void commit();
  void abort();

private:
  void write(key_t const& key, wtype_t::type const& wtype,
             value_t const& value);
  void write_lsd(key_t const& key, wtype_t::type const& wtype, fptr_t& fp);
  void write_lsd(function_t& key_func, wtype_t::type const& wtype,
                 value_t const& value);
  void write_lsd(function_t& key_func, wtype_t::type const& wtype, fptr_t& fp);
  fptr_t alloc_future();
  void retry(std::string const why, bool const rollback = true);
};
}
}
} // namespace
