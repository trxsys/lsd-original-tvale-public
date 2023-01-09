//
//  db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_H_
#define YCSB_C_DB_H_

#include "../../../client/client.hh"
#include "properties.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <vector>

namespace ycsbc {

using novalincs::cs::lsd::lsd_client;
using novalincs::cs::lsd::klist_t;
using novalincs::cs::lsd::future_t;
using novalincs::cs::lsd::fptr_t;
using novalincs::cs::lsd::fptr_map_t;
using novalincs::cs::lsd::result_t;
using novalincs::cs::lsd::result_map_t;

class DB
{
  lsd_client* server;

public:
  typedef std::pair<std::string, std::string> KVPair;
  static const int kOK = 0;
  static const int kErrorNoData = 1;
  static const int kErrorConflict = 2;
  ///
  /// Initializes any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance
  /// globally.
  ///
  void Init(const utils::Properties& p)
  {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    server = new lsd_client(boost::uuids::to_string(uuid));
  }

  ///
  /// Clears any state for accessing this DB.
  /// Called once per DB client (thread); there is a single DB instance
  /// globally.
  ///
  void Close() { delete server; }

  void Begin() { server->begin(); }

  void Commit() { server->commit(); }

  int Read(const std::string& table, const std::string& key, fptr_t& result)
  {
    result = server->get_lsd(key);
    return DB::kOK;
  }
  int Read(const std::string& table, const std::string& key,
           std::string& result, bool for_update = false)
  {
    auto const reply = server->get(key, for_update);
    DASSERT(reply.exists);
    result = reply.value;
    return DB::kOK;
  }

  int MultiRead(const std::string& table, klist_t keys, result_map_t& result,
                bool for_update = false)
  {
    result = server->multiget(keys, for_update);
    return DB::kOK;
  }
  int MultiRead(const std::string& table, klist_t keys, fptr_map_t& result)
  {
    result = server->multiget_lsd(keys);
    return DB::kOK;
  }

  int Scan(const std::string& table, const std::string& key, int len,
           const std::vector<std::string>* fields,
           std::vector<std::vector<KVPair>>& result)
  {
    assert(false);
    return DB::kErrorNoData;
  }

  int Update(const std::string& table, const std::string& key,
             std::string& value)
  {
    server->put(key, value);
    return DB::kOK;
  }
  int Update(const std::string& table, const std::string& key, fptr_t& fp)
  {
    server->put_lsd(key, fp);
    return DB::kOK;
  }

  int Insert(const std::string& table, const std::string& key,
             std::string& value)
  {
    return Update(table, key, value);
  }

  int Delete(const std::string& table, const std::string& key)
  {
    server->remove(key);
    return DB::kOK;
  }

  ~DB() {}
};

} // ycsbc

#endif // YCSB_C_DB_H_
