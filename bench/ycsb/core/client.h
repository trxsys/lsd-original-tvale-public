//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "utils.h"
#include <iostream>
#include <string>
#include <unordered_set>

namespace ycsbc {

using novalincs::cs::lsd::txn_exception;

class Client
{
public:
  Client(DB& db, CoreWorkload& wl)
    : db_(db)
    , workload_(wl)
  {
  }

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() {}

protected:
  virtual int TransactionReadStandard();
  virtual int TransactionReadStandardMulti();
  virtual int TransactionReadLSD();
  virtual int TransactionReadLSDMulti();
  virtual int TransactionRead();
  virtual int TransactionReadModifyWriteStandard();
  virtual int TransactionReadModifyWriteStandardMulti();
  virtual int TransactionReadModifyWriteLSD();
  virtual int TransactionReadModifyWriteLSDMulti();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();

  DB& db_;
  CoreWorkload& workload_;
  std::unordered_set<std::string> keys_;
};

inline bool
Client::DoInsert()
{
  std::string key = workload_.NextSequenceKey();
  std::string value;
  workload_.BuildValue(value);
  auto const success = db_.Insert(workload_.NextTable(), key, value);
  return (success == DB::kOK);
}

inline bool
Client::DoTransaction()
{
#ifndef NDEBUG
  int status = -1;
#endif
  keys_.clear();
  do {
    keys_.insert(workload_.NextTransactionKey());
  } while (keys_.size() < workload_.ops_per_txn());
  try {
    db_.Begin();
    switch (workload_.NextOperation()) {
      case READ:
#ifndef NDEBUG
        status = TransactionRead();
#else
        TransactionRead();
#endif
        break;
      case UPDATE:
#ifndef NDEBUG
        status = TransactionUpdate();
#else
        TransactionUpdate();
#endif
        break;
      case INSERT:
#ifndef NDEBUG
        status = TransactionInsert();
#else
        TransactionInsert();
#endif
        break;
      case SCAN:
#ifndef NDEBUG
        status = TransactionScan();
#else
        TransactionScan();
#endif
        break;
      case READMODIFYWRITE:
#ifndef NDEBUG
        status = TransactionReadModifyWrite();
#else
        TransactionReadModifyWrite();
#endif
        break;
      default:
        throw utils::Exception("Operation request is not recognized!");
    }
    assert(status >= 0);
    db_.Commit();
    return 1;
  } catch (txn_exception const& e) {
    return 0;
  }
}

inline int
Client::TransactionReadStandard()
{
  const std::string& table = workload_.NextTable();
  for (auto const& key : keys_) {
    std::string result;
    db_.Read(table, key, result);
  }
  return DB::kOK;
}
inline int
Client::TransactionReadStandardMulti()
{
  const std::string& table = workload_.NextTable();
  klist_t klist;
  for (auto& key : keys_) {
    klist.push_back(key);
  }
  result_map_t result;
  db_.MultiRead(table, klist, result);
  return DB::kOK;
}
inline int
Client::TransactionReadLSD()
{
  const std::string& table = workload_.NextTable();
  for (auto const& key : keys_) {
    fptr_t result;
    db_.Read(table, key, result);
  }
  return DB::kOK;
}
inline int
Client::TransactionReadLSDMulti()
{
  const std::string& table = workload_.NextTable();
  klist_t klist;
  for (auto& key : keys_) {
    klist.push_back(key);
  }
  fptr_map_t result;
  db_.MultiRead(table, klist, result);
  return DB::kOK;
}
inline int
Client::TransactionRead()
{
#if !defined YCSB_LSD && !defined YCSB_MULTIGET
  return TransactionReadStandard();
#elif !defined YCSB_LSD && defined YCSB_MULTIGET
  return TransactionReadStandardMulti();
#elif defined YCSB_LSD && !defined YCSB_MULTIGET
  return TransactionReadLSD();
#elif defined YCSB_LSD && defined YCSB_MULTIGET
  return TransactionReadLSDMulti();
#endif
}

inline int
Client::TransactionReadModifyWriteStandard()
{
  size_t const size = workload_.ops_per_txn();
  const std::string& table = workload_.NextTable();
  std::vector<std::string> reads;
  reads.reserve(size);
  size_t n = 0;
  for (auto const& key : keys_) {
    std::string result;
    db_.Read(table, key, result, true);
    reads.push_back(result);
    n++;
    DASSERT(reads.size() == n);
  }
  DASSERT(n == size);
  n = 1;
  for (auto const& key : keys_) {
    db_.Update(table, key, reads[n]);
    n = (n + 1) % size;
  }
  return DB::kOK;
}
inline int
Client::TransactionReadModifyWriteStandardMulti()
{
  size_t const size = workload_.ops_per_txn();
  const std::string& table = workload_.NextTable();
  std::vector<std::string> reads;
  reads.reserve(size);
  size_t n = 0;
  klist_t klist;
  for (auto& key : keys_) {
    klist.push_back(key);
  }
  result_map_t result;
  db_.MultiRead(table, klist, result, true);
  for (auto& key : keys_) {
    DASSERT(result.at(key).exists);
    reads.push_back(result.at(key).value);
    n++;
    DASSERT(reads.size() == n);
  }
  DASSERT(n == size);

  n = 1;
  for (auto const& key : keys_) {
    db_.Update(table, key, reads[n]);
    n = (n + 1) % size;
  }
  return DB::kOK;
}
inline int
Client::TransactionReadModifyWriteLSD()
{
  size_t const size = workload_.ops_per_txn();
  const std::string& table = workload_.NextTable();
  std::vector<fptr_t> reads;
  reads.reserve(size);
  size_t n = 0;
  for (auto const& key : keys_) {
    fptr_t result;
    db_.Read(table, key, result);
    reads.push_back(result);
    n++;
    DASSERT(reads.size() == n);
  }
  DASSERT(n == size);
  n = 1;
  for (auto const& key : keys_) {
    db_.Update(table, key, reads[n]);
    n = (n + 1) % size;
  }
  return DB::kOK;
}
inline int
Client::TransactionReadModifyWriteLSDMulti()
{
  size_t const size = workload_.ops_per_txn();
  const std::string& table = workload_.NextTable();
  std::vector<fptr_t> reads;
  reads.reserve(size);
  size_t n = 0;
  klist_t klist;
  for (auto& key : keys_) {
    klist.push_back(key);
  }
  fptr_map_t result;
  db_.MultiRead(table, klist, result);
  for (auto& key : keys_) {
    reads.push_back(result.at(key));
    n++;
    DASSERT(reads.size() == n);
  }
  DASSERT(n == size);

  n = 1;
  for (auto const& key : keys_) {
    db_.Update(table, key, reads[n]);
    n = (n + 1) % size;
  }
  return DB::kOK;
}
inline int
Client::TransactionReadModifyWrite()
{
#if !defined YCSB_LSD && !defined YCSB_MULTIGET
  return TransactionReadModifyWriteStandard();
#elif !defined YCSB_LSD && defined YCSB_MULTIGET
  return TransactionReadModifyWriteStandardMulti();
#elif defined YCSB_LSD && !defined YCSB_MULTIGET
  return TransactionReadModifyWriteLSD();
#elif defined YCSB_LSD && defined YCSB_MULTIGET
  return TransactionReadModifyWriteLSDMulti();
#endif
}

inline int
Client::TransactionScan()
{
  assert(false);
  for (auto const& key : keys_) {
    const std::string& table = workload_.NextTable();
    int len = workload_.NextScanLength();
    std::vector<std::vector<DB::KVPair>> result;
    db_.Scan(table, key, len, NULL, result);
  }
  return DB::kOK;
}

inline int
Client::TransactionUpdate()
{
  for (auto const& key : keys_) {
    const std::string& table = workload_.NextTable();
    std::string value;
    workload_.BuildValue(value);
    db_.Update(table, key, value);
  }
  return DB::kOK;
}

inline int
Client::TransactionInsert()
{
  assert(false);
  return DB::kOK;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
