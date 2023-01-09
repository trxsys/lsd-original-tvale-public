#include "rocks_db.hh"
#include "../common/definitions.hh"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include <fstream>

using namespace rocksdb;

namespace novalincs {
namespace cs {
namespace lsd {

void
RocksDB::init(std::string& id)
{
  Options options;
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.OptimizeLevelStyleCompaction();
  options.OptimizeForPointLookup(512);
  // in-memory sst tables
  // options.allow_mmap_reads = true;
  // options.allow_mmap_writes = true;
  // options.table_factory.reset(NewPlainTableFactory());
  for (auto i = 0; i < 12; i++) {
    Status s =
      DB::Open(options, db_path + id + "_" + std::to_string(i), &db[i]);
    if (!s.ok()) {
      fprintf(stderr, "ABORT: RocksDB::init(%s) returned %s\n", id.c_str(),
              s.ToString().c_str());
      std::abort();
    }
  }
}

Result
RocksDB::get(key_t key)
{
  value_t value;
  Status s = db[table_index(key)]->Get(ReadOptions(), key, &value);
  if (!s.ok() && !s.IsNotFound()) {
    fprintf(stderr, "ABORT: RocksDB::get(%s) returned %s\n", key.c_str(),
            s.ToString().c_str());
    std::abort();
  }
  if (s.IsNotFound())
    return Result();
  else
    return Result(value);
}

void
RocksDB::put(key_t key, value_t value)
{
  Status s = db[table_index(key)]->Put(WriteOptions(), key, value);
  if (!s.ok()) {
    fprintf(stderr, "ABORT: RocksDB::put(%s, %s) returned %s\n", key.c_str(),
            value.c_str(), s.ToString().c_str());
    std::abort();
  }
}

void
RocksDB::put_batch(std::vector<std::tuple<key_t, wtype_t::type, value_t>>& ops)
{
  WriteBatch batch[12];
  for (const auto& ktv : ops) {
    auto const& t = std::get<1>(ktv);
    auto const& key = std::get<0>(ktv);
    if (t == wtype_t::PUT) {
      batch[table_index(key)].Put(std::get<0>(ktv), std::get<2>(ktv));
    } else {
      DASSERT(t == wtype_t::REMOVE);
      batch[table_index(key)].Delete(std::get<0>(ktv));
    }
  }
  for (auto i = 0; i < 12; i++) {
    if (batch[i].Count() > 0) {
      Status s = db[i]->Write(WriteOptions(), &batch[i]);
      if (!s.ok()) {
        fprintf(stderr, "ABORT: RocksDB::put_batch(...) returned %s\n",
                s.ToString().c_str());
        std::abort();
      }
    }
  }
}

void
RocksDB::del(key_t key)
{
  Status s = db[table_index(key)]->Delete(WriteOptions(), key);
  if (!s.ok()) {
    fprintf(stderr, "ABORT: RocksDB::del(%s) returned %s\n", key.c_str(),
            s.ToString().c_str());
    std::abort();
  }
}

void
RocksDB::store_to_disk(std::string const& filename)
{
  DASSERT(false);
}

void
RocksDB::load_from_disk(std::function<void(key_t, value_t)> key_proc,
                        std::string const& filename)
{
  DASSERT(false);
}

} // lsd
} // cs
} // novalincs
