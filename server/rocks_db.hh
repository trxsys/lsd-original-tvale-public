#pragma once

#include "rocksdb/db.h"
#include "storage.hh"
#include <string>

namespace novalincs {
namespace cs {
namespace lsd {

class RocksDB : public Storage
{

public:
  RocksDB()
    : db()
    , db_path("rocks_db_")
  {
  }
  virtual ~RocksDB()
  {
    for (auto i = 0; i < 12; i++) {
      delete db[i];
    }
  }

  virtual void init(std::string& id);
  virtual Result get(key_t key);
  virtual void put(key_t key, value_t value);
  virtual void put_batch(
    std::vector<std::tuple<key_t, wtype_t::type, value_t>>& ops);
  virtual void del(key_t key);
  virtual void store_to_disk(std::string const& filename);
  virtual void load_from_disk(std::function<void(key_t, value_t)> key_proc,
                              std::string const& filename);

private:
  rocksdb::DB* db[12];
  std::string db_path;
};
}
}
}
