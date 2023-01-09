#include "storage.hh"

#include "rocks_db.hh"
#include "std_unordered_map.hh"

namespace novalincs {
namespace cs {
namespace lsd {

Storage*
StorageFactory::create_storage(std::string& impl, std::string& id)
{
  Storage* storage = nullptr;
  if (impl.compare("std_unordered_map") == 0) {
    storage = new StdUnorderedMap();
  } else if (impl.compare("rocksdb") == 0) {
    storage = new RocksDB();
  } else {
    std::abort();
  }
  assert(storage);
  storage->init(id);
  return storage;
}
}
}
}
