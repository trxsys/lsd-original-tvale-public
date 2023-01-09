#ifndef LSD_STORAGE
#define LSD_STORAGE

#include "../thrift/gen-cpp/types_types.h"
#include <string>
#include <tuple>
#include <vector>

namespace novalincs {
namespace cs {
namespace lsd {

inline int
table_index(key_t const& key)
{
  auto const& table = key.substr(0, 3);
  switch (table[0]) {
    case 'w':
      return 0;
    case 'd':
      return 1;
    case 'c':
      switch (table[1]) {
        case 'i':
          return 2;
        default:
          return 3;
      }
    case 'h':
      return 4;
    case 'n':
      switch (table[2]) {
        case 'i':
          return 5;
        default:
          return 6;
      }
    case 'o':
      switch (table[1]) {
        case 'i':
          return 7;
        case 'l':
          return 8;
        default:
          return 9;
      }
    case 'i':
      return 10;
    case 's':
      return 11;
    default:
      return 0;
  }
}

class Result
{
public:
  bool exists;
  value_t value;

  Result()
    : exists(false)
    , value("<n/a>")
  {
  }
  Result(value_t value)
    : exists(true)
    , value(value)
  {
  }
};

class Storage
{
public:
  virtual ~Storage() {}
  virtual void init(std::string& id) = 0;
  virtual Result get(key_t key) = 0;
  virtual void put(key_t key, value_t value) = 0;
  virtual void put_batch(
    std::vector<std::tuple<key_t, wtype_t::type, value_t>>& ops) = 0;
  virtual void del(key_t key) = 0;
  virtual void store_to_disk(std::string const& filename) = 0;
  virtual void load_from_disk(std::function<void(key_t, value_t)> key_proc,
                              std::string const& filename) = 0;
};

class StorageFactory
{
public:
  static Storage* create_storage(std::string& impl, std::string& id);
};
}
}
}

#endif // LSD_STORAGE
