#pragma once

#include "storage.hh"
#include <unordered_map>

namespace novalincs {
namespace cs {
namespace lsd {

class StdUnorderedMap : public Storage
{

public:
  StdUnorderedMap() {}
  virtual ~StdUnorderedMap() {}
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
  std::unordered_map<key_t, value_t> data;
};
}
}
}
