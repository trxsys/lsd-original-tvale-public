#include "std_unordered_map.hh"
#include "../common/definitions.hh"
#include <fstream>

namespace novalincs {
namespace cs {
namespace lsd {

void
StdUnorderedMap::init(std::string& id)
{
}

Result
StdUnorderedMap::get(key_t key)
{
  auto it = data.find(key);
  return it == data.end() ? Result() : Result(it->second);
}

void
StdUnorderedMap::put(key_t key, value_t value)
{
  data[key] = value;
}

void
StdUnorderedMap::put_batch(
  std::vector<std::tuple<key_t, wtype_t::type, value_t>>& ops)
{
  for (const auto& ktv : ops) {
    if (std::get<1>(ktv) == wtype_t::PUT) {
      put(std::get<0>(ktv), std::get<2>(ktv));
    } else {
      DASSERT(std::get<1>(ktv) == wtype_t::REMOVE);
      del(std::get<0>(ktv));
    }
  }
}

void
StdUnorderedMap::del(key_t key)
{
  data.erase(key);
}

void
StdUnorderedMap::store_to_disk(std::string const& filename)
{

  std::ofstream ofs(filename, std::ofstream::binary | std::ofstream::out |
                                std::ofstream::trunc);
  std::size_t const data_size = data.size();
  ofs.write(reinterpret_cast<const char*>(&data_size), sizeof data_size);
  DPRINTF("store_to_disk(%s): size=%" PRIu64 "\n", filename.c_str(), data_size);
  for (auto it = data.cbegin(); it != data.cend(); ++it) {
    auto const key = it->first;
    auto const value = it->second;
    DPRINTF("store_to_disk(%s): key=%s value=%s\n", filename.c_str(),
            key.c_str(), value.c_str());
    std::size_t const key_size = key.size();
    ofs.write(reinterpret_cast<const char*>(&key_size), sizeof key_size);
    char const* key_cstr = key.c_str();
    ofs.write(key_cstr, key_size);
    std::size_t const value_size = value.size();
    ofs.write(reinterpret_cast<const char*>(&value_size), sizeof value_size);
    char const* value_cstr = value.c_str();
    ofs.write(value_cstr, value_size);
  }
  ofs.close();
}

void
StdUnorderedMap::load_from_disk(std::function<void(key_t, value_t)> key_proc,
                                std::string const& filename)
{
  std::ifstream ifs(filename, std::ifstream::binary | std::ifstream::in);
  std::size_t data_size;
  ifs.read(reinterpret_cast<char*>(&data_size), sizeof data_size);
  DPRINTF("load_from_disk(%s): size=%" PRIu64 "\n", filename.c_str(),
          data_size);
  for (std::size_t i = 0; i < data_size; ++i) {
    std::size_t key_size;
    ifs.read(reinterpret_cast<char*>(&key_size), sizeof key_size);
    char* key_cstr = new char[key_size];
    ifs.read(key_cstr, key_size);
    std::size_t value_size;
    ifs.read(reinterpret_cast<char*>(&value_size), sizeof value_size);
    char* value_cstr = new char[value_size];
    ifs.read(value_cstr, value_size);
    std::string key(key_cstr, key_size), value(value_cstr, value_size);
    delete[] key_cstr;
    delete[] value_cstr;
    data[key] = value;
    key_proc(key, value);
  }
  ifs.close();
}

} // lsd
} // cs
} // novalincs
