#pragma once

#include <sstream>
#include <string>
#include <unordered_map>

namespace apache {
namespace thrift {

/* because we are using unordered_map instead of map, and thrift/TToString.h
 * does not define the to_string(unordered_map) template
 */
template <typename K, typename V>
std::string
to_string(const std::unordered_map<K, V>& m)
{
  std::ostringstream o;
  o << "{" << to_string(m.begin(), m.end()) << "}";
  return o.str();
}

template <typename T>
std::string
to_string(const std::list<T>& l)
{
  std::ostringstream o;
  o << "[" << to_string(l.begin(), l.end()) << "]";
  return o.str();
}
}
}
