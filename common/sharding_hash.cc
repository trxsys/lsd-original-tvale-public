//
//  sharding_mod.cc
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "sharding_policy.hh"
#include <functional>
#include <string>

namespace novalincs {
namespace cs {
namespace lsd {

std::size_t
get_server(key_t const& key, std::size_t const& n_servers)
{
  if (n_servers == 1) {
    return 0;
  } else {
    std::hash<std::string> h;
    return h(key) % n_servers;
  }
}

std::size_t
get_server(future_t const& key_func, std::size_t const& n_servers,
           fmap_t const& fmap)
{
  if (n_servers == 1) {
    return 0;
  } else {
    return SHARDING_UNKNOWN;
  }
}
}
}
} // namespace
