//
//  sharding_policy.hh
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include "../thrift/gen-cpp/types_types.h"
#include <cstring>

#define SHARDING_UNKNOWN std::numeric_limits<std::size_t>::max()

namespace novalincs {
namespace cs {
namespace lsd {

std::size_t get_server(key_t const& key, std::size_t const& n_servers);
std::size_t get_server(future_t const& key_func, std::size_t const& n_servers,
                       fmap_t const& fmap);
}
}
} // namespace
