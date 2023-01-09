//
//  config.hh
//  lsd
//
//  Created by Tiago Vale on 28/01/16.
//  Copyright © 2016 Tiago Vale. All rights reserved.
//

#pragma once

#include <string>
#include <vector>

namespace novalincs {
namespace cs {
namespace lsd {

typedef std::vector<std::pair<std::string, int>> servers_t;
typedef servers_t clients_t;

class config
{
protected:
  clients_t clist;
  int cthreads;
  servers_t slist;
  std::string bhost;
  int bport;
  std::string storage;

public:
  config();
  clients_t const& clients();
  int threads(int const client_id);
  int total_threads();
  int global_id(int const client_id, int const thread_id);
  servers_t const& servers();
  std::string barrier_host();
  int barrier_port();
  std::string storage_backend();
};
}
}
}
