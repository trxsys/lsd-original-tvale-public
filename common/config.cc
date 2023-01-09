//
//  config.cc
//  lsd
//
//  Created by Tiago Vale on 28/01/16.
//  Copyright © 2016 Tiago Vale. All rights reserved.
//

#include "config.hh"
#include <boost/program_options.hpp>
#include <fstream>
#include <sstream>

namespace novalincs {
namespace cs {
namespace lsd {

namespace po = boost::program_options;

config::config()
  : clist()
  , cthreads(0)
  , slist()
  , bhost()
  , bport(0)
  , storage()
{
  // create boost's option map
  // also barrier
  std::string clients_str;
  std::string servers_str;
  po::options_description desc("");
  desc.add_options()("ycsb.instances", po::value<std::string>(&clients_str),
                     "ycsb clients hosts:threads");
  desc.add_options()("server.instances", po::value<std::string>(&servers_str),
                     "servers");
  desc.add_options()("server.storage", po::value<std::string>(&storage),
                     "storage backend");
  desc.add_options()("barrier.host", po::value<std::string>(&bhost),
                     "barrier host");
  desc.add_options()("barrier.port", po::value<int>(&bport), "barrier port");
  desc.add_options()("python.sharding", po::value<std::string>(),
                     "sharding policy");
  desc.add_options()("python.ccontrol", po::value<std::string>(),
                     "concurrency control protocol");
  desc.add_options()("python.assume_predicates", po::value<std::string>(),
                     "assume predicate values");
  desc.add_options()("python.2pc_sequential", po::value<std::string>(),
                     "2pc sequential");
  desc.add_options()("tpcc.coord", po::value<std::string>(),
                     "tpcc coordinator host");
  desc.add_options()("tpcc.clients", po::value<std::string>(),
                     "tpcc client hosts");
  desc.add_options()("tpcc.path", po::value<std::string>(),
                     "tpcc code path at clients");
  desc.add_options()("tpcc.use_lsd", po::value<std::string>(), "use lsd");
  po::variables_map opts;
  std::ifstream cfg_file("config.ini");
  po::store(po::parse_config_file(cfg_file, desc), opts);
  po::notify(opts);
  // servers
  std::string server_str;
  std::istringstream siss(servers_str);
  while (siss >> server_str) {
    auto colon_pos = server_str.find(':');
    assert(colon_pos != std::string::npos);
    auto host = server_str.substr(0, colon_pos);
    auto port_str = server_str.substr(colon_pos + 1);
    auto port = std::stoi(port_str);
    slist.emplace_back(host, port);
  }
  // clients
  std::string client_str;
  std::istringstream ciss(clients_str);
  while (ciss >> client_str) {
    auto colon_pos = client_str.find(':');
    assert(colon_pos != std::string::npos);
    auto host = client_str.substr(0, colon_pos);
    auto threads_str = client_str.substr(colon_pos + 1);
    auto client_threads = std::stoi(threads_str);
    clist.emplace_back(host, client_threads);
    cthreads += client_threads;
  }
}

clients_t const&
config::clients()
{
  return clist;
}

int
config::threads(int const client_id)
{
  return clist[client_id].second;
}

int
config::total_threads()
{
  return cthreads;
}

int
config::global_id(int const client_id, int const thread_id)
{
  int id = 0;
  for (int i = 0; i < client_id; i++) {
    id += clist[i].second;
  }
  id += thread_id;
  return id;
}

std::string
config::barrier_host()
{
  return bhost;
}

int
config::barrier_port()
{
  return bport;
}

servers_t const&
config::servers()
{
  return slist;
}

std::string
config::storage_backend()
{
  return storage;
}
}
}
}
