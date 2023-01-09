//
//  main.cc
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "../barrier/client.hh"
#include "../common/config.hh"
#include "data_manager.hh"
#include <csignal>
#include <iostream>
#include <sstream>
#include <string>
#include <thrift/async/TAsyncBufferProcessor.h>
#include <thrift/async/TAsyncProtocolProcessor.h>
#include <thrift/async/TEvhttpServer.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>

namespace lsd = ::novalincs::cs::lsd;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::async;

using boost::shared_ptr;

static TEvhttpServer* server;

void
handle_signal(int signum)
{
  delete server;
}

int
main(const int argc, char const* argv[])
{
  std::istringstream ss(argv[1]);
  int server_id;
  if (!(ss >> server_id)) {
    std::cerr << "Invalid server ID " << argv[1] << std::endl;
    return 1;
  }
  lsd::config cfg;
  auto const& slist = cfg.servers();
  auto const port = slist[server_id].second;
  std::string server_id_str(std::to_string(server_id));
  std::string storage_backend = cfg.storage_backend();
  auto const nservers = slist.size();

  // ycsb
  auto const nclients = cfg.clients().size();
  // tpcc
  // auto const nclients = 1;

  lsd::barrier_client barrier(cfg.barrier_host(), cfg.barrier_port());
  auto const barrier_init =
    "server-init-" + std::to_string(nservers + nclients);
  barrier.create(barrier_init, nservers + nclients);

  shared_ptr<TProtocolFactory> protocol_factory(
    new TBinaryProtocolFactoryT<TBufferBase>());
  shared_ptr<lsd::dm_handler> async_handler(
    new lsd::dm_handler(storage_backend, server_id_str));
  shared_ptr<TAsyncProcessor> async_processor(
    new lsd::data_managerAsyncProcessor(async_handler));
  shared_ptr<TAsyncBufferProcessor> async_buffer_processor(
    new TAsyncProtocolProcessor(async_processor, protocol_factory));
  server = new TEvhttpServer(async_buffer_processor, port);

  signal(SIGINT, handle_signal);

  barrier.wait(barrier_init);
  std::cout << "running data manager " << server_id;
  std::cout << " on port " << port << std::endl;
  server->serve();

  return 0;
}
