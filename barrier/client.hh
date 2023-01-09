//
//  client.hh
//  lsd
//
//  Created by Tiago Vale on 21/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/THttpClient.h>
#include "../thrift/gen-cpp/barrier.h"

namespace novalincs {
namespace cs {
namespace lsd {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

class barrier_client
{
private:
  shared_ptr<TTransport> transport;
  barrierClient client;

public:
  barrier_client(std::string const host, int const port);
  bool create(std::string const& id, int32_t const& expected);
  bool wait(std::string const& id);
  bool destroy(std::string const& id);
};
}
}
} // namespace
