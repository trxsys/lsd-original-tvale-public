//
//  client.cc
//  lsd
//
//  Created by Tiago Vale on 21/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#include "client.hh"

namespace novalincs {
namespace cs {
namespace lsd {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

barrier_client::barrier_client(std::string const host, int const port)
  : transport(new THttpClient(host, port, "/"))
  , client(shared_ptr<TProtocol>(new TBinaryProtocol(transport)))
{
  transport->open();
}

bool
barrier_client::create(std::string const& id, int32_t const& expected)
{
  return client.create(id, expected);
}

bool
barrier_client::wait(std::string const& id)
{
  return client.wait(id);
}

bool
barrier_client::destroy(std::string const& id)
{
  return client.destroy(id);
}
}
}
} // namespace
