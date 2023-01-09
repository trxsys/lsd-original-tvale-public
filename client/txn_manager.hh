//
//  txn_manager.hh
//  lsd
//
//  Created by Tiago Vale on 17/12/15.
//  Copyright © 2015 Tiago Vale. All rights reserved.
//

#pragma once

#include "../common/config.hh"
#include "../thrift/gen-cpp/data_manager.h"
#include "../thrift/gen-cpp/txn_manager.h"
#include "../thrift/gen-cpp/types_types.h"
#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/THttpClient.h>
#include <vector>

namespace novalincs {
namespace cs {
namespace lsd {

using namespace ::apache::thrift::protocol;

using boost::shared_ptr;

typedef std::function<value_t const*(txn_t&, future_t&, mgmap_t const&,
                                     bool const)>
  lambda_op;

class tm_handler : virtual public txn_managerIf
{
private:
  std::vector<shared_ptr<TTransport>> transports;
  std::vector<data_managerClient> clients;
  std::size_t const n_servers;
  lambda_op ops[ftype_t::TOTAL];

public:
  tm_handler(servers_t const& servers);
  void get_notxn(rread_t& _return, key_t const& key);
  void get(rread_t& _return, tid_t const& tid, key_t const& key,
           bool for_update);
  void multiget_notxn(mget_t& _return, klist_t const& klist);
  void multiget(mget_t& _return, tid_t const& tid, klist_t const& klist,
                bool for_update);
  void is_true(assert_t& _return, txn_t const& txn,
               predicate_t const& predicate);
  void commit(vote_t& _return, txn_t const& txn);
  void rollback(txn_t const& txn);
  void resolve_reads(txn_t& txn, rfmap_t const& rfmap);
  value_t const* const resolve(txn_t& txn, future_t& future,
                               mgmap_t const& kr_map = mgmap_t(),
                               bool const transient = false);
};
}
}
} // namespace
