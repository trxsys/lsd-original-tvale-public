#include "client.hh"
#include <iostream>

using novalincs::cs::lsd::lsd_client;
using novalincs::cs::lsd::txn_exception;
using novalincs::cs::lsd::result_t;
using novalincs::cs::lsd::ftype_t;
using novalincs::cs::lsd::fptr_t;
using novalincs::cs::lsd::future_t;
using novalincs::cs::lsd::fbinaryop_t;
using novalincs::cs::lsd::predicate_t;
using novalincs::cs::lsd::function_t;

int
main(int argc, char** argv)
{
  lsd_client txn("client-test-1");

  txn.begin();
  txn.put("c", "10");
  txn.commit();

  txn.begin();
  result_t gres = txn.get("c");
  txn.commit();
  std::cout << gres.exists << " " << gres.value << std::endl;

  txn.begin();
  fptr_t c_f = txn.get_lsd("c");
  future_t add_op;
  add_op.type = ftype_t::ADD_FI;
  fbinaryop_t& add_bin_op = add_op.u.binaryop;
  add_bin_op.resolved = false;
  add_bin_op.left.fi = c_f.u.fi;
  add_bin_op.right.value = "1";
  auto add_op_fp = txn.make_future(add_op);
  txn.put_lsd("c", add_op_fp);
  txn.commit();

  txn.begin();
  fptr_t c_f_ = txn.get_lsd("c");
  future_t gte_op;
  gte_op.type = ftype_t::GTEQ_FI;
  fbinaryop_t& gte_bin_op = gte_op.u.binaryop;
  gte_bin_op.resolved = false;
  gte_bin_op.left.fi = c_f_.u.fi;
  gte_bin_op.right.value = "11";
  auto gte_op_fp = txn.make_future(gte_op);
  predicate_t gte_pred;
  function_t& gte_func = gte_pred.func;
  gte_func.keys = { "c" };
  gte_func.future_fi = gte_op_fp.u.fi;
  bool pres = txn.is_true(gte_pred);
  std::cout << "c > 11 is " << std::to_string(pres) << std::endl;
  lsd_client other_txn("client-test-2");
  other_txn.begin();
  other_txn.put("c", "10");
  try {
    other_txn.commit();
  } catch (txn_exception const& e) {
    std::cout << "client-test-2 failed to commit because client-test-1 has "
                 "plock(c > 11)"
              << std::endl;
  }
  try {
    txn.commit();
  } catch (txn_exception const& e) {
    std::cout << "client-test-1 failed to commit because client-test-2 "
                 "invalidated c > 11"
              << std::endl;
  }

  return 0;
}
