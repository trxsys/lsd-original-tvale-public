import sys
sys.path.append('lib/python2.7/site-packages')
sys.path.append('thrift/gen-py')
sys.path.append('.')
from client.client import lsd_client
from lsdtypes.ttypes import *
txn = lsd_client()

txn.begin()
txn.put('c', '10', lsd_api=False)
txn.commit()

txn.begin()
gres = txn.get('c', lsd_api=False)
txn.commit()
print '{} {}'.format(str(gres.exists), gres.value)

txn.begin()
c_f = txn.get_lsd('c', lsd_api=True)
add_bin_op = fbinaryop_t(resolved=False,
                         left=operand_t(fi=c_f.u.fi),
                         right=operand_t(value='1'))
add_op = future_t(ftype_t.ADD_FI, fstate_t(binaryop=add_bin_op))
add_op_fp = txn.make_future(add_op)
txn.put('c', add_op_fp)
txn.commit()

txn.begin()
c_f_ = txn.get('c', lsd_api=False)
gte_bin_op = fbinaryop_t(resolved=False,
                         left=operand_t(fi=c_f_.u.fi),
                         right=operand_t(value='11'))
gte_op = future_t(ftype_t.GTEQ_FI, fstate_t(binaryop=gte_bin_op))
gte_pred = predicate_t(func=function_t(['c'], gte_op_fp.u.fi))
bool pres = txn.is_true(gte_pred)
print 'c > 11 is {}'.format(std(pres))
other_txn = lsd_client()
other_txn.begin()
other_txn.put('c', '10')
try:
  other_txn.commit()
except TransactionException e:
  why = '{} failed to commit because {} has plock(c > 11)'
  print why.format(other_txn.txn.tid, txn.tid)
try:
  txn.commit()
except TransactionException e:
  why = '{} failed to commit because {} invalidated c > 11'
  print why.format(txn.tid, other_txn.tid)
