namespace cpp novalincs.cs.lsd

include "types.thrift"

/* a transaction manager is a frontend server that knows the sharding policy,
 * i.e. it knows to which data manager should a get request be routed
 * it acts as the coordinator to commit transactions on behalf of clients
 */
service txn_manager {
    types.rread_t get_notxn(1: types.key_t key);
    types.rread_t get(1: types.tid_t tid, 2: types.key_t key, 3: bool will_write);
    types.mget_t multiget_notxn(1: types.klist_t klist);
    types.mget_t multiget(1: types.tid_t tid, 2: types.klist_t klist, 3: bool will_write);
    types.assert_t is_true(1: types.txn_t txn, 2: types.predicate_t predicate);
    types.vote_t commit(1: types.txn_t txn);
    void rollback(1: types.txn_t txn);
}
