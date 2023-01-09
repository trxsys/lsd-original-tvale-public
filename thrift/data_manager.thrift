namespace cpp novalincs.cs.lsd

include "types.thrift"

/* a data manager is a backend server that stores a partition of the database
 * it accepts get requests for keys, commits single-partition transactions,
 * and participates in 2-phase commit to commit multi-partition transactions
 */
/* futures TODO
 */
service data_manager {
    types.rread_t get_notxn(1: types.key_t key);
    types.rread_t get(1: types.tid_t tid, 2: types.key_t key, 3: bool will_write);
    types.mget_t multiget_notxn(1: types.klist_t klist);
    types.mget_t multiget(1: types.tid_t tid, 2: types.klist_t klist, 3: bool will_write);
    types.vote_t tpc_prepare(1: types.txn_t txn);
    types.vote_t tpc_prepare2(1: types.txn_t txn, 2: types.rfmap_t rfmap);
    void tpc_commit(1: types.tid_t tid, 2: types.rfmap_t rfmap);
    void tpc_abort(1: types.tid_t tid);
    types.vote_t opc_commit(1: types.txn_t txn);
    types.assert_t opc_assert(1: types.tid_t tid, 2: types.predicate_t predicate, 3: types.fmap_t fmap);
    bool store_to_disk(1: string filename);
    bool load_from_disk(1: string filename);
}
