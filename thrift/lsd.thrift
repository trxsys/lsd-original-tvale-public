namespace cpp novalincs.cs.lsd

typedef i64    version_t
typedef string value_t
typedef string key_t
typedef string tid_t

/* get operations return a consistent value-version pair
 * the client library maintains this information in the read set
 */
struct get_res_t {
    1: value_t   value;
    2: version_t version;
    3: bool      exists;
}

/* update operations may either be a put or a delete
 * we need to distinguish between the two in the write set
 */
enum upd_type_t {
    PUT    = 1,
    DELETE = 2
}

struct update_t {
    1: upd_type_t type;
    2: value_t    value; // only meaningful if type = PUT
}

/* when the client wants to commit a transaction, it sends both the
 * read and write set to the transaction manager
 */
struct txn_t {
    1: map<key_t, version_t> rset;
    2: map<key_t, update_t>  wset;
}

/* a data manager is a backend server that stores a partition of the database
 * it accepts get requests for keys, commits single-partition transactions,
 * and participates in 2-phase commit to commit multi-partition transactions
 */
service data_manager {
    get_res_t get        (1: key_t key              );
    bool      tpc_prepare(1: tid_t tid, 2: txn_t txn);
    void      tpc_commit (1: tid_t tid              );
    void      tpc_abort  (1: tid_t tid              );
    bool      opc_commit (1: txn_t txn              );
}

/* a transaction manager is a frontend server that knows the sharding policy,
 * i.e. it knows to which data manager should a get request be routed
 * it acts as the coordinator to commit transactions on behalf of clients
 */
service txn_manager {
    get_res_t get   (1: key_t key              );
    bool      commit(1: tid_t tid, 2: txn_t txn);
}
