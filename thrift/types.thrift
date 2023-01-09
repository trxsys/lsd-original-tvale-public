namespace cpp novalincs.cs.lsd
namespace py lsdtypes

cpp_include "<unordered_map>"
cpp_include "<list>"

typedef i64 version_t
typedef string value_t
typedef string key_t
typedef string tid_t
typedef list<key_t> klist_t
typedef i32 fidx_t

enum vote_result_t {
    OK,
    VERSION_VALIDATION, PREDICATE_VALIDATION,
    DEADLOCK_AVOIDANCE
}

/* we keep a single rwset ordered by key to prevent deadlocks
 * each access may either be a read, write, or read-write
 */
enum atype_t { READ, WRITE, READ_WRITE }

struct rread_t {
    1: bool existed;
    2: version_t version;
    3: value_t value;
    4: vote_result_t status;
}

struct fread_t {
    1: bool resolved;
    2: key_t key;
    3: rread_t data;
}

union rstate_t {
    1: fidx_t future_fi;
    2: rread_t concrete;
}

struct read_t {
    1: bool future;
    2: rstate_t u;
}

/* futures TODO
 */
/*   conditionals:
 *     {f} >= int, GTEQ_FI
 *   operations:
 *     {f} + int/double, ADD_FI/D
 *     {f} - int/double, SUB_FI/D
 *     {f} + str, CONCAT_FS
 *     str + {f}, CONCAT_SF
 */
enum ftype_t {
    READ = 0, POINTER = 1, ADD_FI = 2, ADD_FD = 3, SUB_FI = 4,
    SUB_FD = 5, CONCAT_FS = 6, CONCAT_SF = 7, GTEQ_FI = 8, NONE = 9,
    TOTAL = 10
}

union operand_t {
    1: fidx_t fi; // when operand is a future read
    2: value_t value; // when operand is a literal
}

struct funaryop_t {
    1: bool resolved;
    2: value_t value;
    3: operand_t param;
}

struct fbinaryop_t {
    1: bool resolved;
    2: value_t value;
    3: operand_t left;
    4: operand_t right;
}

union fstate_t {
    1: value_t value; // LITERAL
    2: fread_t rdata; // READ
    3: fidx_t fi; // POINTER
    4: funaryop_t unaryop; // EXISTS
    5: fbinaryop_t binaryop; // ADD, SUB, MUL, CONCAT, GTEQ, SUBSTR
}

struct future_t {
    1: ftype_t type;
    2: fstate_t u;
}

struct function_t {
    1: klist_t keys;
    2: fidx_t future_fi;
}

struct predicate_t {
    1: function_t func;
    2: bool expected;
}

/* update operations may either be a put or a delete
 */
enum wtype_t { PUT, REMOVE }

union wstate_t {
    1: fidx_t func_fi;
    2: value_t value; // only meaningful if type = PUT
}

struct write_t {
    1: wtype_t type;
    2: bool future;
    3: wstate_t u;
}

struct readwrite_t {
    1: read_t r;
    2: write_t w;
}

union rwdata_t {
    1: read_t r; // READ
    2: write_t w; // WRITE
    3: readwrite_t rw; // READ_WRITE
}

struct access_t {
    1: atype_t type;
    2: rwdata_t u;
}

/* transaction descriptor keeps the transaction identifier and rwset
 */
struct fwset_entry_t {
    1: function_t func;
    2: write_t w;
}

typedef list<future_t> fmap_t

struct futures_t {
    1: i32 num_fi;
    2: fmap_t fmap;
}

typedef map<key_t, access_t> rwset_t
typedef list<fwset_entry_t> fwset_t
typedef list<predicate_t> cpp_type "std::list<class predicate_t>" pset_t

struct txn_t {
    1: tid_t tid;
    2: rwset_t rwset;
    3: fwset_t future_wset;
    4: pset_t pset;
    5: futures_t futures;
}

/* prepare result TODO
 */
typedef map cpp_type "std::unordered_map<fidx_t, class rread_t>" <fidx_t, rread_t> rfmap_t

struct vote_t {
    1: vote_result_t result;
    2: rfmap_t rfmap;
}

/* multiget TODO
 */
typedef map cpp_type "std::unordered_map<key_t, class rread_t>" <key_t, rread_t> mgmap_t

struct mget_t {
  1: vote_result_t status;
  2: mgmap_t mgmap;
}

struct assert_t {
  1: vote_result_t status;
  2: bool result;
}
