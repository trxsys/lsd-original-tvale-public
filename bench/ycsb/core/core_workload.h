//
//  core_workload.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CORE_WORKLOAD_H_
#define YCSB_C_CORE_WORKLOAD_H_

#include "counter_generator.h"
#include "db.h"
#include "discrete_generator.h"
#include "generator.h"
#include "properties.h"
#include "utils.h"
#include <string>
#include <vector>

namespace ycsbc {

enum Operation
{
  INSERT,
  READ,
  UPDATE,
  SCAN,
  READMODIFYWRITE
};

class CoreWorkload
{
public:
  ///
  /// The name of the database table to run queries against.
  ///
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;

  ///
  /// The name of the property for the field length distribution.
  /// Options are "uniform", "zipfian" (favoring short records), and "constant".
  ///
  static const std::string FIELD_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string FIELD_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the length of a field in bytes.
  ///
  static const std::string FIELD_LENGTH_PROPERTY;
  static const std::string FIELD_LENGTH_DEFAULT;

  ///
  /// The name of the property for the proportion of read transactions.
  ///
  static const std::string READ_PROPORTION_PROPERTY;
  static const std::string READ_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of update transactions.
  ///
  static const std::string UPDATE_PROPORTION_PROPERTY;
  static const std::string UPDATE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of insert transactions.
  ///
  static const std::string INSERT_PROPORTION_PROPERTY;
  static const std::string INSERT_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of scan transactions.
  ///
  static const std::string SCAN_PROPORTION_PROPERTY;
  static const std::string SCAN_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the proportion of
  /// read-modify-write transactions.
  ///
  static const std::string READMODIFYWRITE_PROPORTION_PROPERTY;
  static const std::string READMODIFYWRITE_PROPORTION_DEFAULT;

  ///
  /// The name of the property for the the distribution of request keys.
  /// Options are "uniform", "zipfian" and "latest".
  ///
  static const std::string REQUEST_DISTRIBUTION_PROPERTY;
  static const std::string REQUEST_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the max scan length (number of records).
  ///
  static const std::string MAX_SCAN_LENGTH_PROPERTY;
  static const std::string MAX_SCAN_LENGTH_DEFAULT;

  ///
  /// The name of the property for the scan length distribution.
  /// Options are "uniform" and "zipfian" (favoring short scans).
  ///
  static const std::string SCAN_LENGTH_DISTRIBUTION_PROPERTY;
  static const std::string SCAN_LENGTH_DISTRIBUTION_DEFAULT;

  ///
  /// The name of the property for the order to insert records.
  /// Options are "ordered" or "hashed".
  ///
  static const std::string INSERT_ORDER_PROPERTY;
  static const std::string INSERT_ORDER_DEFAULT;

  static const std::string INSERT_START_PROPERTY;
  static const std::string INSERT_START_DEFAULT;

  static const std::string RECORD_COUNT_PROPERTY;
  static const std::string OPERATION_COUNT_PROPERTY;

  ///
  /// The name of the property for the number of
  /// operations per transaction.
  ///
  static const std::string OPERATIONS_PER_TRANSACTION_PROPERTY;
  static const std::string OPERATIONS_PER_TRANSACTION_DEFAULT;

  static const std::string HOTKEY_PERCENTAGE_PROPERTY;
  static const std::string HOTKEY_PERCENTAGE_DEFAULT;

  static const std::string COUNTER_INITIAL_PROPERTY;
  static const std::string COUNTER_INITIAL_DEFAULT;

  ///
  /// Initialize the scenario.
  /// Called once, in the main client thread, before any operations are started.
  ///
  virtual void Init(const utils::Properties& p);

  virtual void BuildValue(std::string& value);

  virtual std::string NextTable() { return table_name_; }
  virtual std::string NextSequenceKey();    /// Used for loading data
  virtual std::string NextTransactionKey(); /// Used for transactions
  virtual Operation NextOperation() { return op_chooser_.Next(); }
  virtual size_t NextScanLength() { return scan_len_chooser_->Next(); }
  virtual int GetHotKeyPercentage() const { return hot_key_perc_; }
  virtual int GetCounterInitialValue() const { return counter_init_val_; }

  size_t ops_per_txn() const { return ops_per_txn_; }

  CoreWorkload()
    : field_count_(1)
    , field_len_generator_(NULL)
    , key_generator_(NULL)
    , key_chooser_(NULL)
    , scan_len_chooser_(NULL)
    , insert_key_sequence_(3)
    , ordered_inserts_(true)
    , record_count_(0)
    , ops_per_txn_(0)
    , hot_key_perc_(0)
    , counter_init_val_(0)
  {
  }

  virtual ~CoreWorkload()
  {
    if (field_len_generator_)
      delete field_len_generator_;
    if (key_generator_)
      delete key_generator_;
    if (key_chooser_)
      delete key_chooser_;
    if (scan_len_chooser_)
      delete scan_len_chooser_;
  }

protected:
  static Generator<uint64_t>* GetFieldLenGenerator(const utils::Properties& p);

public:
  std::string BuildKeyName(uint64_t key_num);

protected:
  std::string table_name_;
  int field_count_;
  Generator<uint64_t>* field_len_generator_;
  Generator<uint64_t>* key_generator_;
  DiscreteGenerator<Operation> op_chooser_;
  Generator<uint64_t>* key_chooser_;
  Generator<uint64_t>* scan_len_chooser_;
  CounterGenerator insert_key_sequence_;
  bool ordered_inserts_;
  size_t record_count_;
  size_t ops_per_txn_;
  int hot_key_perc_;
  int counter_init_val_;
};

inline std::string
CoreWorkload::NextSequenceKey()
{
  uint64_t key_num = key_generator_->Next();
  return BuildKeyName(key_num);
}

inline std::string
CoreWorkload::NextTransactionKey()
{
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return BuildKeyName(key_num);
}

inline std::string
CoreWorkload::BuildKeyName(uint64_t key_num)
{
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  std::string const str = std::to_string(key_num);
  auto const size = str.size();
  if (size == 16) {
    return str;
  } else {
    assert(size < 16);
    std::string result;
    result.append(16 - size, '0');
    result.append(str);
    return result;
  }
}

} // ycsbc

#endif // YCSB_C_CORE_WORKLOAD_H_
