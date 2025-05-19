//
//  bach_opd.cc
//  YCSB-cpp
//
//  Copyright (c) 2025 BACH-OPD HIT MDC GROUP
//  Modifications Copyright 2025 BACH-OPD HIT MDC GROUP
//

//#include "bach_opd.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"
#include "bach_opd.h"

#include "BACH/BACH.h"
#include "BACH/utils/types.h"

namespace {
//   const std::string PROP_NAME = "bachopd.dbname";
//   const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_STORAGE_DIR = "bachopd.storage_dir";
  const std::string PROP_STORAGE_DIR_DEFAULT = "/tmp/output/db_storage";

  const std::string PROP_MEM_TABLE_MAX_SIZE = "bachopd.memtable_max_size"; 
  const std::string PROP_MEM_TABLE_MAX_SIZE_DEFAULT = "1048576";

  const std::string PROP_READ_BUFFER_SIZE = "bachopd.read_buffer_size";
  const std::string PROP_READ_BUFFER_SIZE_DEFAULT = "1048576";

  const std::string PROP_WRITE_BUFFER_SIZE = "bachopd.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "1048576";

  const std::string PROP_MAX_WORKER_THREAD = "bachopd.max_worker_thread";
  const std::string PROP_MAX_WORKER_THREAD_DEFAULT = "16";

  const std::string PROP_FALSE_POSITIVE = "bachopd.false_positive";
  const std::string PROP_FALSE_POSITIVE_DEFAULT = "0.01";

  const std::string PROP_MAX_LEVEL = "bachopd.max_level";
  const std::string PROP_MAX_LEVEL_DEFAULT = "5";

  const std::string PROP_LEVEL_SIZE_RITIO = "bachopd.level_size_ritio";
  const std::string PROP_LEVEL_SIZE_RITIO_DEFAULT = "10";

  const std::string PROP_MAX_BLOCK_SIZE = "bachopd.max_block_size";
  const std::string PROP_MAX_BLOCK_SIZE_DEFAULT = "4096";

  const std::string PROP_MEMORY_MERGE_IN_TUPLE = "bachopd.memory_merge_in_tuple";
  const std::string PROP_MEMORY_MERGE_IN_TUPLE_DEFAULT = "2048";

  const std::string PROP_ZERO_LEVEL_FILES = "bachopd.zero_level_files";
  const std::string PROP_ZERO_LEVEL_FILES_DEFAULT = "2";

  const std::string PROP_NUM_OF_HIGH_COMPACTION_THREAD = "bachopd.num_of_high_compaction_thread";
  const std::string PROP_NUM_OF_HIGH_COMPACTION_THREAD_DEFAULT = "4";

  const std::string PROP_NUM_OF_LOW_COMPACTION_THREAD = "bachopd.num_of_low_compaction_thread";
  const std::string PROP_NUM_OF_LOW_COMPACTION_THREAD_DEFAULT = "4";

  const std::string PROP_MAX_MEMTABLE_NUM = "bachopd.max_memtable_num";
  const std::string PROP_MAX_MEMTABLE_NUM_DEFAULT = "2";
} // anonymous

namespace ycsbc {

std::unique_ptr<BACH::DB> BachopdDB::db_;
int BachopdDB::ref_cnt_ = 0;
std::mutex BachopdDB::mu_;

void BachopdDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);
  const utils::Properties &props = *props_;
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX, 
                                            CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);
  ref_cnt_++;
  if (db_) {
    return;
  }
  const std::string &db_path = props.GetProperty(PROP_STORAGE_DIR, PROP_STORAGE_DIR_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("BachOpd db path is missing");
  }
  std::shared_ptr<BACH::Options> opt = std::make_shared<BACH::Options>();
  GetOptions(props, opt);

  db_ = std::make_unique<BACH::DB>(opt, fieldcount_ + 1);
}

void BachopdDB::Cleanup() { 
  const std::lock_guard<std::mutex> lock(mu_);
}

void BachopdDB::GetOptions(const utils::Properties &props, std::shared_ptr<BACH::Options> opt) {
  opt->STORAGE_DIR = props.GetProperty(PROP_STORAGE_DIR, PROP_STORAGE_DIR_DEFAULT);

  size_t memtable_max_size = std::stoul(props.GetProperty(PROP_MEM_TABLE_MAX_SIZE, PROP_MEM_TABLE_MAX_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = memtable_max_size;

  size_t read_buffer_size = std::stoul(props.GetProperty(PROP_READ_BUFFER_SIZE, PROP_READ_BUFFER_SIZE_DEFAULT));
  opt->READ_BUFFER_SIZE = read_buffer_size;

  size_t write_buffer_size = std::stoul(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
  opt->WRITE_BUFFER_SIZE = write_buffer_size;

  size_t max_worker_thread = std::stoul(props.GetProperty(PROP_MAX_WORKER_THREAD, PROP_MAX_WORKER_THREAD_DEFAULT));
  opt->MAX_WORKER_THREAD = max_worker_thread;

  double false_positive = std::stod(props.GetProperty(PROP_FALSE_POSITIVE, PROP_FALSE_POSITIVE_DEFAULT));
  opt->FALSE_POSITIVE = false_positive;

  size_t max_level = std::stoul(props.GetProperty(PROP_MAX_LEVEL, PROP_MAX_LEVEL_DEFAULT));
  opt->MAX_LEVEL = max_level;

  size_t level_size_ritio = std::stoul(props.GetProperty(PROP_LEVEL_SIZE_RITIO, PROP_LEVEL_SIZE_RITIO_DEFAULT));
  opt->LEVEL_SIZE_RITIO = level_size_ritio;

  size_t max_block_size = std::stoul(props.GetProperty(PROP_MAX_BLOCK_SIZE, PROP_MAX_BLOCK_SIZE_DEFAULT));
  opt->MAX_BLOCK_SIZE = max_block_size;

  size_t memory_merge_in_tuple = std::stoul(props.GetProperty(PROP_MEMORY_MERGE_IN_TUPLE, PROP_MEMORY_MERGE_IN_TUPLE_DEFAULT));
  opt->MEMORY_MERGE_IN_TUPLE = memory_merge_in_tuple;

  size_t zero_level_files = std::stoul(props.GetProperty(PROP_ZERO_LEVEL_FILES, PROP_ZERO_LEVEL_FILES_DEFAULT));
  opt->ZERO_LEVEL_FILES = zero_level_files;

  size_t num_of_high_compaction_thread = std::stoul(props.GetProperty(PROP_NUM_OF_HIGH_COMPACTION_THREAD, PROP_NUM_OF_HIGH_COMPACTION_THREAD_DEFAULT));
  opt->NUM_OF_HIGH_COMPACTION_THREAD = num_of_high_compaction_thread;

  size_t num_of_low_compaction_thread = std::stoul(props.GetProperty(PROP_NUM_OF_LOW_COMPACTION_THREAD, PROP_NUM_OF_LOW_COMPACTION_THREAD_DEFAULT));
  opt->NUM_OF_LOW_COMPACTION_THREAD = num_of_low_compaction_thread;

  size_t max_memtable_num = std::stoul(props.GetProperty(PROP_MAX_MEMTABLE_NUM, PROP_MAX_MEMTABLE_NUM_DEFAULT));
  opt->MAX_MEMTABLE_NUM = max_memtable_num;
}


void BachopdDB::SerializeRow(const std::vector<Field> &values, BACH::Tuple &data){
  data.row.resize(values.size() + 1);
  for (const Field &field : values) {
    int idx = std::stoi(field.name.c_str() + field_prefix_.size()) + 1;
    data.row[idx] = field.value;
  }
}

void BachopdDB::DeserializeRowFilter(std::vector<Field> &result, const BACH::Tuple &data,
                                     const std::vector<std::string> &fields) {
  for(auto f : fields) {
    int idx = std::stoi(f.c_str() + field_prefix_.size()) + 1;
    result.push_back({f, data.row[idx]});
  }
  assert(result.size() == fields.size());
}

void BachopdDB::DeserializeRow(std::vector<Field> &result, const BACH::Tuple &data) {
  for (size_t i = 1; i < data.row.size(); ++i) {
    result.push_back({field_prefix_ + std::to_string(i), data.row[i]});
  }
}

DB::Status BachopdDB::Read(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  auto z = db_->BeginRelTransaction();
  auto data = z.GetTuple(key);
  if(data.row.empty()){
    return kNotFound;
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status BachopdDB::Scan(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  auto z = db_->BeginRelTransaction();
  auto ans = z.ScanKTuples(len, key);
  for(auto & data : ans) {
    result.emplace_back();
    if (fields != nullptr) {
      DeserializeRowFilter(result.back(), data, *fields);
    } else {
      DeserializeRow(result.back(), data);
      assert(result.back().size() == static_cast<size_t>(fieldcount_));
    }
  }
  return kOK;
}

DB::Status BachopdDB::Update(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  BACH::Tuple data;
  data.row.push_back(key);
  SerializeRow(values, data);
  auto z = db_->BeginRelTransaction();
  z.PutTuple(data, key, 1.0);
  return kOK;
}

DB::Status BachopdDB::Insert(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  BACH::Tuple data;
  data.row.push_back(key);
  SerializeRow(values, data);
  auto z = db_->BeginRelTransaction();
  z.PutTuple(data, key, 1.0);
  return kOK;
}

DB::Status BachopdDB::Delete(const std::string &table, const std::string &key) {
  BACH::Tuple data;
  auto z = db_->BeginRelTransaction();
  z.DelTuple(key);
  return kOK;
}

DB::Status BachopdDB::Filter(const std::string &table, const std::vector<DB::Field> &lvalue,
                             const std::vector<DB::Field> &rvalue, 
                             const std::vector<std::string> *fields, 
                             std::vector<std::vector<Field>> &result) {
  BACH::Tuple data;
  auto z = db_->BeginRelTransaction();
  if(fields != nullptr) {
    for (const auto &f : *fields) {
      int i = std::stoi(f.c_str() + field_prefix_.size());
      z.GetTuplesFromRange(i + 1, lvalue[0].value, rvalue[0].value);
    }
  } else {
    for (size_t i = 0; i < lvalue.size(); ++i) {
      z.GetTuplesFromRange(i + 1, lvalue[0].value, rvalue[0].value);
    }
  }
  return kOK;
}

DB *NewBachopdDB() {
  return new BachopdDB;
}

const bool registered = DBFactory::RegisterDB("bachopd", NewBachopdDB);

} // ycsbc
