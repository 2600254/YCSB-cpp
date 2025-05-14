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
  const std::string PROP_STORAGE_DIR_DEFAULT = "/home/wjj/Benchmark/YCSB-cpp/db";

  const std::string PROP_MERGING_STRATEGY = "bachopd.merging_strategy";
  const std::string PROP_MERGING_STRATEGY_DEFAULT = "ELASTIC";

  const std::string PROP_MEM_TABLE_MAX_SIZE = "bachopd.memtable_max_size"; 
  const std::string PROP_MEM_TABLE_MAX_SIZE_DEFAULT = "1048576";

  const std::string PROP_LEVEL_0_MAX_SIZE = "bachopd.level_0_max_size";
  const std::string PROP_LEVEL_0_MAX_SIZE_DEFAULT = "536870912";

  const std::string PROP_VERTEX_PROPERTY_MAX_SIZE = "bachopd.vertex_max_size";
  const std::string PROP_VERTEX_PROPERTY_MAX_SIZE_DEFAULT = "67108864";

  const std::string PROP_MEMORY_MERGE_NUM = "bachopd.memory_merge_num";
  const std::string PROP_MEMORY_MERGE_NUM_DEFAULT = "8192";

  const std::string PROP_FILE_MERGE_NUM = "bachopd.file_merge_num";
  const std::string PROP_FILE_MERGE_NUM_DEFAULT = "32";

  const std::string PROP_READ_BUFFER_SIZE = "bachopd.read_buffer_size";
  const std::string PROP_READ_BUFFER_SIZE_DEFAULT = "1048576";

  const std::string PROP_WRITE_BUFFER_SIZE = "bachopd.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "1048576";

  const std::string PROP_NUM_OF_COMPACTION_THREAD = "bachopd.num_of_compaction_thread";
  const std::string PROP_NUM_OF_COMPACTION_THREAD_DEFAULT = "16";

  const std::string PROP_QUERY_LIST_SIZE = "bachopd.query_list_size";
  const std::string PROP_QUERY_LIST_SIZE_DEFAULT = "160";

  const std::string PROP_MAX_FILE_READER_CACHE_SIZE = "bachopd.max_file_reader_cache_size";
  const std::string PROP_MAX_FILE_READER_CACHE_SIZE_DEFAULT = "0";

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

} // anonymous

namespace ycsbc {

std::unique_ptr<BACH::DB> BachopdDB::db_;
int BachopdDB::ref_cnt_ = 0;
std::mutex BachopdDB::mu_;
BACH::idx_t column_num = 2;

void BachopdDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);
  const utils::Properties &props = *props_;
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));
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

  db_ = std::make_unique<BACH::DB>(opt, column_num);
  method_read_ = &BachopdDB::ReadSingle;
  method_scan_ = &BachopdDB::ScanSingle;
  method_update_ = &BachopdDB::UpdateSingle;
  method_insert_ = &BachopdDB::InsertSingle;
  method_delete_ = &BachopdDB::DeleteSingle;
}

void BachopdDB::Cleanup() { 
  const std::lock_guard<std::mutex> lock(mu_);
}

void BachopdDB::GetOptions(const utils::Properties &props, std::shared_ptr<BACH::Options> opt) {
  opt->STORAGE_DIR = props.GetProperty(PROP_STORAGE_DIR, PROP_STORAGE_DIR_DEFAULT);
  std::string merging_strategy = props.GetProperty(PROP_MERGING_STRATEGY, PROP_MERGING_STRATEGY_DEFAULT);
  if(merging_strategy == "ELASTIC"){
    opt->MERGING_STRATEGY = BACH::Options::MergingStrategy::ELASTIC;
  }else if (merging_strategy == "TIERING"){
    opt->MERGING_STRATEGY = BACH::Options::MergingStrategy::TIERING;
  }else if (merging_strategy == "LEVELING"){
    opt->MERGING_STRATEGY = BACH::Options::MergingStrategy::LEVELING;
  }else {
    throw utils::Exception("Unknown merging strategy default to ELASTIC");
    opt->MERGING_STRATEGY = BACH::Options::MergingStrategy::ELASTIC;
  }

  size_t memtable_max_size = std::stoul(props.GetProperty(PROP_MEM_TABLE_MAX_SIZE, PROP_MEM_TABLE_MAX_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = memtable_max_size;

  size_t level_0_max_size = std::stoul(props.GetProperty(PROP_LEVEL_0_MAX_SIZE, PROP_LEVEL_0_MAX_SIZE_DEFAULT));
  opt->LEVEL_0_MAX_SIZE = level_0_max_size;

  size_t vertex_max_size = std::stoul(props.GetProperty(PROP_VERTEX_PROPERTY_MAX_SIZE, PROP_VERTEX_PROPERTY_MAX_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = vertex_max_size;

  BACH::vertex_t memory_merge_num = std::stoul(props.GetProperty(PROP_MEMORY_MERGE_NUM, PROP_MEMORY_MERGE_NUM_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = memory_merge_num;

  BACH::vertex_t file_merge_num = std::stoul(props.GetProperty(PROP_FILE_MERGE_NUM, PROP_FILE_MERGE_NUM_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = file_merge_num;

  size_t read_buffer_size = std::stoul(props.GetProperty(PROP_READ_BUFFER_SIZE, PROP_READ_BUFFER_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = read_buffer_size;

  size_t write_buffer_size = std::stoul(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = write_buffer_size;

  size_t num_of_compaction_thread = std::stoul(props.GetProperty(PROP_NUM_OF_COMPACTION_THREAD, PROP_NUM_OF_COMPACTION_THREAD_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = num_of_compaction_thread;

  size_t query_list_size = std::stoul(props.GetProperty(PROP_QUERY_LIST_SIZE, PROP_QUERY_LIST_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = query_list_size;

  size_t max_file_reader_cache_size = std::stoul(props.GetProperty(PROP_MAX_FILE_READER_CACHE_SIZE, PROP_MAX_FILE_READER_CACHE_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = max_file_reader_cache_size;

  size_t max_worker_thread = std::stoul(props.GetProperty(PROP_MAX_WORKER_THREAD, PROP_MAX_WORKER_THREAD_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = max_worker_thread;

  double false_positive = std::stod(props.GetProperty(PROP_FALSE_POSITIVE, PROP_FALSE_POSITIVE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = false_positive;

  size_t max_level = std::stoul(props.GetProperty(PROP_MAX_LEVEL, PROP_MAX_LEVEL_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = max_level;

  size_t level_size_ritio = std::stoul(props.GetProperty(PROP_LEVEL_SIZE_RITIO, PROP_LEVEL_SIZE_RITIO_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = level_size_ritio;

  size_t max_block_size = std::stoul(props.GetProperty(PROP_MAX_BLOCK_SIZE, PROP_MAX_BLOCK_SIZE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = max_block_size;

  size_t memory_merge_in_tuple = std::stoul(props.GetProperty(PROP_MEMORY_MERGE_IN_TUPLE, PROP_MEMORY_MERGE_IN_TUPLE_DEFAULT));
  opt->MEM_TABLE_MAX_SIZE = memory_merge_in_tuple;

}


void BachopdDB::SerializeRow(const std::vector<Field> &values, BACH::Tuple &data){
  std::string str;
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    str.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    str.append(field.name.data(), field.name.size());
    len = field.value.size();
    str.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    str.append(field.value.data(), field.value.size());
    data.row.push_back(str);
    str.clear();
  }
}

void BachopdDB::DeserializeRowFilter(std::vector<Field> &result, const BACH::Tuple &data,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  const char *p = data.row[1].data();
  const char *lim = p + data.row[1].size();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      result.push_back({field, value});
      filter_iter++;
    }
  }
  assert(result.size() == fields.size());
}

void BachopdDB::DeserializeRow(std::vector<Field> &result, const BACH::Tuple &data) {
  const char *p = data.row[1].data();
  const char *lim = p + data.row[1].size();
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    result.push_back({field, value});
  }
  // for(auto it =data.row.begin(); it != data.row.end() ; ++it){
  //   const char *p = (*it).data();
  //   uint32_t len = *reinterpret_cast<const uint32_t *>(p);
  //   p += sizeof(uint32_t);
  //   std::string field(p, static_cast<const size_t>(len));
  //   p += len;
  //   len = *reinterpret_cast<const uint32_t *>(p);
  //   p += sizeof(uint32_t);
  //   std::string value(p, static_cast<const size_t>(len));
  //   p += len;
  //   result.push_back({field, value});
  // }
}

DB::Status BachopdDB::ReadSingle(const std::string &table, const std::string &key,
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

DB::Status BachopdDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  

  return kOK;
}

DB::Status BachopdDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  BACH::Tuple data;
  data.row.push_back(key);
  SerializeRow(values, data);
  auto z = db_->BeginRelTransaction();
  z.PutTuple(data, key, 1.0);
  return kOK;
}

DB::Status BachopdDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  std::string data;
  return kOK;
}

DB::Status BachopdDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  BACH::Tuple data;
  data.row.push_back(key);
  SerializeRow(values, data);
  auto z = db_->BeginRelTransaction();
  z.PutTuple(data, key, 1.0);
  return kOK;
}

DB::Status BachopdDB::DeleteSingle(const std::string &table, const std::string &key) {
  BACH::Tuple data;
  data.row.push_back(key);
  auto z = db_->BeginRelTransaction();
  z.DelTuple(data, key);
  return kOK;
}

DB *NewBachopdDB() {
  return new BachopdDB;
}

const bool registered = DBFactory::RegisterDB("bachopd", NewBachopdDB);

} // ycsbc
