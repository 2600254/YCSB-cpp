//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "laser.h"
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/write_batch.h>

namespace {
  const std::string COLUMN_NUM = "column_num";
  const std::string COLUMN_NUM_DEFAULT = "1";

  const std::string COLUMN_FROM_K = "column_from_k";
  const std::string COLUMN_FROM_K_DEFAULT = "2";

  const std::string MAX_LEVEL = "max_level"; 
  const std::string MAX_LEVEL_DEFAULT = "7";

  const std::string PROP_NAME = "laser.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "laser.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_DESTROY = "laser.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";
  
  const std::string PROP_COMPRESSION = "laser.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";
  
  const std::string PROP_MAX_BG_JOBS = "laser.max_background_jobs";
  const std::string PROP_MAX_BG_JOBS_DEFAULT = "0";
  
  const std::string PROP_TARGET_FILE_SIZE_BASE = "laser.target_file_size_base";
  const std::string PROP_TARGET_FILE_SIZE_BASE_DEFAULT = "0";
  
  const std::string PROP_TARGET_FILE_SIZE_MULT = "laser.target_file_size_multiplier";
  const std::string PROP_TARGET_FILE_SIZE_MULT_DEFAULT = "0";
  
  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE = "laser.max_bytes_for_level_base";
  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT = "0";
  
  const std::string PROP_WRITE_BUFFER_SIZE = "laser.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";
  
  const std::string PROP_MAX_OPEN_FILES = "laser.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "-1";
  
  const std::string PROP_MAX_WRITE_BUFFER = "laser.max_write_buffer_number";
  const std::string PROP_MAX_WRITE_BUFFER_DEFAULT = "0";
  
  const std::string PROP_USE_DIRECT_WRITE = "laser.use_direct_io_for_flush_compaction";
  const std::string PROP_USE_DIRECT_WRITE_DEFAULT = "false";

  const std::string PROP_USE_DIRECT_READ = "laser.use_direct_reads";
  const std::string PROP_USE_DIRECT_READ_DEFAULT = "false";
  
  const std::string PROP_USE_MMAP_WRITE = "laser.allow_mmap_writes";
  const std::string PROP_USE_MMAP_WRITE_DEFAULT = "false";
  
  const std::string PROP_USE_MMAP_READ = "laser.allow_mmap_reads";
  const std::string PROP_USE_MMAP_READ_DEFAULT = "false";

  const std::string PROP_CACHE_SIZE = "laser.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_BLOOM_BITS = "laser.bloom_bits";
  const std::string PROP_BLOOM_BITS_DEFAULT = "0";
  
  const std::string PROP_COMPRESSED_CACHE_SIZE = "laser.compressed_cache_size";
  const std::string PROP_COMPRESSED_CACHE_SIZE_DEFAULT = "0";
  
  const std::string PROP_INCREASE_PARALLELISM = "laser.increase_parallelism";
  const std::string PROP_INCREASE_PARALLELISM_DEFAULT = "false";
  
  const std::string PROP_OPTIMIZE_LEVELCOMP = "laser.optimize_level_style_compaction";
  const std::string PROP_OPTIMIZE_LEVELCOMP_DEFAULT = "false";
  
  
  const std::string PROP_COMPACTION_PRI = "laser.compaction_pri";
  const std::string PROP_COMPACTION_PRI_DEFAULT = "-1";
  
  const std::string PROP_L0_COMPACTION_TRIGGER = "laser.level0_file_num_compaction_trigger";
  const std::string PROP_L0_COMPACTION_TRIGGER_DEFAULT = "0";
  
  const std::string PROP_L0_SLOWDOWN_TRIGGER = "laser.level0_slowdown_writes_trigger";
  const std::string PROP_L0_SLOWDOWN_TRIGGER_DEFAULT = "0";
  
  const std::string PROP_L0_STOP_TRIGGER = "laser.level0_stop_writes_trigger";
  const std::string PROP_L0_STOP_TRIGGER_DEFAULT = "0";
    
  const std::string PROP_OPTIONS_FILE = "laser.optionsfile";
  const std::string PROP_OPTIONS_FILE_DEFAULT = "";
  
  const std::string PROP_ENV_URI = "laser.env_uri";
  const std::string PROP_ENV_URI_DEFAULT = "";
  
  const std::string PROP_FS_URI = "laser.fs_uri";
  const std::string PROP_FS_URI_DEFAULT = "";
  
  const std::string PROP_MERGEUPDATE = "laser.mergeupdate";
  const std::string PROP_MERGEUPDATE_DEFAULT = "false";

  static std::shared_ptr<rocksdb::Env> env_guard;
  static std::shared_ptr<rocksdb::Cache> block_cache;
  #if ROCKSDB_MAJOR < 8
  static std::shared_ptr<rocksdb::Cache> block_cache_compressed;
  #endif
} // anonymous

namespace ycsbc {
  using namespace std;
  using namespace rocksdb;
  using namespace std::chrono;

  const uint32_t cg_size = 2;

  vector<vector<tuple<uint32_t, uint32_t>>> createCGMatrix(int num_levels, uint32_t column_num) {
    vector<vector<tuple<uint32_t, uint32_t>>> matrix(num_levels);

    for(int i = 0; i < 1; i++) {
        vector<tuple<uint32_t, uint32_t>> cg_i(1);
        cg_i[0] = make_tuple(0,column_num-1);
        matrix[i] = cg_i;
    }   
	int num_cgs = column_num/cg_size;	
	for(int i = 1; i < num_levels; i++) {
	  vector<tuple<uint32_t, uint32_t>> cg_i(num_cgs);
          for(int j = 0; j < num_cgs; j++) {
            cg_i[j] = make_tuple(static_cast<uint32_t>(j*cg_size), static_cast<uint32_t>(j*cg_size + cg_size -1));
          }
          matrix[i] = cg_i;
	}
    return matrix;
}


  std::vector<rocksdb::ColumnFamilyHandle *> LaserDB::cf_handles_;
  rocksdb::DB *LaserDB::db_ = nullptr;
  int LaserDB::ref_cnt_ = 0;
  std::mutex LaserDB::mu_;
  
  void LaserDB::Init() {
    // merge operator disabled by default due to link error
    #ifdef USE_MERGEUPDATE
    class YCSBUpdateMerge : public rocksdb::AssociativeMergeOperator {
      public:
      virtual bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
        const rocksdb::Slice &value, std::string *new_value,
        rocksdb::Logger *logger) const override {
          assert(existing_value);
          
          std::vector<Field> values;
          const char *p = existing_value->data();
          const char *lim = p + existing_value->size();
      DeserializeRow(values, p, lim);

      std::vector<Field> new_values;
      p = value.data();
      lim = p + value.size();
      DeserializeRow(new_values, p, lim);

      for (Field &new_field : new_values) {
        bool found = false;
        for (Field &field : values) {
          if (field.name == new_field.name) {
            found = true;
            field.value = new_field.value;
            break;
          }
        }
        if (!found) {
          values.push_back(new_field);
        }
      }

      SerializeRow(values, *new_value);
      return true;
    }

    virtual const char *Name() const override {
      return "YCSBUpdateMerge";
    }
  };
#endif
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
    method_read_ = &LaserDB::ReadSingle;
    method_scan_ = &LaserDB::ScanSingle;
    method_update_ = &LaserDB::UpdateSingle;
    method_insert_ = &LaserDB::InsertSingle;
    method_delete_ = &LaserDB::DeleteSingle;
    method_filter_ = &LaserDB::FilterSingle;
#ifdef USE_MERGEUPDATE
    if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
      method_update_ = &LaserDB::MergeSingle;
    }
#endif
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }

  const std::string &db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("Laser db path is missing");
  }

  rocksdb::Options opt;
  opt.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  GetOptions(props, &opt, &cf_descs);
#ifdef USE_MERGEUPDATE
  opt.merge_operator.reset(new YCSBUpdateMerge);
#endif

  rocksdb::Status s;
  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = rocksdb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("Laser DestroyDB: ") + s.ToString());
    }
  }
  if (cf_descs.empty()) {
    s = rocksdb::DB::Open(opt, db_path, &db_);
  } else {
    s = rocksdb::DB::Open(opt, db_path, cf_descs, &cf_handles_, &db_);
  }
  if (!s.ok()) {
    throw utils::Exception(std::string("Laser Open: ") + s.ToString());
  }
}

void LaserDB::Cleanup() { 
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  for (size_t i = 0; i < cf_handles_.size(); i++) {
    if (cf_handles_[i] != nullptr) {
      db_->DestroyColumnFamilyHandle(cf_handles_[i]);
      cf_handles_[i] = nullptr;
    }
  }
  delete db_;
}

void LaserDB::GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                           std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs) {
  
  int column_nums = std::stoi(props.GetProperty(COLUMN_NUM, COLUMN_NUM_DEFAULT));
  int column_from_k = std::stoi(props.GetProperty(COLUMN_FROM_K, COLUMN_FROM_K_DEFAULT));
  int levels = std::stoi(props.GetProperty(MAX_LEVEL, MAX_LEVEL_DEFAULT));
  opt->cg_range_matrix= createCGMatrix(levels, column_nums);
  opt->levels_cg_count = {1, column_nums/cg_size, column_nums/cg_size, column_nums/cg_size, column_nums/cg_size, column_nums/cg_size, column_nums/cg_size, column_nums/cg_size};
  opt->column_num = column_nums;
  opt->column_from_k = column_from_k;

  const std::string options_file = props.GetProperty(PROP_OPTIONS_FILE, PROP_OPTIONS_FILE_DEFAULT);
  if (options_file != "") {
  } else {
    const std::string compression_type = props.GetProperty(PROP_COMPRESSION,
                                                           PROP_COMPRESSION_DEFAULT);
    if (compression_type == "no") {
      opt->compression = rocksdb::kNoCompression;
    } else if (compression_type == "snappy") {
      opt->compression = rocksdb::kSnappyCompression;
    } else if (compression_type == "zlib") {
      opt->compression = rocksdb::kZlibCompression;
    } else if (compression_type == "bzip2") {
      opt->compression = rocksdb::kBZip2Compression;
    } else if (compression_type == "lz4") {
      opt->compression = rocksdb::kLZ4Compression;
    } else if (compression_type == "lz4hc") {
      opt->compression = rocksdb::kLZ4HCCompression;
    } else if (compression_type == "xpress") {
      opt->compression = rocksdb::kXpressCompression;
    } else if (compression_type == "zstd") {
      opt->compression = rocksdb::kZSTD;
    } else {
      throw utils::Exception("Unknown compression type");
    }

    int val = std::stoi(props.GetProperty(PROP_MAX_BG_JOBS, PROP_MAX_BG_JOBS_DEFAULT));
    if (val != 0) {
      opt->max_background_jobs = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_BASE, PROP_TARGET_FILE_SIZE_BASE_DEFAULT));
    if (val != 0) {
      opt->target_file_size_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_MULT, PROP_TARGET_FILE_SIZE_MULT_DEFAULT));
    if (val != 0) {
      opt->target_file_size_multiplier = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_BYTES_FOR_LEVEL_BASE, PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT));
    if (val != 0) {
      opt->max_bytes_for_level_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if (val != 0) {
      opt->write_buffer_size = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_WRITE_BUFFER, PROP_MAX_WRITE_BUFFER_DEFAULT));
    if (val != 0) {
      opt->max_write_buffer_number = val;
    }
    val = std::stoi(props.GetProperty(PROP_COMPACTION_PRI, PROP_COMPACTION_PRI_DEFAULT));
    if (val != -1) {
      opt->compaction_pri = static_cast<rocksdb::CompactionPri>(val);
    }
    val = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if (val != 0) {
      opt->max_open_files = val;
    }

    val = std::stoi(props.GetProperty(PROP_L0_COMPACTION_TRIGGER, PROP_L0_COMPACTION_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_file_num_compaction_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_SLOWDOWN_TRIGGER, PROP_L0_SLOWDOWN_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_slowdown_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_STOP_TRIGGER, PROP_L0_STOP_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_stop_writes_trigger = val;
    }

    if (props.GetProperty(PROP_USE_DIRECT_WRITE, PROP_USE_DIRECT_WRITE_DEFAULT) == "true") {
      opt->use_direct_io_for_flush_and_compaction = true;
    }
    if (props.GetProperty(PROP_USE_DIRECT_READ, PROP_USE_DIRECT_READ_DEFAULT) == "true") {
      opt->use_direct_reads = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_WRITE, PROP_USE_MMAP_WRITE_DEFAULT) == "true") {
      opt->allow_mmap_writes = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_READ, PROP_USE_MMAP_READ_DEFAULT) == "true") {
      opt->allow_mmap_reads = true;
    }
    if (props.GetProperty(PROP_INCREASE_PARALLELISM, PROP_INCREASE_PARALLELISM_DEFAULT) == "true") {
      opt->IncreaseParallelism();
    }
    if (props.GetProperty(PROP_OPTIMIZE_LEVELCOMP, PROP_OPTIMIZE_LEVELCOMP_DEFAULT) == "true") {
      opt->OptimizeLevelStyleCompaction();
    }
  }
}

void LaserDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void LaserDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
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
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void LaserDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void LaserDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
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
    values.push_back({field, value});
  }
}

void LaserDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

DB::Status LaserDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("Laser Get: ") + s.ToString());
  }
  if (fields != nullptr) {
    DeserializeRowFilter(result, data, *fields);
  } else {
    DeserializeRow(result, data);
    assert(result.size() == static_cast<size_t>(fieldcount_));
  }
  return kOK;
}

DB::Status LaserDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
  rocksdb::Iterator *db_iter = db_->NewIterator(rocksdb::ReadOptions());
  db_iter->Seek(key);
  for (int i = 0; db_iter->Valid() && i < len; i++) {
    std::string data = db_iter->value().ToString();
    result.push_back(std::vector<Field>());
    std::vector<Field> &values = result.back();
    if (fields != nullptr) {
      DeserializeRowFilter(values, data, *fields);
    } else {
      DeserializeRow(values, data);
      assert(values.size() == static_cast<size_t>(fieldcount_));
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB::Status LaserDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &data);
  if (s.IsNotFound()) {
    return kNotFound;
  } else if (!s.ok()) {
    throw utils::Exception(std::string("Laser Get: ") + s.ToString());
  }
  std::vector<Field> current_values;
  DeserializeRow(current_values, data);
  assert(current_values.size() == static_cast<size_t>(fieldcount_));
  for (Field &new_field : values) {
    bool found MAYBE_UNUSED = false;
    for (Field &cur_field : current_values) {
      if (cur_field.name == new_field.name) {
        found = true;
        cur_field.value = new_field.value;
        break;
      }
    }
    assert(found);
  }
  rocksdb::WriteOptions wopt;

  data.clear();
  SerializeRow(current_values, data);
  s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("Laser Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status LaserDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Merge(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("Laser Merge: ") + s.ToString());
  }
  return kOK;
}

DB::Status LaserDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
  std::string data;
  SerializeRow(values, data);
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Put(wopt, key, data);
  if (!s.ok()) {
    throw utils::Exception(std::string("Laser Put: ") + s.ToString());
  }
  return kOK;
}

DB::Status LaserDB::DeleteSingle(const std::string &table, const std::string &key) {
  rocksdb::WriteOptions wopt;
  rocksdb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("Laser Delete: ") + s.ToString());
  }
  return kOK;
}

DB::Status LaserDB::FilterSingle(const std::string &table, const std::vector<Field> &lvalue,
                                   const std::vector<Field> &rvalue, const std::vector<std::string> *fields,
                                   std::vector<std::vector<Field>> &result) {
  rocksdb::Iterator *db_iter = db_->NewIterator(rocksdb::ReadOptions());
  db_iter->SeekToFirst();
  std::vector<std::string> filter_field = {lvalue[0].name};
  std::vector<std::string> l_value = {lvalue[0].value};
  std::vector<std::string> r_value = {rvalue[0].value};
  while (db_iter->Valid()) {
    std::string data = db_iter->value().ToString();
    std::vector<Field> values;
    DeserializeRow(values, data);
    size_t field_id = 0;
    for (; field_id < values.size(); field_id++) {
      if (values[field_id].name == filter_field[0]) {
        break;
      }
    }
    assert(field_id < values.size());
    if (values[field_id].value >= l_value[0] && values[field_id].value <= r_value[0]) {
      result.push_back(std::vector<Field>());
      std::vector<Field> &result_values = result.back();
      if (fields != nullptr) {
        DeserializeRowFilter(result_values, data, *fields);
      } else {
        result_values = values;
      }
    }
    db_iter->Next();
  }
  delete db_iter;
  return kOK;
}

DB *NewLaserDB() {
  return new LaserDB;
}

const bool registered = DBFactory::RegisterDB("laser", NewLaserDB);

} // ycsbc
