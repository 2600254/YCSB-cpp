//
//  elastic_lsm.h
//  YCSB-cpp
//
//  Copyright (c) 2025 BACH-OPD HIT MDC GROUP
//  Modifications Copyright 2025 BACH-OPD HIT MDC GROUP
//

#ifndef YCSB_C_ELASTIC_LSM_H_
#define YCSB_C_ELASTIC_LSM_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include <rocksdb/elastic_lsm.h>

namespace ycsbc {

class ElasticLSMDB : public DB {
 public:
  ElasticLSMDB() {}
  ~ElasticLSMDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

 private:
  enum RocksFormat {
    kSingleRow,
  };
  RocksFormat format_;

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  Status ReadSingle(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingle(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<Field>> &result);
  Status UpdateSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status MergeSingle(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status InsertSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status DeleteSingle(const std::string &table, const std::string &key);

  Status (ElasticLSMDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (ElasticLSMDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (ElasticLSMDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (ElasticLSMDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (ElasticLSMDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;

  static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  static rocksdb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewElasticLSMDB();

} // ycsbc

#endif // YCSB_C_ELASTIC_LSM_H_

