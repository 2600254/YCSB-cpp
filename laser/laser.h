//
//  laser_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LASER_DB_H_
#define YCSB_C_LASER_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include "rocksdb/db.h"
#include <fstream>
//#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

#include <vector>
#include <cstdio>
#include <string>
#include <iostream>
#include <sstream>
#include <thread>         // std::this_thread::sleep_for
#include <chrono>
#include <stdlib.h>
#include <math.h>
#include <random>
#include <ctime>
#include <atomic>
#include <algorithm>
#include <iomanip>
#include <set>
#include <unordered_set>
#include <mutex>
#include <inttypes.h>
#include "rocksdb/options.h"

namespace ycsbc {

class LaserDB : public DB {
 public:
  LaserDB() {}
  ~LaserDB() {}

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

  Status Filter(const std::string &table, const std::vector<DB::Field> &lvalue,
                   const std::vector<DB::Field> &rvalue, const std::vector<std::string> *fields,
                   std::vector<std::vector<Field>> &result) {
    return (this->*(method_filter_))(table, lvalue, rvalue, fields, result);
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
  Status FilterSingle(const std::string &table, const std::vector<Field> &lvalue,
                     const std::vector<Field> &rvalue, const std::vector<std::string> *fields,
                     std::vector<std::vector<Field>> &result);

  Status (LaserDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (LaserDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (LaserDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (LaserDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (LaserDB::*method_delete_)(const std::string &, const std::string &);
  Status (LaserDB::*method_filter_)(const std::string &, const std::vector<Field> &,
                                      const std::vector<Field> &, const std::vector<std::string> *,
                                      std::vector<std::vector<Field>> &);

  int fieldcount_;

  static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  static rocksdb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewLaserDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_

