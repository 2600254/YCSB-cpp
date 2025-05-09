//
//  bach_opd.h
//  YCSB-cpp
//
//  Copyright (c) 2025 BACH-OPD HIT MDC GROUP
//  Modifications Copyright 2025 BACH-OPD HIT MDC GROUP
//

#ifndef YCSB_C_BACH_OPD_H_
#define YCSB_C_BACH_OPD_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"

#include <BACH/BACH.h>

namespace ycsbc {

class BachopdDB : public DB{
 public:
  BachopdDB() {}
  ~BachopdDB() {}

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

  void GetOptions(const utils::Properties &props, std::shared_ptr<BACH::Options> opt);
  static void SerializeRow(const std::vector<Field> &values, BACH::Tuple &data);


  static void DeserializeRowFilter(std::vector<Field> &values, const BACH::Tuple &data,
                                   const std::vector<std::string> &fields);

  static void DeserializeRow(std::vector<Field> &values, const BACH::Tuple &data);

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

  Status (BachopdDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (BachopdDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (BachopdDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (BachopdDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (BachopdDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;

  static std::unique_ptr<BACH::DB> db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewBachopdDB();

} //ycsbc
#endif //YCSB_C_BACH_OPD_H_