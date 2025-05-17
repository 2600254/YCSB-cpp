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
              const std::vector<std::string> *fields, std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);

  Status Filter(const std::string &table, const std::vector<DB::Field> &lvalue,
                const std::vector<DB::Field> &rvalue, const std::vector<std::string> *fields, 
                std::vector<std::vector<Field>> &result);

 private:

  void GetOptions(const utils::Properties &props, std::shared_ptr<BACH::Options> opt);
  inline void SerializeRow(const std::vector<Field> &values, BACH::Tuple &data);


  inline void DeserializeRowFilter(std::vector<Field> &values, const BACH::Tuple &data,
                                   const std::vector<std::string> &fields);

  inline void DeserializeRow(std::vector<Field> &values, const BACH::Tuple &data);

  int fieldcount_;

  static std::unique_ptr<BACH::DB> db_;
  static int ref_cnt_;
  static std::mutex mu_;
  std::string field_prefix_;
};

DB *NewBachopdDB();

} //ycsbc
#endif //YCSB_C_BACH_OPD_H_