#ifndef YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_
#define YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_

#include "utils/utils.h"
#include <array>
#include <memory>
#include <random>
#include <cstdio>
#include "generator.h"

namespace ycsbc {

class DistinctValueGenerator {
 public:
  DistinctValueGenerator() : off_(-1) {}
  ~DistinctValueGenerator() {
    delete generator_;
  };

  void Next(std::string &value, int len);
  void Get(std::string &lvalue, std::string &rvalue, int len, double selectionrate = 0.01);
  void init(int numdistint, Generator<uint64_t> *generator = nullptr);

 private:
  int numdistint_;
  std::vector<std::array<char,6>> values_; 
  int off_;
  Generator<uint64_t> *generator_;
};

inline void DistinctValueGenerator::init(int numdistint, Generator<uint64_t> *generator) {
  numdistint_ = numdistint;
  values_.resize(numdistint_);
  for (int i = 0; i < numdistint_; ++i) {
    int bytes = utils::ThreadLocalRandomInt();
    values_[i][0] = static_cast<char>((bytes & 31) + ' ');
    values_[i][1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
    values_[i][2] = static_cast<char>(((bytes >> 10) & 95)+ ' ');
    values_[i][3] = static_cast<char>(((bytes >> 15) & 31)+ ' ');
    values_[i][4] = static_cast<char>(((bytes >> 20) & 63)+ ' ');
    values_[i][5] = static_cast<char>(((bytes >> 25) & 95)+ ' ');
    std::shuffle(values_[i].begin(), values_[i].end(), std::mt19937(std::random_device()()));
  }
  std::sort(values_.begin(), values_.end());
  generator_ = generator;
  if(generator_ == nullptr) {
    throw utils::Exception("DistinctValueGenerator needs a generator!");
  }
}

inline void DistinctValueGenerator::Next(std::string &value, int len) {
  int k = generator_->Next();
  std::generate_n(std::back_inserter(value), len, 
    [&]() {
      off_ = (off_ + 1) % 6;
      return values_[k][off_]; 
    });
  off_ = -1;
}

inline void DistinctValueGenerator::Get(std::string &lvalue, std::string &rvalue, int len, double selectionrate)
{
  double start = utils::ThreadLocalRandomDouble(0, 1.0 - selectionrate);
  double end = start + selectionrate;
  int start_index = std::round(start * numdistint_);
  int end_index = std::round(end * numdistint_);
  std::generate_n(std::back_inserter(lvalue), len, 
    [&]() {
      off_ = (off_ + 1) % 6;
      return values_[start_index][off_]; 
    });
  off_ = -1;
  std::generate_n(std::back_inserter(rvalue), len, 
    [&]() {
      off_ = (off_ + 1) % 6;
      return values_[end_index][off_]; 
    });
  off_ = -1;
}

} // ycsbc
#endif // YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_