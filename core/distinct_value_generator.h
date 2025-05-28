#ifndef YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_
#define YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_

#include "utils/utils.h"
#include <array>
#include <algorithm>
#include <memory>
#include <random>
#include <cstdio>
#include "zipfian_generator.h"

namespace ycsbc {

class DistinctValueGenerator {
 public:
  DistinctValueGenerator() {}
  ~DistinctValueGenerator() {
    delete generator_;
  };

  void Next(std::string &value);
  void Get(std::string &lvalue, std::string &rvalue);
  void init(int numdistint, Generator<uint64_t> * len_generator, double selectionrate = 0.01, 
            Generator<uint64_t> *generator = nullptr, bool is_zipfian = false);

 private:
  int numdistint_;
  bool zipfian_ = false;
  int zipfian_tail_;
  double selectionrate_;
  std::vector<std::string> values_; 
//  int off_;
  Generator<uint64_t> *generator_;

};

inline void DistinctValueGenerator::init(int numdistint, Generator<uint64_t> * len_generator, double selectionrate, 
                                         Generator<uint64_t> *generator, bool is_zipfian){
  numdistint_ = numdistint;
  values_.resize(numdistint_);
  char buf_[6];
  int off_ = 6;
  for (int i = 0; i < numdistint_; ++i) {
    int len = len_generator->Next();
    std::generate_n(std::back_inserter(values_[i]), len, 
      [&]() {
        if (off_ == 6) {
          int bytes = utils::ThreadLocalRandomInt();
          buf_[0] = static_cast<char>((bytes & 31) + ' ');
          buf_[1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
          buf_[2] = static_cast<char>(((bytes >> 10) & 95)+ ' ');
          buf_[3] = static_cast<char>(((bytes >> 15) & 31)+ ' ');
          buf_[4] = static_cast<char>(((bytes >> 20) & 63)+ ' ');
          buf_[5] = static_cast<char>(((bytes >> 25) & 95)+ ' ');
          off_ = 0;
        }
        return buf_[off_++];
      });
    //std::shuffle(values_[i].begin(), values_[i].end(), std::mt19937(std::random_device()()));
  }
  std::sort(values_.begin(), values_.end());
  generator_ = generator;
  selectionrate_ = selectionrate;
  if(selectionrate_ <= 0 || selectionrate_ > 1.0) {
    throw utils::Exception("Selection rate must be in (0, 1.0]!");
  }
  if(generator_ == nullptr) {
    throw utils::Exception("DistinctValueGenerator needs a generator!");
  }
  if(is_zipfian) {
    zipfian_ = true;
    auto x = static_cast<ZipfianGenerator *>(generator_);
    double zeta_N = 0;
    for (int i = 1; i <= numdistint_; ++i) {
      zeta_N += 1 / std::pow(i, x->theta_);
    }
    double cumulative = 0.0;
    for (int k = numdistint_; k > 0; --k) {
        double p_k = (1.0 / std::pow(k, x->theta_)) / zeta_N;
        cumulative += p_k;
        if (cumulative >= selectionrate) {
            zipfian_tail_ = k - 1;
            break;
        }
    }
  }
}

inline void DistinctValueGenerator::Next(std::string &value) {
  int k = generator_->Next();
  value = values_[k];
}

inline void DistinctValueGenerator::Get(std::string &lvalue, std::string &rvalue)
{
  if(zipfian_) {
    lvalue = values_[zipfian_tail_];
    rvalue = values_[numdistint_ - 1];
    return;
  }
  double start = std::min(utils::ThreadLocalRandomDouble(0, 1.0 - selectionrate_), 1.0 - selectionrate_);
  double end = start + selectionrate_;
  int start_index = std::max((int)std::round(start * numdistint_), 0);
  int end_index = std::min((int)std::round(end * numdistint_), numdistint_ - 1);
  lvalue = values_[start_index];
  rvalue = values_[end_index];
}

} // ycsbc
#endif // YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_