#ifndef YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_
#define YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_

#include "generator.h"
#include "utils/utils.h"
#include <memory>
#include <random>
#include <cstdio>

namespace ycsbc {

class RandomConstByteGenerator : public Generator<char> {
 public:
  RandomConstByteGenerator() : off_(0),count_(0) {}

  char Next();
  char Last();
  void reset();
  virtual void init(int numdistint);
  int count_;

 private:
  int numdistint_;
  
  std::unique_ptr<char[]> buf_; 
  int off_;
};

inline void RandomConstByteGenerator::init(int numdistint) {
  numdistint_ = numdistint;
  buf_ = std::make_unique<char[]>(numdistint_ * 6);
  for (int i = 0; i < numdistint_; ++i) {
    int bytes = utils::ThreadLocalRandomInt();
    buf_[i * 6 + 0] = static_cast<char>((bytes & 31) + ' ');
    buf_[i * 6 + 1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
    buf_[i * 6 + 2] = static_cast<char>(((bytes >> 10) & 95)+ ' ');
    buf_[i * 6 + 3] = static_cast<char>(((bytes >> 15) & 31)+ ' ');
    buf_[i * 6 + 4] = static_cast<char>(((bytes >> 20) & 63)+ ' ');
    buf_[i * 6 + 5] = static_cast<char>(((bytes >> 25) & 95)+ ' ');
  }
}

inline void RandomConstByteGenerator::reset() {
  off_=0;
  count_ = (count_ + 1) % numdistint_;
}

inline char RandomConstByteGenerator::Next() {
  char next_char = buf_[count_ * 6 + off_];
  off_ = (off_+1)%6;
  return next_char;
}

inline char RandomConstByteGenerator::Last() {
  char last_char = buf_[count_ * 6 + ((off_ - 1 + 6) % 6)];
  off_=0;
  return last_char;
}

} // ycsbc
#endif // YCSB_C_RANDOM_CONST_BYTE_GENERATOR_H_