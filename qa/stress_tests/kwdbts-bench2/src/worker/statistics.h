// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
/*
 * @author jiadx
 * @date 2022/5/21
 * @version 1.0
 */

#pragma once

#include <thread>
#include "random"
#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

inline long kget_time_now() {
  struct timespec timestamp;
  clock_gettime(CLOCK_REALTIME, &timestamp);
  return timestamp.tv_sec * 1e9 + timestamp.tv_nsec;
}
/*
 *  Starting statistics is also time-consuming.
 *  The iops of a single zobject for a single thread drops from 2500w/s to 700w/s after statistics is enabled.
 */
#define KWDB_STATS_ON
#define KWDB_TIME (kget_time_now())
//#define KWDB_DURATION(start, end) ( (end) - (start) )
//#define KWDB_DURATION_MS(start, end) (KWDB_DURATION(start, end)*1.0 / 1e6)
#ifdef KWDB_STATS_ON
#define KWDB_START() auto __kwdb_start__ = KWDB_TIME;
#define KWDB_DURATION(x) x.add(KWDB_TIME - __kwdb_start__);
#else
#define KWDB_START()
#define KWDB_DURATION(x)
#endif

namespace kwdbts {
namespace random {
// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random {
 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };
  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  // This is the largest value that can be returned from Next()
  enum : uint32_t {
    kMaxNext = M
  };

  explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

  void Reset(uint32_t s) { seed_ = GoodSeed(s); }

  uint32_t Next() {
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  // Returns a Random instance for use by the current thread without
  // additional locking
  static Random* GetTLSInstance() {
    static __thread Random* tls_instance;
    static __thread std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;
    auto rv = tls_instance;
    if (UNLIKELY(rv == nullptr)) {
      size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
      rv = new(&tls_instance_bytes) Random((uint32_t) seed);
      tls_instance = rv;
    }
    return rv;
  }
};
}/* namespace random */

const int AVG_STAT_NUM = 100;
struct AvgStat {
 private:
  uint64_t sum_[AVG_STAT_NUM];
  uint64_t size_[AVG_STAT_NUM];
  const double scale = 1.0;
  const std::string str;

 public:
  AvgStat() {
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  explicit AvgStat(const std::string &s) : str(s) {
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  ~AvgStat() {}

  void add(double value) {
    // int i = rand() % NUM;
    int i = random::Random::GetTLSInstance()->Next() % AVG_STAT_NUM;
    __sync_fetch_and_add(&(sum_[i]), (uint64_t) (value * scale));
    __sync_fetch_and_add(&(size_[i]), 1);
  }

  double avg() {
    double total_sum = 0.0;
    uint64_t total_size = 0;
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      total_sum += sum_[i] * 1.0 / scale;
      total_size += size_[i];
    }
    if (total_size == 0) return 0;
    return total_sum / total_size;
  }

  double sum() {
    double total_sum = 0;
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      total_sum += sum_[i] * 1.0 / scale;
    }
    return total_sum;
  }

  double max() {
    double max_value = 0;
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      if (max_value < sum_[i]) {
        max_value = sum_[i];
      }
    }
    return max_value;
  }

  uint64_t size() {
    uint64_t total_size = 0;
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      total_size += size_[i];
    }
    return total_size;
  }

  void reset() {
    for (int i = 0; i < AVG_STAT_NUM; i++) {
      sum_[i] = 0;
      size_[i] = 0;
    }
  }

  void print(std::string s) {
    printf("%s: %lf / %ld = %lf\n",
           s.c_str(), sum(), size(), avg());
  }

  void print() {
    printf("%s: %lf / %ld = %lf\n",
           str.c_str(), sum(), size(), avg());
  }
};

struct Statistics {
  // append Statistics
  AvgStat db_append_t;
  AvgStat table_append_t;

  // Number and time of data blocks written by the flush thread per loop
  AvgStat flush_time;
  AvgStat flush_blocks;

  // Size and time required to write data to a partition file
  AvgStat file_write_time;
  AvgStat file_write_size;
  AvgStat key_write_size;

  // The number and time of data blocks read each time according to [from,to]
  AvgStat block_find_num;
  AvgStat block_find_time;

  double WriteGB() {
    return file_write_size.sum() / KB / KB / KB;
  }

  double IoMB() {
    double sum_size = file_write_size.sum() / KB / KB; // MB
    double sum_time = file_write_time.sum() / Second;// second
    return sum_size / sum_time;
  }

  void Show() {
//      double sum_size = file_write_size.sum() / KB / KB; // MB
//      double sum_time = file_write_time.sum() / Second;// second
//      double sum_key_size = key_write_size.sum() / KB / KB; // MB
    fprintf(stdout, "*******Statistics Print******\n"
                    " DB Append =%.2f ns, table append=%.2f ns\n"
        //                   " flush_blocks avg=%.1f, flush_time avg =%.3f ms\n"
//                      " file_write_time avg=%.3f ms, file_write_size avg=%.0f KB, \n"
//                      " total_write_size=%.f GB, total_key_size=%.f MB, IO=%.2f MB/s\n"
//                     " block_find_time=%.3f ms, block_find_num=%.0f\n"
        ,
            db_append_t.avg(), table_append_t.avg()
//              ,flush_blocks.avg(), flush_time.avg() / MS,
//              file_write_time.avg() / MS, file_write_size.avg() / KB,
//              sum_size / 1024, sum_key_size , (sum_size + sum_key_size) / sum_time,
//              block_find_time.avg() / MS, block_find_num.avg()
    );
    fflush(stdout);
  }

  void Reset() {
    db_append_t.reset();
    table_append_t.reset();
    flush_time.reset();
    flush_blocks.reset();
    file_write_time.reset();
    file_write_time.reset();
    block_find_num.reset();
    block_find_time.reset();
  }

 private:
  static const long KB = 1024;
  static const long MS = 1e6;
  static const long Second = 1e9;
};
}