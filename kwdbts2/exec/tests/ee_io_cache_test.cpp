// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ee_io_cache.h"

#include "gtest/gtest.h"

namespace kwdbts {

#define USER_BUFFER_SIZE  10000
#define DATA_ROW_COUNT    20

IO_CACHE chunk_file;

class TestIOCACHE : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(TestIOCACHE, test_write) {
  // Determine whether the IO_CACHE is initialized
  EXPECT_FALSE(my_b_inited(&chunk_file));
  // open file by write
  EXPECT_FALSE(open_cached_file(&chunk_file, "./", "test_", "io_cache", 8192, 0, cache_type::CACHE_WRITE));
  // write data
  k_char buffer[USER_BUFFER_SIZE];
  const char str_content[]="z0x1c5vb6n8ma9s2dfg3hjklqw4erty7uiop";
  srand((unsigned)time(NULL));

  for (int i = 0; i < DATA_ROW_COUNT; ++i) {
    memset(buffer, 0, USER_BUFFER_SIZE);
    k_uint32 _seed = 0;
    // k_uint32 size = rand_r(&_seed) % 5000 + 5000;
    for (k_uint32 j = 0; j < USER_BUFFER_SIZE; ++j) {
      k_int32 pos = rand_r(&_seed) % 36;
      buffer[j] = str_content[pos];
    }

    EXPECT_FALSE(my_b_write(&chunk_file, buffer, USER_BUFFER_SIZE));
  }

  EXPECT_FALSE(kwdbts_flush_io_cache(&chunk_file, 1));
}

TEST_F(TestIOCACHE, test_read) {
  // Determine whether the IO_CACHE is initialized
  EXPECT_TRUE(my_b_inited(&chunk_file));
  // init IO_CACHE by read 
  EXPECT_FALSE(reinit_io_cache(&chunk_file, cache_type::CACHE_READ, 0));
  // read data
  k_char buffer[USER_BUFFER_SIZE];
  k_uint64 total_read_length = 0;
  int ret = 0;
  do {
     memset(buffer, 0, USER_BUFFER_SIZE);
    ret = my_b_read(&chunk_file, buffer, USER_BUFFER_SIZE);
    EXPECT_NE(ret, -1);
    if (0 == ret) {
      total_read_length += USER_BUFFER_SIZE;
    } else if (1 == ret) {
      total_read_length += chunk_file.error_;
    } else {
      total_read_length = 0;
    }
  } while ( 0 == ret);

  EXPECT_EQ(total_read_length, USER_BUFFER_SIZE * DATA_ROW_COUNT);
}

TEST_F(TestIOCACHE, test_read_append) {
  // Determine whether the IO_CACHE is initialized
  EXPECT_TRUE(my_b_inited(&chunk_file));
  // Free IO_CACHE
  EXPECT_FALSE(end_io_cache(&chunk_file));
  system("rm -rf ./test_io_cache");
  // Open the file in a CACHE_SEQ_READ_APPEND way
  EXPECT_FALSE(open_cached_file(&chunk_file, "./", "test_", "io_cache", 8192, 0, cache_type::CACHE_SEQ_READ_APPEND));
  // write data
  k_char buffer[USER_BUFFER_SIZE];
  const char str_content[]="z0x1c5vb6n8ma9s2dfg3hjklqw4erty7uiop";
  srand((unsigned)time(NULL));
  k_uint64 total_read_length = 0;
  for (int i = 0; i < DATA_ROW_COUNT; ++i) {
    memset(buffer, 0, USER_BUFFER_SIZE);
    k_uint32 _seed = 0;
    // k_uint32 size = rand_r(&_seed) % 5000 + 5000;
    for (k_uint32 j = 0; j < USER_BUFFER_SIZE; ++j) {
      k_int32 pos = rand_r(&_seed) % 36;
      buffer[j] = str_content[pos];
    }
    // append file
    EXPECT_FALSE(my_b_append(&chunk_file, buffer, USER_BUFFER_SIZE));
    // read file
    memset(buffer, 0, USER_BUFFER_SIZE);
    EXPECT_FALSE(my_b_read(&chunk_file, buffer, USER_BUFFER_SIZE));
    total_read_length += USER_BUFFER_SIZE;
  }

  memset(buffer, 0, USER_BUFFER_SIZE);
  EXPECT_EQ(my_b_read(&chunk_file, buffer, USER_BUFFER_SIZE), 1);
  EXPECT_EQ(chunk_file.error_, 0);
  total_read_length += chunk_file.error_;

  EXPECT_EQ(total_read_length, USER_BUFFER_SIZE * DATA_ROW_COUNT);
  // Free IO_CACHE
  EXPECT_FALSE(end_io_cache(&chunk_file));
  system("rm -rf ./test_io_cache");
}

}  // namespace kwdbts
