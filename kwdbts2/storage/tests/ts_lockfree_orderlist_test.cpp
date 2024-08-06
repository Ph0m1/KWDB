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

#include <gtest/gtest.h>
#include <thread>
#include "mmap/TSLockfreeOrderList.h"

class TestTsLockfreeOrderList : public ::testing::Test {
 public:
  TestTsLockfreeOrderList() {
    list_.Clear();
  }

  ~TestTsLockfreeOrderList() {
    list_.Clear();
  }

  TSLockfreeOrderList<int, int> list_;
};

TEST_F(TestTsLockfreeOrderList, empty) {
}

TEST_F(TestTsLockfreeOrderList, orderInsert) {
  int num = 100;
  for (size_t i = 0; i < 100; i++) {
    list_.Insert(i, i + 1);
  }
  std::vector<int> keys;
  std::vector<int > values;
  list_.GetAllKey(&keys);
  list_.GetAllValue(&values);
  EXPECT_EQ(keys.size(), num);
  EXPECT_EQ(values.size(), num);
  for (size_t i = 0; i < num; i++) {
    EXPECT_EQ(num - i - 1, keys[i]);
    EXPECT_EQ(num - i, values[i]);
  }
}

TEST_F(TestTsLockfreeOrderList, disorderInsert) {
  int num = 100;
  std::vector<int> keys_o;
  for (size_t i = 0; i < num; i++) {
    keys_o.push_back(i);
  }
  std::random_shuffle(keys_o.begin(), keys_o.end());
  for (size_t i = 0; i < keys_o.size(); i++) {
    list_.Insert(keys_o[i], keys_o[i] + 9);
  }
  
  std::vector<int> keys;
  std::vector<int > values;
  list_.GetAllKey(&keys);
  list_.GetAllValue(&values);
  EXPECT_EQ(keys.size(), num);
  EXPECT_EQ(values.size(), num);
  for (size_t i = 0; i < num; i++) {
    EXPECT_EQ(num - i - 1, keys[i]);
    EXPECT_EQ(num - i + 8, values[i]);
  }
  int key,value;
  bool ret = list_.Seek(5, key, value);
  EXPECT_TRUE(ret);
  EXPECT_EQ(key, 5);
  EXPECT_EQ(value, key + 9);
}

TEST_F(TestTsLockfreeOrderList, MultiInsert) {
  int num = 100;
  int thread_num = 2;
  std::vector<int> keys_o;
  for (size_t i = 0; i < num; i++) {
    keys_o.push_back(i);
  }
  std::random_shuffle(keys_o.begin(), keys_o.end());

  std::vector<std::thread> threads;
  for (size_t i = 0; i < thread_num; i++) {
    threads.push_back(std::thread([=](int thread_id) {
      for (size_t i = 0; i < keys_o.size(); i++) {
        list_.Insert(keys_o[i] * thread_num + thread_id, keys_o[i] * thread_num + thread_id + 9);
      }
    }, i));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }

  std::vector<int> keys;
  std::vector<int > values;
  list_.GetAllKey(&keys);
  list_.GetAllValue(&values);
  EXPECT_EQ(keys.size(), num * thread_num);
  EXPECT_EQ(values.size(), num * thread_num);
  for (size_t i = 0; i < num * thread_num; i++) {
    EXPECT_EQ(num * thread_num - i - 1, keys[i]);
    EXPECT_EQ(keys[i] + 9, values[i]);
  }
  int key,value;
  bool ret = list_.Seek(5, key, value);
  EXPECT_TRUE(ret);
  EXPECT_EQ(key, 5);
  EXPECT_EQ(value, key + 9);
}
