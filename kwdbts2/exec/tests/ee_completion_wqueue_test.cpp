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

#include "ee_completion_wqueue.h"

#include <queue>

#include "ee_global.h"
#include "gtest/gtest.h"

#define QUEUE_NUM   1000000

namespace kwdbts {

class TestQueue : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(TestQueue, ee_queue_int) {
  ee_wqueue_t<k_uint32> *wqueue = ee_wqueue_create<k_uint32>();
  EXPECT_TRUE(wqueue != nullptr);

  for (k_uint32 i = 0; i < 10; ++i) {
    ee_wqueue_add(wqueue, i);
  }

  k_uint32 count = ee_wqueue_get_count(wqueue);
  EXPECT_EQ(count, 10);

  for (k_uint32 i = 0; i < 10; ++i) {
    EXPECT_EQ(ee_wqueue_pop_front(wqueue), i);
  }

  EXPECT_TRUE(ee_wqueue_is_empty(wqueue));

  ee_wqueue_free(wqueue);
}

TEST_F(TestQueue, ee_queue_char) {
  ee_wqueue_t<char *> *wqueue = ee_wqueue_create<char *>();
  EXPECT_TRUE(wqueue != nullptr);

  char **args = static_cast<char **>(malloc(10 * sizeof(char*)));
  for (k_uint32 i = 0; i < 10; ++i) {
    char *str = new char[20];
    memset(str, 0, 20);
    snprintf(str, 20, "%s%d", "hello world", i);
    ee_wqueue_add(wqueue, str);
    args[i] = str;
  }

  for (k_uint32 i = 0; i < 10; ++i) {
    EXPECT_STREQ(ee_wqueue_pop_front(wqueue), args[i]);
  }

  ee_wqueue_free(wqueue);

   for (k_uint32 i = 0; i < 10; ++i) {
    SafeDeleteArray(args[i]);
  }

  SafeFreePointer(args);
}

// Performance deviation, higher performance than internal queues using
// std::list
TEST_F(TestQueue, ee_queue_compare) {
  ee_wqueue_t<char *> *wqueue = ee_wqueue_create<char *>();
  EXPECT_TRUE(wqueue != nullptr);

  // std::queue<char *, std::list<char*>> stl_queue;
  std::queue<char *> stl_queue;
  char **args = static_cast<char **>(malloc(QUEUE_NUM * sizeof(char*)));
  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    char *str = new char[20];
    memset(str, 0, 20);
    snprintf(str, 20, "%s%d", "hello world", i);
    args[i] = str;
  }

  auto start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    ee_wqueue_add(wqueue, args[i]);
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff1 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    stl_queue.push(args[i]);
  }
  end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff2 = end - start;

  std::cout << "ee_wqueue_t add " << QUEUE_NUM << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "std::queue add " << QUEUE_NUM << " data take: " << diff2.count() << "s" << std::endl;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    ee_wqueue_pop_front(wqueue);
  }
  end = std::chrono::steady_clock::now();
  diff1 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    stl_queue.pop();
  }
  end = std::chrono::steady_clock::now();
  diff2 = end - start;

  std::cout << "ee_wqueue_t find all " << QUEUE_NUM << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "std::queue find all " << QUEUE_NUM << " data take: " << diff2.count() << "s" << std::endl;

  ee_wqueue_free(wqueue);

  for (k_uint32 i = 0; i < QUEUE_NUM; ++i) {
    SafeDeleteArray(args[i]);
  }

  SafeFreePointer(args);
}

}  // namespace kwdbts
