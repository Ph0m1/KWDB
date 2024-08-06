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

#include "ee_completion_list.h"

#include <list>

#include "ee_global.h"
#include "gtest/gtest.h"

#define LIST_NUM    1000000

namespace kwdbts {

class TestList : public ::testing::Test {
 protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(TestList, ee_list_int) {
  ee_list_t<k_uint32> *list = ee_list_create<k_uint32>();
  EXPECT_TRUE(list != nullptr);

  for (k_uint32 i = 0; i < 10; ++i) {
    EXPECT_TRUE(nullptr != ee_list_add_last(list, i));
  }

  ee_list_node_t<k_uint32> *last = ee_list_get_last(list);
  EXPECT_TRUE(last != nullptr);

  k_uint32 j = 11;
  ee_list_node_t<k_uint32> *node = ee_list_add_before(list, j, last);
  EXPECT_EQ(node->data_, 11);

  ee_list_remove(list, node);
  SafeFreePointer(node);

  k_uint32 size = ee_list_get_size(list);
  EXPECT_EQ(size, 10);

  node = ee_list_get_first(list);
  for (k_uint32 i = 0; i < size; ++i, node = node->next_) {
    EXPECT_EQ(node->data_, i);
  }

  ee_list_free(list);
}

TEST_F(TestList, ee_list_char) {
  ee_list_t<char *> *list = ee_list_create<char *>();
  EXPECT_TRUE(list != nullptr);

  char **args = static_cast<char **>(malloc(10 * sizeof(char*)));
  for (k_uint32 i = 0; i < 10; ++i) {
    char *str = new char[20];
    memset(str, 0, 20);
    snprintf(str, 20, "%s%d", "hello world", i);
    EXPECT_TRUE(nullptr != ee_list_add_last(list, str));
    args[i] = str;
  }

  k_uint32 size = ee_list_get_size(list);
  EXPECT_EQ(size, 10);

  ee_list_node_t<char *> *node = ee_list_get_first(list);
  for (k_uint32 i = 0; i < size; ++i, node = node->next_) {
    EXPECT_STREQ(node->data_, args[i]);
  }

  ee_list_free(list);

  for (k_uint32 i = 0; i < 10; ++i) {
    SafeDeleteArray(args[i]);
  }

  SafeFreePointer(args);
}

// fast than std::list
TEST_F(TestList, ee_list_compare) {
  std::list<char *> stl_list;
  ee_list_t<char *> *list = ee_list_create<char *>();
  EXPECT_TRUE(list != nullptr);

  char **args = static_cast<char **>(malloc(LIST_NUM * sizeof(char*)));
  for (k_uint32 i = 0; i < LIST_NUM; ++i) {
    char *str = new char[20];
    memset(str, 0, 20);
    snprintf(str, 20, "%s%d", "hello world", i);
    EXPECT_TRUE(nullptr != ee_list_add_last(list, str));
    args[i] = str;
  }

  auto start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < LIST_NUM; ++i) {
    ee_list_add_last(list, args[i]);
  }
  auto end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff1 = end - start;

  start = std::chrono::steady_clock::now();
  for (k_uint32 i = 0; i < LIST_NUM; ++i) {
    stl_list.push_back(args[i]);
  }
  end = std::chrono::steady_clock::now();
  std::chrono::duration<double> diff2 = end - start;

  std::cout << "ee_list_t add " << LIST_NUM << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "std::list add " << LIST_NUM << " data take: " << diff2.count() << "s" << std::endl;

  start = std::chrono::steady_clock::now();
  ee_list_node_t<char *> *node = ee_list_get_first(list);
  for (k_uint32 i = 0; i < LIST_NUM; ++i, node = node->next_) {
    EXPECT_STREQ(node->data_, args[i]);
  }
  end = std::chrono::steady_clock::now();
  diff1 = end - start;

  start = std::chrono::steady_clock::now();
  int j = 0;
  for (auto iter = stl_list.begin(); iter != stl_list.end(); ++iter, ++j) {
    EXPECT_STREQ(*iter, args[j]);
  }
  end = std::chrono::steady_clock::now();
  diff2 = end - start;

  std::cout << "ee_list_t find all " << LIST_NUM << " data take: " << diff1.count() << "s" << std::endl;
  std::cout << "std::list find all " << LIST_NUM << " data take: " << diff2.count() << "s" << std::endl;

  ee_list_free(list);

  for (k_uint32 i = 0; i < LIST_NUM; ++i) {
    SafeDeleteArray(args[i]);
  }

  SafeFreePointer(args);
}

}  // namespace kwdbts
