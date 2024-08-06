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

#include "gtest/gtest.h"
#include "ee_global.h"

namespace kwdbts {
class SafeMacrosTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};
// test SafeDeletePointer macro
TEST(SafeMacrosTest, SafeDeletePointer) {
  int* ptr = new int(42);
  SafeDeletePointer(ptr);
  EXPECT_EQ(ptr, nullptr);  // Check if the pointer is set to nullptr
}

// test SafeFreePointer macro
TEST(SafeMacrosTest, SafeFreePointer) {
  int* ptr = static_cast<int*>(malloc(sizeof(int)));
  *ptr = 42;
  SafeFreePointer(ptr);
  EXPECT_EQ(ptr, nullptr);  // Check if the pointer is set to nullptr
}

}  // namespace kwdbts

