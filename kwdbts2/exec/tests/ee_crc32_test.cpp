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

#include "ee_crc32.h"
#include "ee_string.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestCRC32 : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestCRC32, TestCRC32MathFunc) {
  const char* crc32msg = "abc123";
  size_t len = 6;
  kwdbts::k_int64 icode32 = kwdbts::kwdb_crc32_castagnoli(crc32msg, len);
  EXPECT_EQ(icode32, 26154185);
  icode32 = kwdbts::kwdb_crc32_ieee(crc32msg, len);
  EXPECT_EQ(icode32, 3473062748);
}

}  // namespace kwdbts
