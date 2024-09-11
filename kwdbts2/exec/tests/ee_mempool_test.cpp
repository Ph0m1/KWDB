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

#include "ee_mempool.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestMempool : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

TEST_F(TestMempool, TestCreateMempool) {
  kwdbts::EE_PoolInfoDataPtr pstPoolTestInfo = kwdbts::EE_MemPoolInit(1, 16);
  EXPECT_EQ((pstPoolTestInfo != nullptr), true);
  kwdbts::k_char* pcTestMsg = kwdbts::EE_MemPoolMalloc(pstPoolTestInfo, ROW_BUFFER_SIZE);
  EXPECT_EQ((pcTestMsg != nullptr), true);
  kwdbts::KStatus status = kwdbts::EE_MemPoolFree(pstPoolTestInfo, pcTestMsg);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  status = kwdbts::EE_MemPoolCleanUp(pstPoolTestInfo);
  EXPECT_EQ(status, kwdbts::SUCCESS);
  pstPoolTestInfo = nullptr;
}

}  // namespace kwdbts
