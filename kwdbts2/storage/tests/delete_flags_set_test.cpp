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

#include <unistd.h>
#include <gtest/gtest.h>
#include <mmap/mmap_entity_idx.h>

class TestDeleteFlagSet : public ::testing::Test {};

TEST_F(TestDeleteFlagSet, oneRow) {
  char flag[8];
  memset(flag, 0, 8);
  setRowDeleted(flag, 1);
  EXPECT_EQ(flag[0], 1);
  setRowDeleted(flag, 1);
  EXPECT_EQ(flag[0], 1);
  setRowDeleted(flag, 2);
  EXPECT_EQ(flag[0], 3);
  setRowDeleted(flag, 8);
  EXPECT_EQ(flag[0], static_cast<char>(3 + (1 << 7)));
  setRowDeleted(flag, 9);
  EXPECT_EQ(flag[1], 1);
  EXPECT_EQ(flag[0], static_cast<char>(3 + (1 << 7)));
  setRowDeleted(flag, 57);
  EXPECT_EQ(flag[7], 1);
  EXPECT_TRUE(isAllDeleted(flag, 1, 1));
  EXPECT_TRUE(isAllDeleted(flag, 1, 2));
  EXPECT_TRUE(!isAllDeleted(flag, 1, 3));
  EXPECT_TRUE(!isAllDeleted(flag, 1, 8));
}

TEST_F(TestDeleteFlagSet, batchRow) {
  char flag[8];
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 1);
  EXPECT_EQ(flag[0], 1);
  EXPECT_TRUE(isAllDeleted(flag, 1, 1));
  setBatchDeleted(flag, 1, 1);
  EXPECT_EQ(flag[0], 1);
  EXPECT_TRUE(isAllDeleted(flag, 1, 1));
  setBatchDeleted(flag, 2, 1);
  EXPECT_EQ(flag[0], 3);
  EXPECT_TRUE(isAllDeleted(flag, 1, 2));
  setBatchDeleted(flag, 8, 1);
  EXPECT_EQ(flag[0], static_cast<char>(3 + (1 << 7)));
  setBatchDeleted(flag, 9, 1);
  EXPECT_EQ(flag[1], 1);
  EXPECT_EQ(flag[0], static_cast<char>(3 + (1 << 7)));
  EXPECT_TRUE(isAllDeleted(flag, 8, 2));
}

TEST_F(TestDeleteFlagSet, batchRows) {
  char flag[8];
  // in same byte
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 3);
  EXPECT_EQ(flag[0], 1 + 2 + 4);
  setBatchDeleted(flag, 1, 4);
  EXPECT_EQ(flag[0], 1 + 2 + 4 + 8);
  EXPECT_TRUE(isAllDeleted(flag, 1, 4));
  EXPECT_TRUE(!isAllDeleted(flag, 1, 5));

  // in continous bytes
  memset(flag, 0, 8);
  setBatchDeleted(flag, 8, 4);
  EXPECT_EQ(flag[0], static_cast<char>(1 << 7));
  EXPECT_EQ(flag[1], 1 + 2 + 4);
  EXPECT_TRUE(isAllDeleted(flag, 8, 4));
  EXPECT_TRUE(!isAllDeleted(flag, 7, 4));
  EXPECT_TRUE(!isAllDeleted(flag, 9, 4));
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 16);
  EXPECT_EQ(flag[0], static_cast<char>(0xFF));
  EXPECT_EQ(flag[1], static_cast<char>(0xFF));
  EXPECT_TRUE(isAllDeleted(flag, 1, 16));
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 14);
  EXPECT_EQ(flag[0], static_cast<char>(0xFF));
  EXPECT_EQ(flag[1], static_cast<char>(0x3F));
  memset(flag, 0, 8);
  setBatchDeleted(flag, 3, 12);
  EXPECT_EQ(flag[0], static_cast<char>(0xFC));
  EXPECT_EQ(flag[1], static_cast<char>(0x3F));
  EXPECT_TRUE(isAllDeleted(flag, 3, 12));
  EXPECT_TRUE(!isAllDeleted(flag, 4, 12));
  EXPECT_TRUE(!isAllDeleted(flag, 3, 13));

  // not in continous bytes
  memset(flag, 0, 8);
  setBatchDeleted(flag, 8, 4 + 16);
  EXPECT_EQ(flag[0], static_cast<char>(1 << 7));
  EXPECT_EQ(flag[1], static_cast<char>(0xFF));
  EXPECT_EQ(flag[2], static_cast<char>(0xFF));
  EXPECT_EQ(flag[3], 1 + 2 + 4);
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 16 + 16);
  EXPECT_EQ(flag[0], static_cast<char>(0xFF));
  EXPECT_EQ(flag[1], static_cast<char>(0xFF));
  EXPECT_EQ(flag[2], static_cast<char>(0xFF));
  EXPECT_EQ(flag[3], static_cast<char>(0xFF));
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 14 + 16);
  EXPECT_EQ(flag[0], static_cast<char>(0xFF));
  EXPECT_EQ(flag[1], static_cast<char>(0xFF));
  EXPECT_EQ(flag[2], static_cast<char>(0xFF));
  EXPECT_EQ(flag[3], static_cast<char>(0x3F));
  memset(flag, 0, 8);
  setBatchDeleted(flag, 3, 12 + 16);
  EXPECT_EQ(flag[0], static_cast<char>(0xFC));
  EXPECT_EQ(flag[1], static_cast<char>(0xFF));
  EXPECT_EQ(flag[2], static_cast<char>(0xFF));
  EXPECT_EQ(flag[3], static_cast<char>(0x3F));
  EXPECT_TRUE(isAllDeleted(flag, 3, 12 + 16));
  EXPECT_TRUE(!isAllDeleted(flag, 2, 12 + 16));
  EXPECT_TRUE(!isAllDeleted(flag, 3, 12 + 17));
}

TEST_F(TestDeleteFlagSet, TestDeleteFlagSet_batchRows2_Test) {
  char flag[8];
  // in continous two bytes
  memset(flag, 0, 8);
  setBatchDeleted(flag, 1, 5);
  setBatchDeleted(flag, 7, 5);
  EXPECT_TRUE(isAllDeleted(flag, 1, 5));
  EXPECT_TRUE(isAllDeleted(flag, 7, 5));
  EXPECT_TRUE(!isAllDeleted(flag, 1, 11));

  memset(flag, 0, 8);
  setBatchDeleted(flag, 9, 7);
  setBatchDeleted(flag, 17, 7);
  EXPECT_TRUE(isAllDeleted(flag, 9, 7));
  EXPECT_TRUE(!isAllDeleted(flag, 9, 8));
  EXPECT_TRUE(!isAllDeleted(flag, 9, 12));
}
