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
#include <random>
#include "test_util.h"
#include "mmap/mmap_tag_table.h"
#include "utils/big_table_utils.h"
#include "test_tag_util.h"
#include "payload_generator.h"

const string TestBigTableInstance::kw_home_ = "./data_kw/";  // NOLINT
const string TestBigTableInstance::db_name_ = "testdb/";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTagTable : public TestBigTableInstance {
 public:
  TestTagTable() : db_name_(TestBigTableInstance::db_name_), db_path_(TestBigTableInstance::kw_home_) { }

  std::string db_name_;
  std::string db_path_;

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}
};

TEST_F(TestTagTable, TEST_CREATE_DROP) {
  vector<TagInfo> schema = {
      {0, DATATYPE::TIMESTAMP64, 8, 0, 8, PRIMARY_TAG},
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::DOUBLE, 8, 0, 8, GENERAL_TAG}
  };
  ErrorInfo err_info;
  TagTable *bt = CreateTagTable(schema, db_path_, db_name_, 5, 1, 1, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);
  // EXPECT_EQ(bt->remove(err_info), 0);
  delete bt;

  bt = OpenTagTable(db_path_, db_name_, 5, 1, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);
  EXPECT_EQ(bt->remove(err_info), 0);
  delete bt;
  bt = nullptr;
}