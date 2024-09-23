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
#if 0
#include <gtest/gtest.h>
#include <random>
#include "test_util.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "utils/big_table_utils.h"
#include "test_tag_util.h"
#include "payload_generator.h"

const string TestBigTableInstance::kw_home_ = "./data_kw/";  // NOLINT
const string TestBigTableInstance::db_name_ = "testdb/";  // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestTagAlterTable : public TestBigTableInstance {
 public:
  TestTagAlterTable() : db_name_(TestBigTableInstance::db_name_), db_path_(TestBigTableInstance::kw_home_) { }

  std::string db_name_;
  std::string db_path_;

 protected:
  static void setUpTestCase() {}

  static void tearDownTestCase() {}
};

TEST_F(TestTagAlterTable, add_drop_tag_column) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {5, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {6, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {7, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG},
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 12, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  size_t cnt = 10;
  PayloadGenerator *pg = new PayloadGenerator(schema);
  char *ptag;
  for (size_t i = 0; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i,
                                 i * 10,
                                 std::to_string(i * 10),
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10);
    // insert
    // 2,4 col is primaryTag, convert it to hashpoint
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  int64_t c1 = 0, c2 = 0;
  char c3[32] = {}, c4[32] = {}, c5[32] = {};
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 0, &c1);
    bt->getColumnValue(i, 1, &c2);
    bt->getColumnValue(i, 2, c3);
    bt->getColumnValue(i, 3, c4);
    bt->getColumnValue(i, 4, c5);
    ASSERT_EQ(c1, i - 1);
    ASSERT_EQ(c2, (i - 1) * 10);
    ASSERT_EQ(c3, std::to_string((i - 1) * 10));
    ASSERT_EQ(c4, std::to_string(i - 1));
    ASSERT_EQ(c5, "Hello_" + std::to_string(i - 1));
  }
  // add tag
  TagInfo newtag = {8, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG};
  ASSERT_EQ(bt->AddTagColumn(newtag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  size_t cnt1 = cnt;
  cnt += 10;
  schema.push_back(newtag);
  pg = new PayloadGenerator(schema);
  for (size_t i = cnt1; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i,
                                 i * 10,
                                 std::to_string(i * 10),
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10,
                                 i * 10);
    // insert
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  int32_t c8 = 0;
  for (size_t i = 1; i <= cnt; ++i) {
    if (!bt->isNull(i, 7)) {
      bt->getColumnValue(i, 7, &c8);
      ASSERT_EQ(c8, (i - 1) * 10);
    }
  }

  // add tag
  newtag = {9, DATATYPE::VARSTRING, 20, 0, 20, GENERAL_TAG};
  ASSERT_EQ(bt->AddTagColumn(newtag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  cnt1 = cnt;
  cnt += 10;
  schema.push_back(newtag);
  pg = new PayloadGenerator(schema);
  for (size_t i = cnt1; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i,
                                 i * 10,
                                 std::to_string(i * 10),
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10,
                                 i * 10,
                                 std::to_string(i * 10));
    // insert
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  char c9[32] = {0x00};
  for (size_t i = 1; i <= cnt; ++i) {
    if (!bt->isNull(i, 8)) {
      bt->getColumnValue(i, 8, c9);
      ASSERT_EQ(c9, std::to_string((i - 1) * 10));
    }
  }

  // drop tag
  newtag = {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};
  ASSERT_EQ(bt->DropTagColumn(newtag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  cnt1 = cnt;
  cnt += 10;
  schema.erase(schema.begin() + 2);
  pg = new PayloadGenerator(schema);
  for (size_t i = cnt1; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i,
                                 i * 10,
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10,
                                 i * 10,
                                 std::to_string(i * 10));
    // insert
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  memset(c9, 0x00, 32);
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 0, &c1);
    bt->getColumnValue(i, 1, &c2);
    bt->getColumnValue(i, 2, c4);
    bt->getColumnValue(i, 3, c5);
    ASSERT_EQ(c1, i - 1);
    ASSERT_EQ(c2, (i - 1) * 10);
    ASSERT_EQ(c4, std::to_string(i - 1));
    ASSERT_EQ(c5, "Hello_" + std::to_string(i - 1));
    if (!bt->isNull(i, 6)) {
      bt->getColumnValue(i, 6, &c8);
      ASSERT_EQ(c8, (i - 1) * 10);
    }
    if (!bt->isNull(i, 7)) {
      bt->getColumnValue(i, 7, c9);
      ASSERT_EQ(c9, std::to_string((i - 1) * 10));
    }
  }
  // drop tag
  newtag = {8, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG};
  ASSERT_EQ(bt->DropTagColumn(newtag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  cnt1 = cnt;
  cnt += 10;
  schema.erase(schema.begin() + 6);
  pg = new PayloadGenerator(schema);
  for (size_t i = cnt1; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i,
                                 i * 10,
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10,
                                 std::to_string(i * 10));
    // insert
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  memset(c9, 0x00, 32);
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 0, &c1);
    bt->getColumnValue(i, 1, &c2);
    bt->getColumnValue(i, 2, c4);
    bt->getColumnValue(i, 3, c5);
    ASSERT_EQ(c1, i - 1);
    ASSERT_EQ(c2, (i - 1) * 10);
    ASSERT_EQ(c4, std::to_string(i - 1));
    ASSERT_EQ(c5, "Hello_" + std::to_string(i - 1));
    if (!bt->isNull(i, 6)) {
      bt->getColumnValue(i, 6, c9);
      ASSERT_EQ(c9, std::to_string((i - 1) * 10));
    }
  }
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
}


TEST_F(TestTagAlterTable, alter_tag_type) {
  ErrorInfo err_info;
  vector<TagInfo> schema = {
      {1, DATATYPE::DOUBLE, 8, 0, 8, GENERAL_TAG},
      {2, DATATYPE::INT64, 8, 0, 8, PRIMARY_TAG},
      {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {4, DATATYPE::VARSTRING, 32, 0, 32, PRIMARY_TAG},
      {5, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG},
      {6, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG},
      {7, DATATYPE::INT32, 4, 0, 4, GENERAL_TAG},
      {8, DATATYPE::INT16, 2, 0, 2, GENERAL_TAG},
  };
  MMapTagColumnTable* bt = CreateTagTable(schema, db_path_, db_name_, 12, 1, TAG_TABLE, err_info);
  EXPECT_NE(bt, nullptr);
  EXPECT_EQ(err_info.errcode, 0);

  size_t cnt = 10;
  PayloadGenerator *pg = new PayloadGenerator(schema);
  for (size_t i = 0; i < cnt; ++i) {
    auto payload = pg->Construct(nullptr,
                                 i * 1.0,
                                 i * 10,
                                 std::to_string(i * 10),
                                 std::to_string(i),
                                 "Hello_" + std::to_string(i),
                                 std::to_string(i * 10),
                                 i * 10,
                                 i * 10);
    // insert
    bt->insert(i, i % 500, pg->GetHashPoint(), reinterpret_cast<const char *>(payload));
    PayloadGenerator::Destroy(payload);
  }
  delete pg;
  // check
  ASSERT_EQ(bt->size(), cnt);
  double c1 = 0;
  int64_t c2 = 0;
  char c3[32] = {}, c4[32] = {}, c5[32] = {};
  int32_t c7 = 0;
  int16_t c8 = 0;
  char c6[32] = {0x00};
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 0, &c1);
    bt->getColumnValue(i, 1, &c2);
    bt->getColumnValue(i, 2, c3);
    bt->getColumnValue(i, 3, c4);
    bt->getColumnValue(i, 4, c5);
    bt->getColumnValue(i, 5, c6);
    bt->getColumnValue(i, 6, &c7);
    bt->getColumnValue(i, 7, &c8);
    ASSERT_DOUBLE_EQ(c1, (i - 1) * 1.0);
    ASSERT_EQ(c2, (i - 1) * 10);
    ASSERT_EQ(c3, std::to_string((i - 1) * 10));
    ASSERT_EQ(c4, std::to_string(i - 1));
    ASSERT_EQ(c5, "Hello_" + std::to_string(i - 1));
    ASSERT_EQ(c6, std::to_string((i - 1) * 10));
    ASSERT_EQ(c7, (i - 1) * 10);
    ASSERT_EQ(c8, (i - 1) * 10);
  }
  // change type
  // int16 -> int64
  TagInfo old_tag = {8, DATATYPE::INT16, 2, 0, 2, GENERAL_TAG};
  TagInfo new_tag = {8, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  ASSERT_EQ(bt->AlterTagType(old_tag, new_tag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  // check
  int64_t c8_1 = 0;
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 7, &c8_1);
    ASSERT_EQ(c8_1, (i - 1) * 10);
  }

  // int64 -> varchar(32)
  old_tag = {8, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  new_tag = {8, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};
  ASSERT_EQ(bt->AlterTagType(old_tag, new_tag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  // check
  char c8_2[32] = {0x00};
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 7, c8_2);
    ASSERT_EQ(c8_2, std::to_string((i - 1) * 10));
  }

  // char(20) -> varchar(32)
  old_tag = {6, DATATYPE::CHAR, 20, 0, 20, GENERAL_TAG};
  new_tag = {6, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};
  ASSERT_EQ(bt->AlterTagType(old_tag, new_tag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  // check
  char c6_2[32] = {0x00};
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 5, c6_2);
    ASSERT_EQ(c6_2, std::to_string((i - 1) * 10));
  }

  // varchar(32) -> int64
  old_tag = {3, DATATYPE::VARSTRING, 32, 0, 8, GENERAL_TAG};
  new_tag = {3, DATATYPE::INT64, 8, 0, 8, GENERAL_TAG};
  ASSERT_EQ(bt->AlterTagType(old_tag, new_tag, err_info), 0);
  bt->sync_with_lsn(100);
  CleanTagFiles(db_path_ + db_name_, 12, 101);

  // check
  int64_t c3_1 = 0;
  for (size_t i = 1; i <= cnt; ++i) {
    bt->getColumnValue(i, 2, &c3_1);
    ASSERT_EQ(c3_1, (i - 1) * 10);
  }
  // drop table
  EXPECT_EQ(bt->remove(), 0);
  delete bt;
  bt = nullptr;
}
#endif