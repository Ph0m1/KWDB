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
#include "ee_table.h"

// #include "ee_metadata_data_test.h"
#include "gtest/gtest.h"
namespace kwdbts {
  const int datatype[13][2] = {
      {roachpb::DataType::TIMESTAMP, KWDBTypeFamily::TimestampFamily},
      {roachpb::DataType::SMALLINT, KWDBTypeFamily::IntFamily},
      {roachpb::DataType::INT, KWDBTypeFamily::IntFamily},
      {roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
      {roachpb::DataType::BOOL, KWDBTypeFamily::IntFamily},
      {roachpb::DataType::FLOAT, KWDBTypeFamily::FloatFamily},
      {roachpb::DataType::DOUBLE, KWDBTypeFamily::FloatFamily},
      {roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
      {roachpb::DataType::BINARY, KWDBTypeFamily::StringFamily},
      {roachpb::DataType::NCHAR, KWDBTypeFamily::StringFamily},
      {roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily},
      {roachpb::DataType::NVARCHAR, KWDBTypeFamily::TimestampFamily},
      {roachpb::DataType::VARBINARY, KWDBTypeFamily::TimestampFamily}};
class TestTable : public testing::Test {
 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
};

void CreateTagReaderSpec(TSTagReaderSpec **spec) {
  *spec = new TSTagReaderSpec();
  (*spec)->set_tableversion(1);
  for (k_int32 i = 0; i < sizeof(datatype)/sizeof(int)/2 ; ++i) {
    TSCol*col = (*spec)->add_colmetas();
    col->set_storage_type((roachpb::DataType)datatype[i][0]);
  }
}

TEST_F(TestTable, TestTableInit) {
  kwdbts::kwdbContext_t g_pool_context;
  kwdbts::kwdbContext_p thread_pool_ctx_ = &g_pool_context;
  InitServerKWDBContext(thread_pool_ctx_);

  TSTagReaderSpec *post_{nullptr};
  KDatabaseId schemaID = 1;
  KTableId tableID = 1;
  TABLE *table = new TABLE(schemaID, tableID);
  CreateTagReaderSpec(&post_);

  ASSERT_EQ(table->Init(thread_pool_ctx_, post_), SUCCESS);
  k_int32 count = table->FieldCount();
  ASSERT_EQ(count, sizeof(datatype)/sizeof(int)/2);

  for (k_int32 i = 0; i < count; ++i) {
    Field *field = table->GetFieldWithColNum(i);
    EXPECT_TRUE(field != nullptr);
  }

  SafeDeletePointer(post_);
  SafeDeletePointer(table);
}
}  // namespace kwdbts
