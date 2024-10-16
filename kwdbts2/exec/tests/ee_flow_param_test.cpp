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
#include "ee_flow_param.h"

#include "ee_global.h"
#include "ee_iterator_data_test.h"
// #include "ee_metadata_data_test.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
namespace kwdbts {
class TestFlowParam : public testing::Test {
 public:
  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;

  TestFlowParam() { InitServerKWDBContext(ctx_); }

 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
  void CreatePostProcessSpec(TSTagReaderSpec **spec, TSPostProcessSpec** post) {
    *spec = new TSTagReaderSpec();
    (*spec)->set_tableversion(1);
    TSCol* col0 = (*spec)->add_colmetas();
    col0->set_storage_type(roachpb::DataType::INT);
    col0->set_storage_len(4);
    TSCol* col1 = (*spec)->add_colmetas();
    col1->set_storage_type(roachpb::DataType::INT);
    col1->set_storage_len(4);

    *post = KNEW TSPostProcessSpec();
    (*post)->set_limit(3);
    (*post)->set_offset(1);
    (*post)->set_filter(
        "@2 + 3:::INT8 > 3:::INT8 AND @2 - 4:::INT8 < 5:::INT8 AND @2 == 4:::INT8 "
        "AND @2  != 'a ':::STRING AND"
        "@2 ^ 3:::INT8 > Function:::floor(@2)"
        " AND @2 % 4:::INT8 < "
        "5:::INT8 AND @2 == 4:::INT8 AND @2 LIKE 'a ':::STRING OR "
        "@2 / 2:::FLOAT >= 3:::INT8 OR @2 * 2:::INT8 <= 5:::INT8 AND @1 = "
        "'\\xbbffee':::BYTES");
    (*post)->add_outputcols(0);
    (*post)->add_outputcols(1);
    (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
    (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  }
  virtual void SetUp() {
    KDatabaseId schemaID = 1;
    KTableId tableID = 1;
    table_ = new TABLE(schemaID, tableID);

    CreatePostProcessSpec(&spec_, &post_);
    table_->Init(ctx_, spec_);
  }
  virtual void TearDown() {
    SafeDeletePointer(table_);
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
    free(renders_);
  }
  TABLE* table_{nullptr};
  TSTagReaderSpec *spec_{nullptr};
  TSPostProcessSpec* post_{nullptr};
  Field* filter_{nullptr};
  Field** renders_{nullptr};
};

TEST_F(TestFlowParam, TestReadPostResolve) {
  ReaderPostResolve* postResolve = new ReaderPostResolve(post_, table_);
  // ASSERT_EQ(postResolve->Init(ctx_), EEIteratorErrCode::EE_OK);

  ASSERT_EQ(postResolve->ResolveFilter(ctx_, &filter_, false),
            EEIteratorErrCode::EE_OK);
  // EXPECT_NE(filter_, nullptr);
  // render num
  k_uint32 num_{0};
  postResolve->RenderSize(ctx_, &num_);
  EXPECT_EQ(num_, 2);
  // int this layer, the operator projection column

  ASSERT_EQ(postResolve->ResolveRender(ctx_, &renders_, num_),
            EEIteratorErrCode::EE_OK);

  // null tag filter
  Field* tag_filter_{nullptr};
  ASSERT_EQ(postResolve->ResolveFilter(ctx_, &tag_filter_, true),
            EEIteratorErrCode::EE_OK);

  ASSERT_EQ(postResolve->ResolveOutputType(ctx_, renders_, num_),
            EEIteratorErrCode::EE_OK);

  SafeDeletePointer(postResolve);
}

}  // namespace kwdbts
