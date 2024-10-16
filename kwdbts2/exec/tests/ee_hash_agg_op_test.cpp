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

#include "ee_exec_pool.h"
#include "ee_iterator_create_test.h"
#include "engine.h"
#include "../../engine/tests/test_util.h"
#include "ee_op_test_base.h"
#include "ee_kwthd_context.h"
#include "gtest/gtest.h"

namespace kwdbts {

class TestAggIterator : public OperatorTestBase {
 public:
  TestAggIterator() : OperatorTestBase() {}
  virtual void SetUp() {
    OperatorTestBase::SetUp();
    thd_ = new KWThdContext();
    current_thd = thd_;
    ASSERT_TRUE(current_thd != nullptr);
    parallelGroup_ = new ParallelGroup();
    parallelGroup_->SetDegree(1);
    current_thd->SetParallelGroup(parallelGroup_);
    agg_.SetUp(ctx_, table_id_);
  }

  virtual void TearDown() {
    OperatorTestBase::TearDown();
    agg_.TearDown();
    SafeDeletePointer(thd_);
    SafeDeletePointer(parallelGroup_);
  }

 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  CreateAggregate agg_;
  KWThdContext *thd_{nullptr};
  ParallelGroup* parallelGroup_{nullptr};
};

TEST_F(TestAggIterator, AggTest) {
  KStatus ret = KStatus::FAIL;
  BaseOperator *agg = agg_.iter_;
  ASSERT_EQ(agg->Init(ctx_), EE_OK);

  ASSERT_EQ(agg->Start(ctx_), EE_OK);

  DataChunkPtr chunk = nullptr;
  k_int32 j = 2;
  while (j--) {
    if (1 == j) {
      ASSERT_EQ(agg->Next(ctx_, chunk), EE_OK);
    } else {
      ASSERT_EQ(agg->Next(ctx_, chunk), EE_END_OF_RECORD);
    }
  }

  ret = agg->Close(ctx_);
  EXPECT_EQ(ret, KStatus::SUCCESS);
}

}  // namespace kwdbts
