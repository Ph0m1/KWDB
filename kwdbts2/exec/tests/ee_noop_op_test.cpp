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

#include "ee_noop_op.h"

#include "ee_exec_pool.h"
#include "ee_iterator_create_test.h"
#include "engine.h"
#include "../../engine/tests/test_util.h"
#include "ee_op_test_base.h"
#include "ee_kwthd_context.h"
#include "gtest/gtest.h"
namespace kwdbts {

class TestNoopOperator : public OperatorTestBase {
 public:
  TestNoopOperator() : OperatorTestBase() {}
  virtual void SetUp() {
    OperatorTestBase::SetUp();
    thd_ = new KWThdContext();
    current_thd = thd_;
    ASSERT_TRUE(current_thd != nullptr);
    parallelGroup_ = new ParallelGroup();
    parallelGroup_->SetDegree(1);
    current_thd->SetParallelGroup(parallelGroup_);
    noop_.SetUp(ctx_, table_id_);
  }

  virtual void TearDown() {
    OperatorTestBase::TearDown();
    noop_.TearDown(ctx_);
    SafeDeletePointer(thd_);
    SafeDeletePointer(parallelGroup_);
  }

  //   CreateMeta meta_;
  CreateNoop noop_;
  KWThdContext *thd_{nullptr};
  ParallelGroup* parallelGroup_{nullptr};
};

TEST_F(TestNoopOperator, NoopIterTest) {
  BaseOperator *noop_iter = noop_.iter_;
  EXPECT_EQ(noop_iter->Init(ctx_), EE_OK);
  EXPECT_EQ(noop_iter->Start(ctx_), EE_OK);
  EXPECT_EQ(noop_iter->Next(ctx_), EE_END_OF_RECORD);
  EXPECT_EQ(noop_iter->Close(ctx_), SUCCESS);
  noop_iter->GetRowBatch(ctx_);
}

}  // namespace kwdbts
