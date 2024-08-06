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
#include "ee_test_util.h"
#include "ee_kwthd.h"
#include "gtest/gtest.h"
#include "th_kwdb_dynamic_thread_pool.h"

string kDbPath = "./test_db";

const string TestBigTableInstance::kw_home =
    kDbPath;  // The current directory is the storage directory of the big table
const string TestBigTableInstance::db_name = "tsdb";  // database name
const uint64_t TestBigTableInstance::iot_interval = 3600;
namespace kwdbts {

class TestNoopOperator : public TestBigTableInstance {
 public:
  kwdbContext_t g_kwdb_context;
  kwdbContext_p ctx_ = &g_kwdb_context;
  virtual void SetUp() {
    system(("rm -rf " + kDbPath + "/*").c_str());
    TestBigTableInstance::SetUp();
    // meta_.SetUp(ctx_);
    engine_.SetUp(ctx_, kDbPath, table_id_);
    KWDBDynamicThreadPool::GetThreadPool().Init(15, ctx_);
    ASSERT_EQ(ExecPool::GetInstance().Init(ctx_), SUCCESS);
    thd_ = new KWThd();
    current_thd = thd_;
    ASSERT_TRUE(current_thd != nullptr);
    pipeGroup_ = KNEW PipeGroup();
    pipeGroup_->SetDegree(1);
    current_thd->SetPipeGroup(pipeGroup_);
    noop_.SetUp(ctx_, table_id_);
  }

  virtual void TearDown() {
    TestBigTableInstance::TearDown();
    system(("rm -rf " + kDbPath + "/*").c_str());
    // engine_.TearDown(ctx_);
    noop_.TearDown(ctx_);
    CloseTestTsEngine(ctx_);
    SafeDelete(thd_);
    ExecPool::GetInstance().Stop();
    KWDBDynamicThreadPool::GetThreadPool().Stop();
    delete pipeGroup_;
  }

  //   CreateMeta meta_;
  CreateEngine engine_;
  CreateNoop noop_;
  KDatabaseId table_id_{10};
  KWThd *thd_{nullptr};
  PipeGroup* pipeGroup_{nullptr};
};

TEST_F(TestNoopOperator, NoopIterTest) {
  BaseOperator *noop_iter = noop_.iter_;
  EXPECT_EQ(noop_iter->PreInit(ctx_), EE_OK);
  EXPECT_EQ(noop_iter->Init(ctx_), EE_OK);
  EXPECT_EQ(noop_iter->Next(ctx_), EE_END_OF_RECORD);
  EXPECT_EQ(noop_iter->Close(ctx_), SUCCESS);
  noop_iter->GetRowBatch(ctx_);
}

}  // namespace kwdbts
