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

#include "ee_executor.h"

#include <iostream>
#include <memory>
#include <string>

#include "../../roachpb/ee_pb_plan.pb.h"
// #include "cm_assert.h"
#include "gtest/gtest.h"
// #include "me_cache_manager.h"
// #include "th_kwdb_dynamic_thread_pool.h"
// #include "cn_conn_array.h"

namespace kwdbts {

extern kwdbContext_p g_kwdb_context;
kwdbContext_p ctx = g_kwdb_context;

class TestExecutor : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    InitServerKWDBContext(ctx);
  }

  static void TearDownTestCase() {
    DestroyKWDBContext(ctx);
  }

  void SetUp() override {}
  void TearDown() override {}

 public:
  TestExecutor() {}
};

// Verify Executor initialization and destruction
TEST_F(TestExecutor, ExecutorInitAndDestroy) {
  kwdbts::EngineOptions options;
  options.db_path = "tsdb";
  options.thread_pool_size = 1;
  options.task_queue_size = 1;
  options.buffer_pool_size = 1;
  EXPECT_EQ(kwdbts::InitExecutor(ctx, options), SUCCESS);
  std::cout << "Executor Init ok. " << std::endl;
  EXPECT_EQ(kwdbts::DestoryExecutor(), SUCCESS);
  std::cout << "Executor Destory ok. " << std::endl;
  EXPECT_TRUE(true);
}

}  // namespace kwdbts
