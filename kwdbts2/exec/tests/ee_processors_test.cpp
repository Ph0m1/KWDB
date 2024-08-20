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

#include "ee_processors.h"

#include <filesystem>
#include <fstream>

#include "ee_exec_pool.h"
#include "ee_executor.h"
#include "ee_iterator_data_test.h"
#include "ee_kwthd_context.h"
#include "ee_metadata_data_test.h"
#include "th_kwdb_dynamic_thread_pool.h"
namespace fs = std::filesystem;

string kDbPath = "./test_db";

const string TestBigTableInstance::kw_home =
    kDbPath;  // The current directory is the storage directory of the big table
const string TestBigTableInstance::db_name = "tsdb";  // database name
const uint64_t TestBigTableInstance::iot_interval = 3600;
namespace kwdbts {

class TestProcessors : public TestBigTableInstance {
 public:
  kwdbContext_t g_pool_context;
  kwdbContext_p ctx_ = &g_pool_context;

  TestProcessors() {
    InitServerKWDBContext(ctx_);
  }
  ~TestProcessors() {}

 protected:
  static void SetUpTestCase() {}

  static void TearDownTestCase() {}

  virtual void SetUp() {
    system(("rm -rf " + kDbPath + "/*").c_str());

    KTableKey table_id = 100;
    CreateTestTsEngine(ctx_, kDbPath, table_id);
    CreateScanFlowSpecAllCases(ctx_, &flow_, table_id);
  }
  virtual void TearDown() {
    system(("rm -rf " + kDbPath + "/*").c_str());
    SafeDelete(flow_);
  }
  TSFlowSpec* flow_{nullptr};
  EngineOptions opts_;
};

TEST_F(TestProcessors, TestPubsubResult) {
  KWThdContext *thd = new KWThdContext();
  current_thd = thd;
  ASSERT_EQ(ExecPool::GetInstance().Init(ctx_), SUCCESS);
  ExecPool::GetInstance().db_path_ = kDbPath + "/temp_db_/";
  if (access(ExecPool::GetInstance().db_path_.c_str(), 0)) {
    std::string cmd = "mkdir -p " + ExecPool::GetInstance().db_path_;
    system(cmd.c_str());
  } else {
    for (const auto& entry :
         fs::directory_iterator(ExecPool::GetInstance().db_path_)) {
      if (!fs::is_directory(entry)) {
        fs::remove(entry.path());  // delete file
      }
    }
  }

  char* result{nullptr};
  Processors* processors = KNEW Processors();
  ASSERT_EQ(processors->Init(ctx_, flow_), SUCCESS);
  ASSERT_EQ(processors->InitIterator(ctx_), SUCCESS);
  k_uint32 count;
  k_uint32 size;
  k_bool is_last;
  EXPECT_EQ(processors->RunWithEncoding(ctx_, &result, &size, &count, &is_last),
            KStatus::SUCCESS);
  if (result) {
    free(result);
  }
  EXPECT_EQ(processors->CloseIterator(ctx_), KStatus::SUCCESS);
  processors->Reset();
  delete processors;
  KTableKey table_id = 0;
  DropTsTable(ctx_, table_id);
  CloseTestTsEngine(ctx_);
  ExecPool::GetInstance().Stop();
  thd->Reset();
  SafeDeletePointer(thd);
}

// Processors::InitProcessorsOptimization
}  // namespace kwdbts
