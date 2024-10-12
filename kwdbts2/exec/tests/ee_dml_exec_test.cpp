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
#include "ee_dml_exec.h"

#include "gtest/gtest.h"
// #include <sys/stat.h>
#include "ee_global.h"
#include "libkwdbts2.h"
// #include "test_util.h"
#include "ee_iterator_data_test.h"
#include "ee_metadata_data_test.h"
#include "ee_exec_pool.h"

string kDbPath = "./test_db";

const string TestBigTableInstance::kw_home =
    kDbPath;  // Bigtable's storage directory
const string TestBigTableInstance::db_name = "tsdb";  // database name
const uint64_t TestBigTableInstance::iot_interval = 3600;

namespace kwdbts {
class TestDmlExec : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;

  TestDmlExec() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    // opts_.enable_wal_ = true;
    // opts_.db_path = db_path;

    system(("rm -rf " + kDbPath + "/*").c_str());
    // clean directory
    // KStatus s = TSEngineImpl::OpenTSEngine(ctx_, db_path, opts_,
    // &ts_engine_);
  }

  ~TestDmlExec() {}

 protected:
  virtual void SetUp() {
    ASSERT_EQ(ExecPool::GetInstance().Init(ctx_), SUCCESS);
    // create source data
    CreateTestTsEngine(ctx_, kDbPath, 10);
    CreateScanFlowSpec(ctx_, &flow_, objectid_);
  }
  virtual void TearDown() {
    CloseTestTsEngine(ctx_);
    system(("rm -rf " + kDbPath + "/*").c_str());
    SafeDelete(flow_);
    ExecPool::GetInstance().Stop();
  }
  KTableId objectid_{301};
  TSFlowSpec *flow_{nullptr};
  TSEngine *ts_engine_{nullptr};
};

TEST_F(TestDmlExec, TestDmlExecInit) {
  size_t size = flow_->ByteSizeLong();

  QueryInfo reqInfo, respInfo;
  char *req = static_cast<char *>(static_cast<void *>(&reqInfo));
  char *resp = static_cast<char *>(static_cast<void *>(&respInfo));
  void *message = static_cast<void *>(malloc(size));
  flow_->SerializeToArray(message, size);

  // QueryInfo req;
  QueryInfo *info = reinterpret_cast<QueryInfo *>(req);
  QueryInfo *info2 = reinterpret_cast<QueryInfo *>(resp);
  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = size;
  info->id = 0;
  info->unique_id = 0;
  info->handle = nullptr;
  info->value = message;
  info->relBatchData = nullptr;
  info->relRowCount = 0;

  // DmlExec exec;
  ASSERT_EQ(DmlExec::ExecQuery(ctx_, info, info2), KStatus::SUCCESS);
  // next
  info->tp = EnMqType::MQ_TYPE_DML_NEXT;

  do {
    KStatus ret = DmlExec::ExecQuery(ctx_, info, info2);
    if (ret != KStatus::SUCCESS) {
      break;
    }
    if (respInfo.value) {
      free(respInfo.value);
      respInfo.value = nullptr;
    }
  } while (respInfo.code != -1);
  info->handle = respInfo.handle;
  info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
  DmlExec::ExecQuery(ctx_, info, info2);

  free(message);
}
}  // namespace kwdbts
