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

#pragma once

#include <gtest/gtest.h>
#include <string>
#include "ee_kwthd_context.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "engine.h"
#include "ee_exec_pool.h"
#include "ee_op_engine_utils.h"

extern DedupRule g_dedup_rule;

extern "C" {
// Tests are run in plain C++, we need a symbol for isCanceledCtx, normally
// implemented on the Go side.
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}  // extern "C"

namespace kwdbts {
class OperatorTestBase : public ::testing::Test {
 public:
  static const string kw_home;
  static const string data_root;

  OperatorTestBase() = delete;

  explicit OperatorTestBase(KTableId table_id) : table_id_(table_id) {
    g_dedup_rule = kwdbts::DedupRule::KEEP;
    system(("rm -rf " + kw_home).c_str());
    system(("rm -rf " + data_root).c_str());

    InitServerKWDBContext(ctx_);
    engine_ = CreateTestTsEngine(ctx_, data_root);
  }

  ~OperatorTestBase() override {
  }

 protected:
  void SetUp() override {
    ExecPool::GetInstance().Init(ctx_);
  }

  void TearDown() override {
    ExecPool::GetInstance().Stop();
    CloseTestTsEngine(ctx_);
  }

  kwdbContext_t test_context;
  kwdbContext_p ctx_ = &test_context;
  TSEngine* engine_{nullptr};
  KTableId table_id_{0};
};

const string OperatorTestBase::kw_home = "./test_db";
const string OperatorTestBase::data_root = "tsdb";
}