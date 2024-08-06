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
#include <memory>

#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_handler.h"
#include "ee_row_batch.h"
#include "ee_task.h"

namespace kwdbts {
class Handler;
class CMergeOperator;
class PipeGroup;
typedef std::shared_ptr<PipeGroup> PipeGroupPtr;
class PipeGroup : public ExecTask {
 private:
  BaseOperator *iterator_{nullptr};
  CMergeOperator *sparent_{nullptr};
  TABLE *table_{nullptr};
  void *ts_engine_{nullptr};
  void *fetcher_{nullptr};
  k_bool is_parallel_pg_{KFALSE};
  bool is_stop_{false};
  uint64_t relation_ctx_;
  k_int32 degree_{1};

 public:
  PipeGroup() {}
  ~PipeGroup();
  CMergeOperator *GetParent() { return sparent_; }
  k_bool GetParallel() { return is_parallel_pg_; }
  k_int32 GetDegree() { return degree_; }
  void SetParallel(k_bool parallel) { is_parallel_pg_ = parallel; }
  void SetIterator(BaseOperator *it) { iterator_ = it; }
  void SetParent(CMergeOperator *parent) { sparent_ = parent; }
  void SetTable(TABLE *table) { table_ = table; }
  void SetDegree(k_int32 degree) { degree_ = degree; }
  KStatus Init(kwdbContext_p ctx);
  void Run(kwdbContext_p ctx);
  void *GetTsEngine() { return ts_engine_; }
  void Stop() { is_stop_ = true; }
};
};  // namespace kwdbts
