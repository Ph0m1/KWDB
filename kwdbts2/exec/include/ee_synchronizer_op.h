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

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <deque>

#include "ee_aggregate_flow_spec.h"
#include "ee_base_op.h"
#include "ee_flow_param.h"
#include "ee_pb_plan.pb.h"
#include "ee_parallel_group.h"
#include "ee_row_batch.h"
#include "ee_tag_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "tag_iterator.h"
#include "ee_global.h"

namespace kwdbts {

class AggregateRowBatch;
class TSPostProcessSpec;
class TagScanIterator;

class SynchronizerOperator : public BaseOperator {
 public:
  SynchronizerOperator(BaseOperator *input, TSSynchronizerSpec *spec,
                       TSPostProcessSpec *post, TABLE *table,
                       int32_t processor_id)
      : BaseOperator(table, processor_id),
        input_{input},
        post_{post},
        input_fields_{input->OutputFields()} {
    if (spec->has_degree()) {
      degree_ = spec->degree();
    }
  }
  virtual ~SynchronizerOperator() {
    for (auto it : clone_iter_list_) {
      SafeDeletePointer(it)
    }
    clone_iter_list_.clear();
    parallel_groups_.clear();
  }

  KStatus PushData(DataChunkPtr &chunk, bool wait = false);
  void PopData(kwdbContext_p ctx, DataChunkPtr &chunk);
  void FinishParallelGroup(EEIteratorErrCode code, const EEPgErrorInfo &pgInfo);
  void CalculateDegree();
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode Start(kwdbContext_p ctx) override;
  KStatus Close(kwdbContext_p ctx) override;

  EEIteratorErrCode InitParallelGroup(kwdbContext_p ctx);
  Field **GetRender() { return input_->GetRender(); }
  Field *GetRender(int i) { return input_->GetRender(i); }
  k_uint32 GetRenderSize() { return input_->GetRenderSize(); }
  std::vector<Field*>& OutputFields() { return input_->OutputFields(); }
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr &chunk);

 protected:
  BaseOperator *input_{nullptr};  // input iterator
  bool is_tp_stop_{false};
  k_uint32 group_num_{0};
  k_uint32 group_done_num_{0};
  TSPostProcessSpec *post_{nullptr};
  EEIteratorErrCode group_code_{EEIteratorErrCode::EE_OK};
  std::list<BaseOperator *> clone_iter_list_;

  // input field (FieldNum)
  std::vector<Field*>& input_fields_;
  k_int32 degree_{0};
  EEPgErrorInfo pg_info_;
  bool is_parallel_{false};

 private:
  /**
   * @brief concurrent locks
   */
  mutable std::mutex lock_;
  /**
   * @brief condition variable
   *
   */
  std::condition_variable not_fill_cv_;
  /*
   * @brief wait condition variable
   */
  std::condition_variable wait_cond_;

  typedef std::deque<DataChunkPtr> DataChunkQueue;
  DataChunkQueue data_queue_;
  /**
   * @brief concurrent locks
   */
  mutable std::mutex pg_lock_;
  std::vector<ParallelGroupPtr> parallel_groups_;
  k_uint32 max_queue_size_{0};
};

};  // namespace kwdbts
