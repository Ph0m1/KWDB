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
#include <pthread.h>
#include <list>
#include <utility>

#include "ee_row_batch.h"
#include "ee_pipegroup.h"
#include "kwdb_type.h"

namespace kwdbts {
class KWThd {
 private:
  RowBatchPtr row_batch_{nullptr};
  PipeGroup* pipe_group_{nullptr};
  k_uint64 thd_id_{0};
  Handler  *handler_{nullptr};
  RowContainer* data_chunk_;

 public:
  KWThd() { thd_id_ = pthread_self(); }
  ~KWThd() { Reset(); }

  void SetRowBatch(RowBatchPtr ptr) {
    row_batch_ = ptr;
  }

  void SetPipeGroup(PipeGroup* ptr) {
    pipe_group_ = ptr;
  }

  void Reset() {
    if (row_batch_) {
      row_batch_.reset();
    }
    SafeDeletePointer(handler_);
  }
  RowBatchPtr& GetRowBatch() { return row_batch_; }

  /**
   * Only used to read the data in row_batch_ structure.
   * The caller should ensure the row_batch_ shared_ptr is valid during the read process.
   * @return the original pointer for row_batch_ structure.
   */
  RowBatch* GetRowBatchOriginalPtr() { return row_batch_.get(); }
  PipeGroup* GetPipeGroup() { return pipe_group_; }
  k_uint32 GetDegree() {
    if (pipe_group_) {
      return pipe_group_->GetDegree();
    }
    return 1;
  }
  k_uint64 GetThdID() { return thd_id_; }

  Handler* InitHandler(kwdbContext_p ctx, TABLE *table) {
    handler_ = KNEW Handler(table);
    if (!handler_) {
      return nullptr;
    }
    handler_->PreInit(ctx);
    return handler_;
  }

  Handler *GetHandler() { return handler_; }
  void SetDataChunk(RowContainer *ptr) { data_chunk_ = ptr; }
  RowContainer* GetDataChunk() { return data_chunk_; }

 public:
  static thread_local KWThd *thd_;
};

#define current_thd KWThd::thd_

};  // namespace kwdbts
