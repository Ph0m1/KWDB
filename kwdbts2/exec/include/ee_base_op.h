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
// Created by liguoliang on 2022/07/18.
#pragma once

#include <utility>
#include <vector>

#include "cm_kwdb_context.h"
#include "ee_row_batch.h"
#include "ee_global.h"
#include "er_api.h"
#include "ee_data_chunk.h"

namespace kwdbts {

class TABLE;
class Field;
/**
 * @brief   The base class of the operator module
 *
 * @author  liguoliang
 */
class BaseOperator {
 public:
  BaseOperator() {}
  explicit BaseOperator(TABLE* table, int32_t processor_id) : table_(table), processor_id_(processor_id) {}
  virtual ~BaseOperator() {
    if (num_ > 0 && renders_) {
      free(renders_);
    }

    for (auto field : output_fields_) {
      SafeDeletePointer(field);
    }

    num_ = 0;
    renders_ = nullptr;
  }

  BaseOperator(const BaseOperator&) = delete;
  BaseOperator& operator=(const BaseOperator&) = delete;
  BaseOperator(BaseOperator&&) = delete;
  BaseOperator& operator=(BaseOperator&&) = delete;

  virtual EEIteratorErrCode Init(kwdbContext_p ctx) = 0;

  virtual EEIteratorErrCode Start(kwdbContext_p ctx) = 0;
  // get next batch data
  virtual EEIteratorErrCode Next(kwdbContext_p ctx) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  virtual EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  virtual EEIteratorErrCode Reset(kwdbContext_p ctx) {
    return EEIteratorErrCode::EE_OK;
  }

  virtual RowBatchPtr GetRowBatch(kwdbContext_p ctx) {return nullptr; }

  virtual KStatus Close(kwdbContext_p ctx) = 0;

  virtual Field** GetRender() { return renders_; }

  virtual Field* GetRender(int i) {
    if (i < num_)
      return renders_[i];
    else
      return nullptr;
  }
  // render size
  virtual k_uint32 GetRenderSize() { return num_; }
  // table object
  virtual TABLE* table() { return table_; }
  // output fields
  virtual std::vector<Field*>& OutputFields() { return output_fields_; }
  virtual BaseOperator* Clone() { return nullptr; }
  virtual k_uint32 GetTotalReadRow() { return 0;}

 protected:
  Field** renders_{nullptr};  // the operator projection column of this layer
  k_uint32 num_{0};           // count of projected column
  TABLE* table_{nullptr};     // table object
  k_int32 processor_id_{0};   // operator ID
  // output columns of the current layer，input columns of Parent
  // operator(FieldNum)
  std::vector<Field*> output_fields_;
  bool is_clone_{false};
};

template <class RealNode, class... Args>
BaseOperator* NewIterator(Args&&... args) {
  return new RealNode(std::forward<Args>(args)...);
}

}  // namespace kwdbts
