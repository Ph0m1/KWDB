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

#include "ee_sort_scan_op.h"

#include <queue>

#include "cm_func.h"
#include "ee_cancel_checker.h"
#include "ee_common.h"
#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "lg_api.h"

namespace kwdbts {

SortScanOperator::SortScanOperator(TSReaderSpec* spec, TSPostProcessSpec* post,
                                   TABLE* table, BaseOperator* input,
                                   int32_t processor_id)
    : TableScanOperator(spec, post, table, input, processor_id), spec_{spec} {}

SortScanOperator::SortScanOperator(const SortScanOperator& other,
                                   BaseOperator* input, int32_t processor_id)
    : TableScanOperator(other, input, processor_id), spec_{other.spec_} {}

SortScanOperator::~SortScanOperator() {}

EEIteratorErrCode SortScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  do {
    code = TableScanOperator::Init(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("TableScanOperator::PreInit() failed\n");
      break;
    }
    const TSOrdering& order = spec_->sorter();
    k_uint32 order_size_ = order.columns_size();
    if (order_size_ != 1) {
      LOG_ERROR("SortScanOperator must have one order column");
      break;
    }

    k_int32 idx = order.columns(0).col_idx();
    TSOrdering_Column_Direction direction = order.columns(0).direction();
    if (TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC ==
        direction) {
      table_->is_reverse_ = 1;
    }

    order_field_ = param_.GetOutputField(ctx, idx);
  } while (0);

  Return(code);
}

EEIteratorErrCode SortScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = TableScanOperator::Start(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    return code;
  }

  // set current offset
  cur_offset_ = offset_;

  KWThdContext* thd = current_thd;
  StorageHandler* handler = handler_;

  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  code = initContainer(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    return code;
  }

  k_uint32 limit = limit_ + offset_;
  // read data
  while (true) {
    ScanRowBatchPtr data_handle;
    code = InitScanRowBatch(ctx, &data_handle);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    data_handle->ts_ = ts_;
    code = handler->TsNext(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      if (EEIteratorErrCode::EE_END_OF_RECORD == code ||
          EEIteratorErrCode::EE_TIMESLICE_OUT == code) {
        code = EEIteratorErrCode::EE_OK;
      }
      break;
    }

    // resolve filter
    ResolveFilter(ctx, data_handle);
    if (0 == data_handle->Count()) {
      continue;
    }
    // sort
    PrioritySort(ctx, data_handle, limit_ + offset_);
  }

  Return(code);
}

EEIteratorErrCode SortScanOperator::Next(kwdbContext_p ctx,
                                         DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  if (is_done_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }

  chunk = std::move(data_chunk_);
  is_done_ = true;

  Return(EEIteratorErrCode::EE_OK);
}

KStatus SortScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = input_->Close(ctx);
  Reset(ctx);

  Return(ret);
}

EEIteratorErrCode SortScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  TableScanOperator::Reset(ctx);
  Data* del = nullptr;
  while (!data_asc_.empty()) {
    del = data_asc_.top();
    data_asc_.pop();
    SafeDeletePointer(del);
  }
  while (!data_desc_.empty()) {
    del = data_desc_.top();
    data_desc_.pop();
    SafeDeletePointer(del);
  }

  ts_ = 0;
  order_field_ = nullptr;
  spec_ = nullptr;
  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator* SortScanOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter =
      NewIterator<SortScanOperator>(*this, input, this->processor_id_);
  return iter;
}

EEIteratorErrCode SortScanOperator::initContainer(kwdbContext_p ctx) {
  // init col
  std::vector<ColumnInfo> col_info;
  col_info.reserve(output_fields_.size());
  for (auto field : output_fields_) {
    col_info.emplace_back(field->get_storage_length(),
                          field->get_storage_type(), field->get_return_type());
  }

  data_chunk_ = std::make_unique<DataChunk>(col_info, limit_ + offset_);
  if (data_chunk_->Initialize() != true) {
    data_chunk_ = nullptr;
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  return EEIteratorErrCode::EE_OK;
}

EEIteratorErrCode SortScanOperator::ResolveFilter(kwdbContext_p ctx,
                                                  ScanRowBatchPtr data_handle) {
  if (nullptr == filter_) {
    return EEIteratorErrCode::EE_OK;
  }

  for (int i = 0; i < data_handle->count_; ++i) {
    k_int64 ret = filter_->ValInt();
    if (0 == ret) {
      data_handle->NextLine();
      continue;
    }

    data_handle->AddSelection();
    data_handle->NextLine();
  }
  data_handle->ResetLine();
  data_handle->is_filter_ = true;
  return EEIteratorErrCode::EE_OK;
}

EEIteratorErrCode SortScanOperator::PrioritySort(kwdbContext_p ctx,
                                                 ScanRowBatchPtr data_handle,
                                                 k_uint32 limit) {
  k_uint32 count = data_handle->Count();
  k_uint32 free = limit - data_chunk_->Count();
  k_uint32 num = free > count ? count : free;
  if (0 == table_->is_reverse_) {  // asc
    for (k_uint32 i = 0; i < num; ++i) {
      Data* data = new Data;
      data->ts_ = order_field_->ValInt();
      data->rowno_ = data_chunk_->Count();
      data_asc_.push(data);
      FieldsToChunk(renders_, num_, data->rowno_, data_chunk_);
      data_chunk_->AddCount();
      data_handle->NextLine();
    }

    for (k_uint32 i = num; i < count; ++i) {
      k_int64 ts = order_field_->ValInt();
      Data *top = data_asc_.top();
      if (ts < top->ts_) {
        data_asc_.pop();
        Data* data = new Data;
        data->ts_ = ts;
        data->rowno_ = top->rowno_;
        FieldsToChunk(renders_, num_, top->rowno_, data_chunk_);
        data_asc_.push(data);
        SafeDeletePointer(top);
      }
      data_handle->NextLine();
    }
    if (data_asc_.size() == limit) {
      ts_ = data_asc_.top()->ts_;
    }
  } else {
    for (k_uint32 i = 0; i < num; ++i) {
      Data* data = new Data;
      data->ts_ = order_field_->ValInt();
      data->rowno_ = data_chunk_->Count();
      data_desc_.push(data);
      FieldsToChunk(renders_, num_, data->rowno_, data_chunk_);
      data_chunk_->AddCount();
      data_handle->NextLine();
    }

    for (k_uint32 i = num; i < count; ++i) {
      k_int64 ts = order_field_->ValInt();
      Data *top = data_desc_.top();
      if (ts > top->ts_) {
        data_desc_.pop();
        Data* data = new Data;
        data->ts_ = ts;
        data->rowno_ = top->rowno_;
        FieldsToChunk(renders_, num_, top->rowno_, data_chunk_);
        data_desc_.push(data);
        SafeDeletePointer(top);
      }
      data_handle->NextLine();
    }
    if (data_desc_.size() == limit) {
      ts_ = data_desc_.top()->ts_;
    }
  }

  return EEIteratorErrCode::EE_OK;
}

}  // namespace kwdbts
