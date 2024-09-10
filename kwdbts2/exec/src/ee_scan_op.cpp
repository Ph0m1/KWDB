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

#include "ee_scan_op.h"

#include <cmath>
#include <memory>
#include <string>
#include <vector>
#include <chrono>

#include "cm_func.h"
#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_tag_scan_op.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "lg_api.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

TableScanOperator::TableScanOperator(TSReaderSpec* spec, TSPostProcessSpec* post,
                                     TABLE* table, BaseOperator* input, int32_t processor_id)
    : BaseOperator(table, processor_id),
      post_(post),
      schema_id_(0),
      object_id_(spec->tableid()),
      limit_(post->limit()),
      offset_(post->offset()),
      param_(post, table),
      input_{input} {
  int count = spec->ts_spans_size();
  for (int i = 0; i < count; ++i) {
    TsSpan* span = spec->mutable_ts_spans(i);
    KwTsSpan ts_span;
    if (span->has_fromtimestamp()) {
      ts_span.begin = span->fromtimestamp();
    }
    if (span->has_totimestamp()) {
      ts_span.end = span->totimestamp();
    }
    ts_kwspans_.push_back(ts_span);
  }
  if (0 == count) {
    KwTsSpan ts_span;
    ts_span.begin = kInt64Min;
    ts_span.end = kInt64Max;
    ts_kwspans_.push_back(ts_span);
  }
}

TableScanOperator::TableScanOperator(const TableScanOperator& other, BaseOperator* input, int32_t processor_id)
    : BaseOperator(other.table_, processor_id),
      post_(other.post_),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      limit_(other.limit_),
      offset_(other.offset_),
      param_(other.post_, other.table_),
      input_{input} {
  is_clone_ = true;
}

TableScanOperator::~TableScanOperator() = default;

EEIteratorErrCode TableScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();

  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    if (input_) {
      ret = input_->Init(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Tag Scan Init() failed\n");
        break;
      }
    }

    // post->filter;
    ret = param_.ResolveFilter(ctx, &filter_, false);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve::ResolveFilter() failed\n");
      break;
    }

    // render num
    param_.RenderSize(ctx, &num_);

    // resolve renders
    ret = param_.ResolveRender(ctx, &renders_, num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveRender() failed\n");
      break;
    }

    // resolve output type(return type)
    ret = param_.ResolveOutputType(ctx, renders_, num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputType() failed\n");
      break;
    }

    // output Field
    ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // init column info used by data chunk.
    output_col_info_.reserve(output_fields_.size());
    for (auto field : output_fields_) {
      output_col_info_.emplace_back(field->get_storage_length(), field->get_storage_type(), field->get_return_type());
    }

    constructDataChunk();
    if (current_data_chunk_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR)
    }

    batch_copy_ = nullptr == filter_ && 0 == offset_ && 0 == limit_;

    for (int i = 0; i < num_; i++) {
      auto field = renders_[i];
      if (field->get_field_type() != Field::Type::FIELD_ITEM) {
        batch_copy_ = false;
      }
    }

    if (!is_clone_) {
      ret = param_.ResolveScanCols(ctx);
    }
  } while (0);

  Return(ret);
}

EEIteratorErrCode TableScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  cur_offset_ = offset_;

  code = InitHandler(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  if (input_) {
    code = input_->Start(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("Tag Scan Init() failed\n");
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;

  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  do {
    if (limit_ && examined_rows_ >= limit_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      break;
    }
    ScanRowBatchPtr data_handle;
    code = InitScanRowBatch(ctx, &data_handle);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // read data
    while (true) {
      code = handler_->TsNext(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        break;
      }

      total_read_row_ += data_handle->count_;

      if (nullptr == filter_ && 0 == cur_offset_ && 0 == limit_) {
        examined_rows_ += data_handle->count_;
        break;
      }

      // filter || offset || limit
      for (int i = 0; i < data_handle->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            data_handle->NextLine();
            continue;
          }
        }

        k_bool result = ResolveOffset();
        if (result) {
          data_handle->NextLine();
          continue;
        }

        if (limit_ && examined_rows_ >= limit_) {
          break;
        }

        data_handle->AddSelection();
        data_handle->NextLine();
        ++examined_rows_;
      }

      if (0 != data_handle->GetSelection()->size()) {
        break;
      }
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode TableScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;

  auto start = std::chrono::high_resolution_clock::now();
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  do {
    if (limit_ && examined_rows_ >= limit_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      is_done_ = true;
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        output_queue_.push(std::move(current_data_chunk_));
      }
      break;
    }

    ScanRowBatchPtr data_handle;
    code = InitScanRowBatch(ctx, &data_handle);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // read data
    while (!is_done_) {
      code = handler_->TsNext(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        break;
      }

      if (data_handle->count_ < 1) continue;

      if (nullptr == filter_ && 0 == cur_offset_ && 0 == limit_) {
        total_read_row_ += data_handle->count_;
        examined_rows_ += data_handle->count_;
        break;
      }

      // filter || offset || limit
      for (int i = 0; i < data_handle->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            data_handle->NextLine();
            ++total_read_row_;
            continue;
          }
        }

        k_bool result = ResolveOffset();
        if (result) {
          data_handle->NextLine();
          ++total_read_row_;
          continue;
        }

        if (limit_ && examined_rows_ >= limit_) {
          break;
        }

        data_handle->AddSelection();
        data_handle->NextLine();
        ++examined_rows_;
        ++total_read_row_;
      }

      if (0 != data_handle->GetSelection()->size()) {
        break;
      }
    }

    if (!is_done_) {
      if (data_handle->Count() > 0) {
        if (current_data_chunk_->Capacity() - current_data_chunk_->Count() < data_handle->Count()) {
          // the current chunk have no enough space to save the whole data results in data_handle,
          // push it into the output queue, create a new one and ensure it has enough space.
          if (current_data_chunk_->Count() > 0) {
            output_queue_.push(std::move(current_data_chunk_));
          }
          k_uint32 capacity = DataChunk::EstimateCapacity(output_col_info_);
          if (capacity >= data_handle->Count()) {
            constructDataChunk();
          } else {
            constructDataChunk(data_handle->Count());
          }
          if (current_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(EEIteratorErrCode::EE_ERROR)
          }
        }
        current_data_chunk_->AddRowBatchData(ctx, data_handle.get(), renders_,
                                             batch_copy_ && !data_handle->hasFilter());
      }
    } else {
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        output_queue_.push(std::move(current_data_chunk_));
      }
    }
  } while (!is_done_ && output_queue_.empty());

  auto* fetchers = static_cast<VecTsFetcher*>(ctx->fetcher);
  if (fetchers != nullptr && fetchers->collected) {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<int64_t, std::nano> duration = end - start;
    if (nullptr != chunk) {
      ScanRowBatchPtr data_handle = std::dynamic_pointer_cast<ScanRowBatch>(thd->GetRowBatch());
      int64_t bytes_read = int64_t(chunk->Capacity()) * int64_t(chunk->RowSize());
      chunk->GetFvec().AddAnalyse(ctx, this->processor_id_, duration.count(),
                                  int64_t(data_handle->count_), bytes_read, 0, 0);
    }
  }

  if (!output_queue_.empty()) {
    chunk = std::move(output_queue_.front());
    output_queue_.pop();
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK)
    } else {
      Return(code)
    }
  } else {
    if (is_done_) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD);
    } else {
      Return(code)
    }
  }
}

EEIteratorErrCode TableScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  SafeDeletePointer(handler_);
  Return(EEIteratorErrCode::EE_OK);
}

KStatus TableScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  KStatus ret;
  if (input_) {
    ret = input_->Close(ctx);
  }
  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitHandler(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  handler_ = KNEW StorageHandler(table_);
  if (!handler_) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  handler_->Init(ctx);
  handler_->SetTagScan(static_cast<TagScanOperator*>(input_));
  handler_->SetSpans(&ts_kwspans_);

  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitScanRowBatch(kwdbContext_p ctx, ScanRowBatchPtr* row_batch) {
  EnterFunc();
  KWThdContext* thd = current_thd;
  *row_batch = std::dynamic_pointer_cast<ScanRowBatch>(thd->GetRowBatch());
  if (nullptr != *row_batch) {
    (*row_batch)->Reset();
  } else {
    *row_batch = std::make_shared<ScanRowBatch>(table_);
    thd->SetRowBatch(*row_batch);
  }

  if (nullptr == (*row_batch)) {
    LOG_ERROR("make scan data handle failed");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

RowBatchPtr TableScanOperator::GetRowBatch(kwdbContext_p ctx) {
  EnterFunc();
  KWThdContext* thd = current_thd;
  Return(thd->GetRowBatch());
}

k_bool TableScanOperator::ResolveOffset() {
  k_bool ret = KFALSE;

  do {
    if (cur_offset_ == 0) {
      break;
    }
    --cur_offset_;
    ret = KTRUE;
  } while (0);

  return ret;
}

BaseOperator* TableScanOperator::Clone() {
  // input_ is TagScanIterator object，shared by TableScanIterator
  BaseOperator* iter = NewIterator<TableScanOperator>(*this, input_, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
