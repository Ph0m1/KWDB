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

TableScanOperator::TableScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, TSPostProcessSpec* post,
                                     TABLE* table, BaseOperator* input, int32_t processor_id)
    : BaseOperator(collection, table, processor_id),
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
    : BaseOperator(other.collection_, other.table_, processor_id),
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
    code = InitScanRowBatch(ctx, &row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // read data
    while (true) {
      code = handler_->TsNext(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        break;
      }

      total_read_row_ += row_batch_->count_;
      LOG_DEBUG("TableScanOperator::Next data_count:%d", total_read_row_);
      if (nullptr == filter_ && 0 == cur_offset_ && 0 == limit_) {
        examined_rows_ += row_batch_->count_;
        break;
      }

      // filter || offset || limit
      for (int i = 0; i < row_batch_->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            row_batch_->NextLine();
            continue;
          }
        }

        k_bool result = ResolveOffset();
        if (result) {
          row_batch_->NextLine();
          continue;
        }

        if (limit_ && examined_rows_ >= limit_) {
          break;
        }

        row_batch_->AddSelection();
        row_batch_->NextLine();
        ++examined_rows_;
      }

      if (0 != row_batch_->GetSelection()->size()) {
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

    code = InitScanRowBatch(ctx, &row_batch_);
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

      if (row_batch_->count_ < 1) continue;

      if (nullptr == filter_ && 0 == cur_offset_ && 0 == limit_) {
        total_read_row_ += row_batch_->count_;
        examined_rows_ += row_batch_->count_;
        break;
      }

      // filter || offset || limit
      for (int i = 0; i < row_batch_->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            row_batch_->NextLine();
            ++total_read_row_;
            continue;
          }
        }

        k_bool result = ResolveOffset();
        if (result) {
          row_batch_->NextLine();
          ++total_read_row_;
          continue;
        }

        if (limit_ && examined_rows_ >= limit_) {
          break;
        }

        row_batch_->AddSelection();
        row_batch_->NextLine();
        ++examined_rows_;
        ++total_read_row_;
      }

      if (0 != row_batch_->GetSelection()->size()) {
        break;
      }
    }

    if (!is_done_) {
      if (row_batch_->Count() > 0) {
        if (current_data_chunk_->Capacity() - current_data_chunk_->Count() < row_batch_->Count()) {
          // the current chunk have no enough space to save the whole data results in row_batch_,
          // push it into the output queue, create a new one and ensure it has enough space.
          if (current_data_chunk_->Count() > 0) {
            output_queue_.push(std::move(current_data_chunk_));
          }
          k_uint32 capacity = DataChunk::EstimateCapacity(output_col_info_);
          if (capacity >= row_batch_->Count()) {
            constructDataChunk();
          } else {
            constructDataChunk(row_batch_->Count());
          }
          if (current_data_chunk_ == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(EEIteratorErrCode::EE_ERROR)
          }
        }
        current_data_chunk_->AddRowBatchData(ctx, row_batch_, renders_,
                                             batch_copy_ && !row_batch_->hasFilter());
      }
    } else {
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        output_queue_.push(std::move(current_data_chunk_));
      }
    }
  } while (!is_done_ && output_queue_.empty());

  if (!output_queue_.empty()) {
    chunk = std::move(output_queue_.front());
    output_queue_.pop();
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(chunk->Count(), (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, 0);
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK)
    } else {
      Return(code)
    }
  } else {
    auto end = std::chrono::high_resolution_clock::now();
    fetcher_.Update(0, (end - start).count(), 0, 0, 0, 0);
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
  SafeDeletePointer(row_batch_);
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
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  handler_->Init(ctx);
  handler_->SetTagScan(static_cast<TagScanBaseOperator*>(input_));
  handler_->SetSpans(&ts_kwspans_);

  Return(ret);
}

EEIteratorErrCode TableScanOperator::InitScanRowBatch(kwdbContext_p ctx, ScanRowBatch** row_batch) {
  if (nullptr != *row_batch) {
    (*row_batch)->Reset();
  } else {
    *row_batch = KNEW ScanRowBatch(table_);
    KWThdContext* thd = current_thd;
    thd->SetRowBatch(*row_batch);
  }

  if (nullptr == (*row_batch)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("make scan data handle failed");
    return EEIteratorErrCode::EE_ERROR;
  }
  return EEIteratorErrCode::EE_OK;
}

RowBatch* TableScanOperator::GetRowBatch(kwdbContext_p ctx) {
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
