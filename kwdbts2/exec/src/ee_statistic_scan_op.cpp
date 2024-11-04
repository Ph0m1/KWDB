
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

#include "ee_statistic_scan_op.h"

#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_tag_scan_op.h"
#include "lg_api.h"
#include "ee_scan_op.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

TableStatisticScanOperator::TableStatisticScanOperator(TsFetcherCollection* collection,
    TSStatisticReaderSpec* spec, TSPostProcessSpec* post, TABLE* table, BaseOperator* input, int32_t processor_id)
    : BaseOperator(collection, table, processor_id),
      post_(post),
      schema_id_(0),
      object_id_(spec->tableid()),
      param_(spec, post, table),
      input_(input) {
  for (int i = 0; i < spec->tsspans_size(); i++) {
    TsSpan* span = spec->mutable_tsspans(i);
    KwTsSpan ts_span;
    if (span->has_fromtimestamp()) {
      ts_span.begin = span->fromtimestamp();
    }

    if (span->has_totimestamp()) {
      ts_span.end = span->totimestamp();
    }
    ts_kwspans_.push_back(ts_span);
  }
  if (0 == spec->tsspans_size()) {
    KwTsSpan ts_span;
    ts_span.begin = kInt64Min;
    ts_span.end = kInt64Max;
    ts_kwspans_.push_back(ts_span);
  }
}

TableStatisticScanOperator::TableStatisticScanOperator(
    const TableStatisticScanOperator& other, BaseOperator* input,
    int32_t processor_id)
    : BaseOperator(other),
      post_(other.post_),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      param_(other.param_.spec_, other.post_, other.table_),
      input_{input} {
  is_clone_ = true;
  param_.insert_ts_index_ = other.param_.insert_ts_index_;
  param_.insert_last_tag_ts_num_ = other.param_.insert_last_tag_ts_num_;
}

TableStatisticScanOperator::~TableStatisticScanOperator() {
}

EEIteratorErrCode TableStatisticScanOperator::InitHandler(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  handler_ = KNEW StorageHandler(table_);
  if (!handler_) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  handler_->Init(ctx);
  handler_->SetTagScan(static_cast<TagScanBaseOperator *>(input_));
  handler_->SetSpans(&ts_kwspans_);
  Return(ret);
}

EEIteratorErrCode TableStatisticScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

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

EEIteratorErrCode TableStatisticScanOperator::InitScanRowBatch(kwdbContext_p ctx, ScanRowBatch **row_batch) {
  EnterFunc();
  KWThdContext *thd = current_thd;
  *row_batch = static_cast<ScanRowBatch *>(thd->GetRowBatch());
  if (nullptr != *row_batch) {
    (*row_batch)->Reset();
  } else {
    *row_batch = KNEW ScanRowBatch(table_);
    thd->SetRowBatch(*row_batch);
  }

  if (nullptr == (*row_batch)) {
    LOG_ERROR("make scan data handle failed");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::Init(kwdbContext_p ctx) {
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

    param_.RenderSize(ctx, &num_);

    if (!is_clone_) {
      ret = param_.ResolveScanCols(ctx);
    }

    ret = param_.ResolveRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve ResolveRender failed!\n");
      break;
    }

    // resolve output field(return type)
    ret = param_.ResolveOutputType(ctx, renders_, num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputType() failed\n");
      break;
    }

    // dispose Output Fields
    ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // dispose tag span filter
    tag_count_read_index_ = param_.ResolveChecktTagCount();

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
  } while (0);

  Return(ret);
}

KStatus TableStatisticScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  KStatus ret;
  if (input_) {
    ret = input_->Close(ctx);
  }
  Return(KStatus::SUCCESS);
}

EEIteratorErrCode TableStatisticScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  SafeDeletePointer(handler_);
  SafeDeletePointer(row_batch_);
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  do {
    code = InitScanRowBatch(ctx, &row_batch_);
    if (EEIteratorErrCode::EE_OK != code) {
      Return(code);
    }

    while (!is_done_) {
      code = handler_->TsNext(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        code = EEIteratorErrCode::EE_END_OF_RECORD;
        if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
          output_queue_.push(std::move(current_data_chunk_));
        }
        break;
      }
      if (row_batch_->count_ < 1) {
        continue;
      } else {
        break;
      }
    }

    // reset line
    row_batch_->ResetLine();

    if (!is_done_) {
      if (row_batch_->Count() > 0) {
        if (0 == ProcessPTagSpanFilter(row_batch_)) {
          continue;
        }
        if (current_data_chunk_->Capacity() - current_data_chunk_->Count() < row_batch_->Count()) {
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

        KStatus status = current_data_chunk_->AddRowBatchData(ctx, row_batch_, renders_);
        if (status != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
      }
    } else {
      if (current_data_chunk_ != nullptr && current_data_chunk_->Count() > 0) {
        output_queue_.push(std::move(current_data_chunk_));
      }
    }
  } while (!is_done_ && output_queue_.empty());

  if (!output_queue_.empty()) {
    chunk = std::move(output_queue_.front());
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, thd, chunk);
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

k_int64 TableStatisticScanOperator::ProcessPTagSpanFilter(RowBatch* row_batch) {
  if (tag_count_read_index_ < 0) {
    return 1;
  }

  row_batch->ResetLine();
  char* data = row_batch->GetData(tag_count_read_index_, sizeof(k_int64),
                                  roachpb::KWDBKTSColumn::TYPE_DATA,
                                  roachpb::DataType::BIGINT);
  k_int64 val = *reinterpret_cast<k_int64*>(data);
  return val;
}

BaseOperator* TableStatisticScanOperator::Clone() {
  // input_ is the object of TagScanIterator，shared by the object of TableScanIterator
  BaseOperator* iter = NewIterator<TableStatisticScanOperator>(*this, input_, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
