
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

#include "ee_handler.h"
#include "ee_kwthd.h"
#include "ee_tag_scan_op.h"
#include "lg_api.h"
#include "ee_scan_op.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

TableStatisticScanOperator::TableStatisticScanOperator(
    TSStatisticReaderSpec* spec, TSPostProcessSpec* post, TABLE* table, BaseOperator* input, int32_t processor_id)
    : BaseOperator(table, processor_id),
      post_(post),
      schema_id_(0),
      object_id_(spec->tableid()),
      param_(spec, post, table),
      input_(input) {
  for (int i = 0; i < spec->tsspans_size(); i++) {
    TsSpan *span = spec->mutable_tsspans(i);
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
    : BaseOperator(other.table_, processor_id),
      post_(other.post_),
      schema_id_(other.schema_id_),
      object_id_(other.object_id_),
      ts_kwspans_(other.ts_kwspans_),
      param_(other.param_.spec_, other.post_, other.table_),
      input_{input} {
  is_clone_ = true;
  param_.is_insert_ts_index_ = other.param_.is_insert_ts_index_;
}

TableStatisticScanOperator::~TableStatisticScanOperator() {
}

EEIteratorErrCode TableStatisticScanOperator::InitHandler(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  handle_ = new Handler(table_);
  handle_->PreInit(ctx);
  handle_->SetTagScan(static_cast<TagScanOperator *>(input_));

  if (EEIteratorErrCode::EE_OK == ret) {
    ret = handle_->Init(ctx, &ts_kwspans_);
  }

  Return(ret);
}

EEIteratorErrCode TableStatisticScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KStatus ret = KStatus::FAIL;

  do {
    code = InitHandler(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    if (input_) {
      code = input_->Init(ctx);
      if (code != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Tag Scan Init() failed\n");
        break;
      }
    }
  } while (0);

  Return(code);
}

EEIteratorErrCode TableStatisticScanOperator::InitScanRowBatch(
    kwdbContext_p ctx) {
  EnterFunc();
  if (nullptr != data_handle_) {
    data_handle_ = std::make_shared<ScanRowBatch>(data_handle_.get());
  } else {
    data_handle_ = std::make_shared<ScanRowBatch>(table_);
  }

  if (nullptr == data_handle_) {
    LOG_ERROR("make scan statistic data handle failed");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  current_thd->SetRowBatch(data_handle_);

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::PreInit(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;

  do {
    if (input_) {
      ret = input_->PreInit(ctx);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("Tag Scan PreInit() failed\n");
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
  SafeDeletePointer(handle_);
  if (nullptr != data_handle_) {
    data_handle_.reset();
    data_handle_ = nullptr;
  }
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TableStatisticScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = InitScanRowBatch(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }
  code = handle_->TsNext(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // reset line
  data_handle_->ResetLine();
  if (data_handle_->Count() == 0) {
    Return(code);
  }

  auto start = std::chrono::high_resolution_clock::now();
  // init DataChunk
  if (nullptr == chunk) {
    // init column info
    std::vector<ColumnInfo> col_info;
    col_info.reserve(output_fields_.size());
    for (auto field : output_fields_) {
      col_info.emplace_back(field->get_storage_length(),
                            field->get_storage_type(),
                            field->get_return_type());
    }
    chunk = std::make_unique<DataChunk>(col_info, data_handle_->Count());
    if (chunk->Initialize() < 0) {
      chunk = nullptr;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  // dispose render
  KStatus status = chunk->AddRowBatchData(ctx, data_handle_.get(), renders_);
  if (status != KStatus::SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
  if (fetchers != nullptr && fetchers->collected) {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<int64_t, std::nano> duration = end - start;
    if (nullptr != chunk) {
      int64_t bytes_read = int64_t(chunk->Capacity()) * int64_t(chunk->RowSize());
      chunk->GetFvec().AddAnalyse(ctx, this->processor_id_,
                        duration.count(), int64_t(data_handle_->count_), bytes_read, 0, 0);
    }
  }

  Return(code);
}

BaseOperator* TableStatisticScanOperator::Clone() {
  // input_ is the object of TagScanIterator，shared by the object of TableScanIterator
  BaseOperator* iter = NewIterator<TableStatisticScanOperator>(*this, input_, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
