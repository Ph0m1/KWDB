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

#include <queue>

#include "ee_sort_op.h"
#include "cm_func.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "lg_api.h"
// #include "rocksdb/db.h"

namespace kwdbts {

SortOperator::SortOperator(BaseOperator* input, TSSorterSpec* spec,
                           TSPostProcessSpec* post, TABLE* table, int32_t processor_id)
    : BaseOperator(table, processor_id),
      spec_{spec},
      post_{post},
      param_(input, spec, post, table),
      limit_(post->limit()),
      offset_(post->offset()),
      input_{input},
      input_fields_{input->OutputFields()} {}

SortOperator::SortOperator(const SortOperator& other, BaseOperator* input, int32_t processor_id)
    : BaseOperator(other.table_, processor_id),
      spec_(other.spec_),
      post_(other.post_),
      param_(input, other.spec_, other.post_, other.table_),
      limit_(other.post_->limit()),
      offset_(other.post_->offset()),
      input_{input},
      input_fields_{input->OutputFields()} {
  is_clone_ = true;
}

SortOperator::~SortOperator() {
  //  delete input_
  if (is_clone_) {
    delete input_;
  }
}

KStatus SortOperator::ResolveSortCols(kwdbContext_p ctx) {
  EnterFunc();
  if (!spec_->has_output_ordering()) {
    LOG_ERROR("order by clause must has a order field");
    Return(KStatus::FAIL);
  }

  const TSOrdering& ordering = spec_->output_ordering();
  int order_size_ = ordering.columns_size();

  for (k_int32 i = 0; i < order_size_; i++) {
    k_uint32 idx = ordering.columns(i).col_idx();
    TSOrdering_Column_Direction direction = ordering.columns(i).direction();
    order_info_.push_back(ColumnOrderInfo{idx, direction});
  }

  Return(KStatus::SUCCESS);
}

EEIteratorErrCode SortOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  do {
    // input preinit
    code = input_->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve renders num
    param_.RenderSize(ctx, &num_);
    if (0 != num_) {
      // resolve render
      code = param_.ResolveRender(ctx, &renders_, num_);
      if (EEIteratorErrCode::EE_OK != code) {
        LOG_ERROR("ResolveRender() error\n");
        break;
      }
    } else {
      k_uint32 num = input_fields_.size();
      renders_ = static_cast<Field **>(malloc(num * sizeof(Field *)));
      if (!renders_) {
        LOG_ERROR("Malloc faield, size : %lu", num * sizeof(Field *));
        break;
      }
      num_ = num;
      for (k_uint32 i = 0; i < num_; i++) {
        renders_[i] = input_fields_[i];
      }
    }
    // dispose sort col
    KStatus ret = ResolveSortCols(ctx);
    if (ret != KStatus::SUCCESS) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }

    // output Field
    code = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
  } while (0);

  Return(code);
}

EEIteratorErrCode SortOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  cur_offset_ = offset_;

  code = input_->Start(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  std::queue<DataChunkPtr> buffer;
  size_t total_count = 0;
  size_t buffer_size = 0;
  int64_t duration = 0;
  int64_t read_row_num = 0;
  // sort all data
  for (;;) {
    DataChunkPtr chunk = nullptr;

    // read a batch of data
    code = input_->Next(ctx, chunk);
    if (code != EEIteratorErrCode::EE_OK) {
      if (code == EEIteratorErrCode::EE_END_OF_RECORD ||
          code == EEIteratorErrCode::EE_TIMESLICE_OUT) {
        code = EEIteratorErrCode::EE_OK;
        break;
      }
      LOG_ERROR("Failed to fetch data from child operator, return code = %d.\n", code);
      Return(code);
    }
    // no data, continue
    if (chunk == nullptr || chunk->Count() == 0) {
      continue;
    }
    auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
    if (fetchers != nullptr && fetchers->collected) {
      goLock(fetchers->goMutux);
      chunk->GetFvec().GetAnalyse(ctx);
      goUnLock(fetchers->goMutux);
      // analyse collection
      read_row_num += chunk->Count();
    }
    auto start = std::chrono::high_resolution_clock::now();

    LOG_DEBUG("Read a batch of data %d\n", chunk->Count());

    total_count += chunk->Count();
    buffer_size += chunk->RowSize() * chunk->Count();
    KStatus ret = SUCCESS;
    if (is_mem_container) {
      buffer.push(std::move(chunk));
      if (buffer_size > SORT_MAX_MEM_BUFFER_SIZE) {
        is_mem_container = false;
        ret = initContainer(total_count, buffer);
      }
    } else {
      ret = container_->Append(chunk.get());
    }
    if (ret != SUCCESS) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Append data failed.");
      Return(EEIteratorErrCode::EE_ERROR);
    }

    auto end = std::chrono::high_resolution_clock::now();
    if (fetchers != nullptr && fetchers->collected) {
      std::chrono::duration<int64_t, std::nano> t = end - start;
      duration += t.count();
    }
  }

  auto all_start = std::chrono::high_resolution_clock::now();
  if (is_mem_container) {
    KStatus ret = initContainer(total_count, buffer);
    if (ret != SUCCESS) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Init sort container.");
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }
  // Sort
  container_->Sort();

  auto all_end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<int64_t, std::nano> all_t = all_end - all_start;
  duration += all_t.count();
  analyseFetcher(ctx, this->processor_id_, duration, read_row_num, 0,
                 0, 1, 0);

  Return(code);
}

EEIteratorErrCode SortOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  if (is_done_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }

  if (nullptr == chunk) {
    std::vector<ColumnInfo> col_info;
    col_info.reserve(output_fields_.size());
    for (auto field : output_fields_) {
      col_info.emplace_back(field->get_storage_length(),
                            field->get_storage_type(),
                            field->get_return_type());
    }

    chunk = std::make_unique<DataChunk>(col_info);
    if (chunk->Initialize() != true) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      chunk = nullptr;
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }
  current_thd->SetDataChunk(container_.get());
  k_uint32 BATCH_SIZE = chunk->Capacity();
  // record location in current result batch.
  k_uint32 location = 0;
  while (scanned_rows_ < container_->Count()) {
    k_int32 row = container_->NextLine();
    if (row < 0) {
      break;
    }
    ++scanned_rows_;

    // limit
    if (limit_ && examined_rows_ >= limit_) {
      is_done_ = true;
      break;
    }

    // offset
    if (cur_offset_ > 0) {
      --cur_offset_;
      continue;
    }
    chunk->InsertData(ctx, container_.get(), num_ != 0 ? renders_ : nullptr);

    // rowcount ++
    ++examined_rows_;
    ++location;

    if (examined_rows_ % BATCH_SIZE == 0) {
      break;
    }
  }

  if (scanned_rows_ == container_->Count()) {
    is_done_ = true;
  }

  Return(EEIteratorErrCode::EE_OK);
}

KStatus SortOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = input_->Close(ctx);
  Reset(ctx);

  Return(ret);
}

EEIteratorErrCode SortOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  input_->Reset(ctx);

  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator* SortOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter = NewIterator<SortOperator>(*this, input, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
