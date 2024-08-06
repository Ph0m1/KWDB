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

#include "ee_post_agg_scan_op.h"
#include <variant>

#include "ee_kwthd.h"
#include "ee_pb_plan.pb.h"
#include "ee_common.h"

namespace kwdbts {

PostAggScanOperator::PostAggScanOperator(BaseOperator* input,
                                         TSAggregatorSpec* spec,
                                         TSPostProcessSpec* post,
                                         TABLE* table, int32_t processor_id)
    : HashAggregateOperator(input, spec, post, table, processor_id) {}

PostAggScanOperator::PostAggScanOperator(const PostAggScanOperator& other, BaseOperator* input, int32_t processor_id)
    : HashAggregateOperator(other, input, processor_id) {}

EEIteratorErrCode PostAggScanOperator::PreInit(kwdbContext_p ctx) {
  EnterFunc();
  auto code = HashAggregateOperator::PreInit(ctx);

  // construct the output column information for agg output.
  agg_output_col_info.reserve(output_fields_.size());
  for (auto field : output_fields_) {
    agg_output_col_info.emplace_back(field->get_storage_length(), field->get_storage_type(),
                                     field->get_return_type());
  }

  data_types_.clear();
  data_types_.reserve(output_fields_.size());
  for (auto field : output_fields_) {
    data_types_.push_back(field->get_storage_type());
  }

  Return(code);
}

KStatus PostAggScanOperator::accumulateRows(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

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
      Return(KStatus::FAIL);
    }

    // no data
    if (chunk->Count() == 0) {
      continue;
    }
    // the chunk->isScanAgg() is always true.
    pass_agg_ &= chunk->isPassAgg();
    agg_result_counter_ += chunk->Count();
    processed_chunks_.push(std::move(chunk));
  }


  // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
  // aggregated.

  /**
   * scalar group
   * select max(c1) from t1 => handler_->NewTagIterator
   */
  if (processed_chunks_.empty()) {
    if (buckets_.empty() && group_cols_.empty() &&
        group_type_ == TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR) {
      // return null
      CombinedGroupKey group_keys;
      group_keys.Reserve(1);
      group_keys.AddGroupKey(std::monostate(), roachpb::DataType::UNKNOWN);

      auto ptr = KNEW char[agg_row_size_];
      std::memset(ptr, 0, agg_row_size_);
      InitFirstLastTimeStamp(ptr);

      // COUNT_ROW or COUNT，return 0
      for (int i = 0; i < aggregations_.size(); i++) {
        if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT) {
          // set not null
          AggregateFunc::SetNotNull(ptr + agg_null_offset_, i);
        }
      }

      buckets_[group_keys] = ptr;
    }
  } else {
    // combine all the result into one chunk and do HASH Agg.
    if (!pass_agg_) {
      DataChunkPtr chunk = constructAggResults(agg_result_counter_);
      if (chunk == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        Return(KStatus::FAIL);
      }
      chunk->Append(processed_chunks_);
      accumulateBatch(ctx, chunk);
    }
  }
  Return(KStatus::SUCCESS);
}

KStatus PostAggScanOperator::getAggResults(kwdbContext_p ctx, DataChunkPtr& results) {
  EnterFunc();
  if (pass_agg_) {
    if (processed_chunks_.empty()) {
      is_done_ = true;
    } else {
      if (nullptr == having_filter_ && 0 == cur_offset_ && 0 == limit_) {
        results = std::move(processed_chunks_.front());
        processed_chunks_.pop();
        examined_rows_ += results->Count();
        Return(KStatus::SUCCESS);
      }

      results = std::move(processed_chunks_.front());
      processed_chunks_.pop();

      // handle offset
      while (cur_offset_ != 0) {
        k_uint64 count = results->Count();
        if (cur_offset_ - (total_read_row_ + count) >= 0) {
          // skip current data chunk
          total_read_row_ += count;
          cur_offset_ -= count;
          results = std::move(processed_chunks_.front());
          processed_chunks_.pop();
        } else {
          // copy data to a new data chunk, and then return it.
          auto data = constructAggResults(count - cur_offset_);
          if (data == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(KStatus::FAIL);
          }
          data->CopyFrom(results, cur_offset_ - total_read_row_, count);
          results = std::move(data);
        }
      }

      // handle limit
      if (limit_ && examined_rows_ >= limit_) {
        is_done_ = true;
        Return(KStatus::SUCCESS);
      }

      k_uint64 count = results->Count();
      while (examined_rows_ >= limit_) {
        if (limit_ - (examined_rows_ + count) > 0) {
          // return current data chunk
          examined_rows_ += count;
          break;
        } else {
          // copy data to a new data chunk, and then return it.
          k_uint32 copy_row_number = limit_ - examined_rows_;

          auto data = constructAggResults(copy_row_number);
          if (data == nullptr) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
            Return(KStatus::FAIL);
          }
          data->CopyFrom(results, 0, copy_row_number - 1);
          results = std::move(data);
          examined_rows_ += copy_row_number;
          break;
        }
      }
      Return(KStatus::SUCCESS);
    }
  } else {
    k_uint32 BATCH_SIZE = results->Capacity();

    // assembling rows
    k_uint32 index = 0;
    while (total_read_row_ < buckets_.size()) {
      DatumRowPtr data = iter_->second;
      for (int col = 0; col < output_fields_.size(); col++) {
        // dispose null
        char* bitmap = data + agg_row_size_ - (output_fields_.size() + 7) / 8;
        if (AggregateFunc::IsNull(bitmap, col)) {
          results->SetNull(index, col);
          continue;
        }

        k_uint32 offset = funcs_[col]->GetOffset();
        if (IsStringType(output_fields_[col]->get_storage_type())) {
          k_uint32 len = 0;
          std::memcpy(&len, data + offset, STRING_WIDE);
          results->InsertData(index, col, data + offset + STRING_WIDE, len);
        } else if (output_fields_[col]->get_storage_type() == roachpb::DataType::DECIMAL) {
          k_uint32 len = output_fields_[col]->get_storage_length();
          results->InsertData(index, col, data + offset, len + BOOL_WIDE);
        } else {
          k_uint32 len = output_fields_[col]->get_storage_length();
          results->InsertData(index, col, data + offset, len);
        }
      }
      results->AddCount();
      ++index;
      iter_++;
      ++examined_rows_;
      ++total_read_row_;

      if (examined_rows_ % BATCH_SIZE == 0) {
        break;
      }
    }

    if (examined_rows_ == buckets_.size()) {
      is_done_ = true;
    }
  }
  Return(KStatus::SUCCESS);
}

BaseOperator* PostAggScanOperator::Clone() {
  BaseOperator* input = input_->Clone();
  // input_:TagScanOperator
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter = NewIterator<PostAggScanOperator>(*this, input, this->processor_id_);
  return iter;
}

KStatus PostAggScanOperator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;

  // all agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    k_int32 func_type = agg.func();

    // POST AGG SCAN :input column is same with output column
    k_uint32 argIdx = i;
    k_uint32 len = output_fields_[argIdx]->get_storage_length();

    unique_ptr<AggregateFunc> agg_func;
    switch (func_type) {
      case Sumfunctype::MAX: {
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<MaxAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MaxAggregate<k_int16>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MaxAggregate<k_int32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MaxAggregate<k_int64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MaxAggregate<k_float32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<MaxAggregate<k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MaxAggregate<std::string>>
                (i, argIdx, len + STRING_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for max aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::MIN: {
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique< MinAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MinAggregate<k_int16>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MinAggregate<k_int32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MinAggregate<k_int64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MinAggregate<k_float32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<MinAggregate<k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MinAggregate<std::string>>
                (i, argIdx, len + STRING_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for min aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        // create the Assembling rows of columns list (POST AGG SCAN）
        agg_source_target_col_map_[agg.col_idx(0)] = i;

        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique< AnyNotNullAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<AnyNotNullAggregate<k_int16>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<AnyNotNullAggregate<k_int32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<AnyNotNullAggregate<k_int64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<AnyNotNullAggregate<k_float32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<AnyNotNullAggregate<k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<AnyNotNullAggregate<std::string>>
                (i, argIdx, len + STRING_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for any_not_null aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::SUM: {
        LOG_DEBUG("SUM aggregations argument column : %u\n", argIdx);

        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<SumAggregate<k_int16, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<SumAggregate<k_int32, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<SumAggregate<k_int64, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<SumAggregate<k_float32, k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<SumAggregate<k_double64, k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DECIMAL:
            agg_func = make_unique<SumAggregate<k_decimal, k_decimal>>
                (i, argIdx, len + BOOL_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for sum aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::COUNT:
      case Sumfunctype::COUNT_ROWS: {
        // for post agg scan, needs to sum the results from Agg Scan OP.
        len = sizeof(k_int64);
        agg_func = make_unique<SumIntAggregate>(i, argIdx, len);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 tsIdx = argIdx + 1;

        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<LastAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<LastAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 tsIdx = argIdx;

        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = make_unique<LastTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 tsIdx = argIdx + 1;

        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<LastRowAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<LastRowAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 tsIdx = argIdx;
        len = sizeof(KTimestamp) * 2;

        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));

        agg_func = make_unique<LastRowTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 tsIdx = argIdx + 1;

        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<FirstAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<FirstAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 tsIdx = argIdx;
        len = sizeof(KTimestamp) * 2;

        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));
        agg_func = make_unique<FirstTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 tsIdx = argIdx + 1;

        output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());
        len = output_fields_[i]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<FirstRowAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<FirstRowAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 tsIdx = argIdx;

        len = sizeof(KTimestamp) * 2;
        output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        output_fields_[i]->set_storage_length(sizeof(KTimestamp));

        agg_func = make_unique<FirstRowTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::STDDEV: {
        len = sizeof(k_int64);
        agg_func = make_unique<STDDEVRowAggregate>(i, len);
        break;
      }
      case Sumfunctype::AVG: {
        len = sizeof(k_int64);
        agg_func = make_unique<AVGRowAggregate<k_int64>>(i, argIdx, len);
        break;
      }
      default:
      LOG_ERROR("unknown aggregation function type %d\n", func_type);
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      agg_func->SetOffset(func_offsets_[argIdx]);
      funcs_.push_back(std::move(agg_func));
    }
  }

  Return(status);
}

void PostAggScanOperator::CalculateAggOffsets() {
  if (output_fields_.empty()) {
    return;
  }
  func_offsets_.resize(output_fields_.size());
  k_uint32 offset = 0;
  for (int i = 0; i < output_fields_.size(); i++) {
    func_offsets_[i] = offset;
    if (IsStringType(output_fields_[i]->get_storage_type())) {
      offset += STRING_WIDE;
    } else if (output_fields_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
      offset += BOOL_WIDE;
    }

    if (IsFirstLastAggFunc(aggregations_[i].func())) {
        offset += sizeof(KTimestamp);
    }
    offset += output_fields_[i]->get_storage_length();
  }
}

void PostAggScanOperator::ResolveGroupByCols(kwdbContext_p ctx) {
  k_uint32 group_size_ = spec_->group_cols_size();
  for (k_int32 i = 0; i < group_size_; ++i) {
    k_uint32 groupcol = spec_->group_cols(i);
    group_cols_.push_back(agg_source_target_col_map_[groupcol]);
  }
}

}  // namespace kwdbts
