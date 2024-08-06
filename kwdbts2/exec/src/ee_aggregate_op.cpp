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

#include <variant>
#include <chrono>
#include "ee_aggregate_op.h"
#include "cm_func.h"
#include "ee_kwthd.h"
#include "ee_pb_plan.pb.h"
#include "lg_api.h"
#include "ee_common.h"

namespace kwdbts {

BaseAggregator::BaseAggregator(BaseOperator* input, TSAggregatorSpec* spec, TSPostProcessSpec* post,
                                TABLE* table, int32_t processor_id)
    : BaseOperator(table, processor_id),
      spec_{spec},
      post_{post},
      param_(input, spec, post, table, this),
      group_type_(spec->type()),
      input_{input},
      offset_(post->offset()),
      limit_(post->limit()),
      input_fields_{input->OutputFields()} {
  for (k_int32 i = 0; i < spec_->aggregations_size(); ++i) {
    aggregations_.push_back(spec_->aggregations(i));
  }
}

BaseAggregator::BaseAggregator(const BaseAggregator& other, BaseOperator* input, int32_t processor_id)
    : BaseOperator(other.table_, processor_id),
      spec_(other.spec_),
      post_(other.post_),
      param_(input, other.spec_, other.post_, other.table_, this),
      group_type_(other.spec_->type()),
      input_{input},
      offset_(other.offset_),
      limit_(other.limit_),
      input_fields_{input->OutputFields()} {
  for (k_int32 i = 0; i < other.spec_->aggregations_size(); ++i) {
    aggregations_.push_back(spec_->aggregations(i));
  }
  is_clone_ = true;
}

BaseAggregator::~BaseAggregator() {}

KStatus BaseAggregator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;

  // all agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    unique_ptr<AggregateFunc> agg_func;

    k_int32 func_type = agg.func();
    switch (func_type) {
      case Sumfunctype::MAX: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = input_fields_[argIdx]->get_storage_length();

        switch (input_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<MaxAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MaxAggregate<k_int16>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MaxAggregate<k_int32>>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MaxAggregate<k_int64>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MaxAggregate<k_float32>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<MaxAggregate<k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MaxAggregate<std::string>>(i, argIdx, len + STRING_WIDE);
            break;
          case roachpb::DataType::DECIMAL:
            agg_func = make_unique<MaxAggregate<k_decimal>>(i, argIdx, len + BOOL_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for max aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::MIN: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = input_fields_[argIdx]->get_storage_length();

        switch (input_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<MinAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MinAggregate<k_int16>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MinAggregate<k_int32>>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MinAggregate<k_int64>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MinAggregate<k_float32>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<MinAggregate<k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MinAggregate<std::string>>(i, argIdx, len + STRING_WIDE);
            break;
          case roachpb::DataType::DECIMAL:
            agg_func = make_unique<MinAggregate<k_decimal>>(i, argIdx, len + BOOL_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for min aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = input_fields_[argIdx]->get_storage_length();

        switch (input_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<AnyNotNullAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func =
                make_unique<AnyNotNullAggregate<k_int16>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func =
                make_unique<AnyNotNullAggregate<k_int32>>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func =
                make_unique<AnyNotNullAggregate<k_int64>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func =
                make_unique<AnyNotNullAggregate<k_float32>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func =
                make_unique<AnyNotNullAggregate<k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func =
                make_unique<AnyNotNullAggregate<std::string>>(i, argIdx, len + STRING_WIDE);
            break;
          case roachpb::DataType::DECIMAL:
            agg_func =
                make_unique<AnyNotNullAggregate<k_decimal>>(i, argIdx, len + BOOL_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for any_not_null aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::SUM: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = param_.aggs_[i]->get_storage_length();

        switch (input_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<SumAggregate<k_int16, k_decimal>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<SumAggregate<k_int32, k_decimal>>(i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<SumAggregate<k_int64, k_decimal>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<SumAggregate<k_float32, k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<SumAggregate<k_double64, k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::DECIMAL:
            agg_func = make_unique<SumAggregate<k_decimal, k_decimal>>(i, argIdx, len + BOOL_WIDE);
            break;
          default:
          LOG_ERROR("unsupported data type for sum aggregation\n");
            status = KStatus::FAIL;
            break;
        }

        break;
      }
      case Sumfunctype::SUM_INT: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<SumIntAggregate>(i, argIdx, len);

        break;
      }
      case Sumfunctype::COUNT: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<CountAggregate>(i, argIdx, len);

        break;
      }
      case Sumfunctype::COUNT_ROWS: {
        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<CountRowAggregate>(i, len);

        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);

        k_uint32 len = input_fields_[argIdx]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(param_.aggs_[i]->get_storage_type())) {
          agg_func = make_unique<LastAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
          agg_func = make_unique<LastAggregate>(i, argIdx, tsIdx, len + BOOL_WIDE);
        } else {
          agg_func = make_unique<LastAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }

        k_uint32 len = input_fields_[tsIdx]->get_storage_length() + sizeof(KTimestamp);
        agg_func = make_unique<LastTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);

        k_uint32 len = input_fields_[argIdx]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(param_.aggs_[i]->get_storage_type())) {
          agg_func = make_unique<LastRowAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
          agg_func = make_unique<LastRowAggregate>(i, argIdx, tsIdx, len + BOOL_WIDE);
        } else {
          agg_func = make_unique<LastRowAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }

        k_uint32 len = input_fields_[tsIdx]->get_storage_length() + sizeof(KTimestamp);
        agg_func = make_unique<LastRowTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);

        k_uint32 len = input_fields_[argIdx]->get_storage_length() + sizeof(KTimestamp);

        if (IsStringType(param_.aggs_[i]->get_storage_type())) {
          agg_func = make_unique<FirstAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
          agg_func = make_unique<FirstAggregate>(i, argIdx, tsIdx, len + BOOL_WIDE);
        } else {
          agg_func = make_unique<FirstAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }

        k_uint32 len = input_fields_[tsIdx]->get_storage_length() + sizeof(KTimestamp);
        agg_func = make_unique<FirstTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(1);

        k_uint32 len = input_fields_[argIdx]->get_storage_length() + sizeof(KTimestamp);
        if (IsStringType(param_.aggs_[i]->get_storage_type())) {
          agg_func = make_unique<FirstRowAggregate>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
          agg_func = make_unique<FirstRowAggregate>(i, argIdx, tsIdx, len + BOOL_WIDE);
        } else {
          agg_func = make_unique<FirstRowAggregate>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 argIdx = agg.col_idx(0);
        k_uint32 tsIdx = agg.col_idx(0);
        if (agg.col_idx_size() > 1) {
          tsIdx = agg.col_idx(1);
        }

        k_uint32 len = input_fields_[tsIdx]->get_storage_length() + sizeof(KTimestamp);

        agg_func = make_unique<FirstRowTSAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::STDDEV: {
        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<STDDEVRowAggregate>(i, len);
        break;
      }
      case Sumfunctype::AVG: {
        k_uint32 argIdx = agg.col_idx(0);

        k_uint32 len = param_.aggs_[i]->get_storage_length();
        switch (input_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<AVGRowAggregate<k_int16>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<AVGRowAggregate<k_int32>>(i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<AVGRowAggregate<k_int64>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<AVGRowAggregate<k_float32>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<AVGRowAggregate<k_double64>>(i, argIdx, len);
            break;
          // case roachpb::DataType::DECIMAL:
          //   agg_func = make_unique<AVGRowAggregate<k_decimal>>(i, argIdx, len);
          //   break;
          default:
          LOG_ERROR("unsupported data type for sum aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      default:
      LOG_ERROR("unknonw aggregation function type %d\n", func_type);
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      agg_func->SetOffset(func_offsets_[i]);
      param_.aggs_[i]->set_column_offset(func_offsets_[i]);
      funcs_.push_back(std::move(agg_func));
    }
  }

  Return(status);
}

EEIteratorErrCode BaseAggregator::PreInit(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  do {
    // init subquery iterator
    code = input_->PreInit(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    // resolve renders num
    param_.RenderSize(ctx, &num_);

    // resolve render
    code = param_.ResolveRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveRender() error\n");
      break;
    }

    // resolve having
    code = param_.ResolveFilter(ctx, &having_filter_, false);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("Resolve having clause error\n");
      break;
    }

    // dispose Output Fields
    code = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // calculate the offset of the aggregation result in the bucket
    CalculateAggOffsets();

    // dispose Agg func
    KStatus ret = ResolveAggFuncs(ctx);
    if (ret != KStatus::SUCCESS) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }

    // dispose Group By
    ResolveGroupByCols(ctx);

    // calculate Agg wideth
    for (int i = 0; i < param_.aggs_size_; i++) {
      agg_row_size_ += param_.aggs_[i]->get_storage_length();
      if (IsStringType(param_.aggs_[i]->get_storage_type())) {
        agg_row_size_ += STRING_WIDE;
      } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
        agg_row_size_ += BOOL_WIDE;
      } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
        agg_row_size_ += sizeof(k_int64);
      }

      if (IsFirstLastAggFunc(aggregations_[i].func())) {
        agg_row_size_ += sizeof(KTimestamp);
      }
    }
    agg_null_offset_ = agg_row_size_;
    agg_row_size_ += (param_.aggs_size_ + 7) / 8;

    for (auto field : input_fields_) {
      data_types_.push_back(field->get_storage_type());
    }
  } while (0);

  Return(code);
}

void BaseAggregator::ResolveGroupByCols(kwdbContext_p ctx) {
  k_uint32 group_size_ = spec_->group_cols_size();
  for (k_int32 i = 0; i < group_size_; ++i) {
    k_uint32 groupcol = spec_->group_cols(i);
    group_cols_.push_back(groupcol);
  }
}

void BaseAggregator::CalculateAggOffsets() {
  if (param_.aggs_size_ < 1) {
    return;
  }

  func_offsets_.resize(param_.aggs_size_);
  k_uint32 offset = 0;
  for (int i = 0; i < param_.aggs_size_; i++) {
    func_offsets_[i] = offset;
    if (IsStringType(param_.aggs_[i]->get_storage_type())) {
      offset += STRING_WIDE;
    } else if (param_.aggs_[i]->get_storage_type() == roachpb::DataType::DECIMAL) {
      offset += BOOL_WIDE;
    } else if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG) {
      offset += sizeof(k_int64);
    }

    if (IsFirstLastAggFunc(aggregations_[i].func())) {
      offset += sizeof(KTimestamp);
    }
    offset += param_.aggs_[i]->get_storage_length();
  }
}

void BaseAggregator::InitFirstLastTimeStamp(DatumRowPtr ptr) {
  for (int i = 0; i < aggregations_.size(); i++) {
    auto func_type = aggregations_[i].func();
    k_uint32 offset = funcs_[i]->GetOffset();
    k_uint32 len = funcs_[i]->GetLen();

    if (func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW ||
        func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS) {
      KTimestamp max_ts = INT64_MAX;
      std::memcpy(ptr + offset + len - sizeof(KTimestamp), &max_ts, sizeof(KTimestamp));
    } else if (func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW ||
        func_type == TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS) {
      KTimestamp min_ts = INT64_MIN;
      std::memcpy(ptr + offset + len - sizeof(KTimestamp), &min_ts, sizeof(KTimestamp));
    }
  }
}

KStatus BaseAggregator::Close(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = input_->Close(ctx);
  Reset(ctx);

  Return(ret);
}

EEIteratorErrCode BaseAggregator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  input_->Reset(ctx);
  Return(EEIteratorErrCode::EE_OK);
}

KStatus BaseAggregator::accumulateRowIntoBucket(kwdbContext_p ctx, DatumRowPtr bucket, k_uint32 agg_null_offset,
                                                DataChunkPtr& chunk, k_uint32 line) {
  EnterFunc();
  for (int i = 0; i < funcs_.size(); i++) {
    // distinct
    const auto& agg = aggregations_[i];

    // Distinct Agg
    if (agg.distinct()) {
      // group cols + agg cols
      if (!funcs_[i]->isDistinct(chunk, line, data_types_, group_cols_)) {
        continue;
      }
    }

    // execute agg
    funcs_[i]->addOrUpdate(bucket, bucket + agg_null_offset, chunk, line);
  }

  Return(KStatus::SUCCESS);
}

///////////////// HashAggregateOperator //////////////////////

HashAggregateOperator::HashAggregateOperator(BaseOperator* input,
                                             TSAggregatorSpec* spec,
                                             TSPostProcessSpec* post,
                                             TABLE* table,
                                             int32_t processor_id)
    : BaseAggregator(input, spec, post, table, processor_id) {}

HashAggregateOperator::HashAggregateOperator(const HashAggregateOperator& other,
                                              BaseOperator* input,
                                              int32_t processor_id)
    : BaseAggregator(other, input, processor_id) {}

HashAggregateOperator::~HashAggregateOperator() {
  //  delete input
  if (is_clone_) {
    delete input_;
  }

  for (auto& bucket : buckets_) {
    SafeDeleteArray(bucket.second);
  }
}

EEIteratorErrCode HashAggregateOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  // set current offset
  cur_offset_ = offset_;

  code = input_->Init(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // aggregation calculation for all data from sub operators
  KStatus ret = accumulateRows(ctx);
  if (ret != KStatus::SUCCESS) {
    code = EEIteratorErrCode::EE_ERROR;
  }

  iter_ = buckets_.begin();
  Return(code);
}

EEIteratorErrCode HashAggregateOperator::Next(kwdbContext_p ctx,
                                              DataChunkPtr& chunk) {
  EnterFunc();
  if (is_done_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }
  if (nullptr == chunk) {
    // init data chunk
    std::vector<ColumnInfo> col_info;
    col_info.reserve(output_fields_.size());
    for (auto field : output_fields_) {
      col_info.emplace_back(field->get_storage_length(),
                            field->get_storage_type(),
                            field->get_return_type());
    }
    chunk = std::make_unique<DataChunk>(col_info);
    if (chunk->Initialize() < 0) {
      chunk = nullptr;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  // write aggdata to result_set
  getAggResults(ctx, chunk);
  Return(EEIteratorErrCode::EE_OK);
}

KStatus HashAggregateOperator::accumulateBatch(kwdbContext_p ctx,
                                               DataChunkPtr& chunk) {
  EnterFunc();

  std::vector<DatumRowPtr> dest_ptrs;
  dest_ptrs.reserve(chunk->Count());

  for (k_uint32 line = 0; line < chunk->Count(); ++line) {
    CombinedGroupKey group_keys;
    AggregateFunc::ConstructGroupKeys(chunk, group_cols_, data_types_, line,
                                      group_keys);

    if (buckets_.find(group_keys) == buckets_.end()) {
      // create new bucket
      auto ptr = KNEW char[agg_row_size_];
      std::memset(ptr, 0, agg_row_size_);
      InitFirstLastTimeStamp(ptr);
      buckets_[group_keys] = ptr;
    }

    dest_ptrs[line] = buckets_[group_keys];
  }

  for (int i = 0; i < funcs_.size(); i++) {
    // call agg func
    DistinctOpt opt{aggregations_[i].distinct(), data_types_, group_cols_};
    funcs_[i]->AddOrUpdate(chunk, dest_ptrs, agg_null_offset_, opt);
  }
  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::accumulateRow(kwdbContext_p ctx, DataChunkPtr& chunk, k_uint32 line) {
  EnterFunc();

  // groupby col
  CombinedGroupKey group_keys;
  AggregateFunc::ConstructGroupKeys(chunk, group_cols_, data_types_, line, group_keys);

  if (buckets_.find(group_keys) == buckets_.end()) {
    // create new bucket
    auto ptr = KNEW char[agg_row_size_];
    std::memset(ptr, 0, agg_row_size_);
    InitFirstLastTimeStamp(ptr);
    buckets_[group_keys] = ptr;
  }

  accumulateRowIntoBucket(ctx, buckets_[group_keys], agg_null_offset_, chunk, line);

  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::accumulateRows(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  int64_t duration = 0;
  int64_t read_row_num = 0;
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

    // no data ,read??
    if (chunk->Count() == 0) {
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
    // loop through each row
    for (k_uint32 line = 0; line < chunk->Count(); ++line) {
      accumulateRow(ctx, chunk, line);
    }
    // accumulateBatch(ctx, chunk);
    auto end = std::chrono::high_resolution_clock::now();

    if (fetchers != nullptr && fetchers->collected) {
      std::chrono::duration<int64_t, std::nano> t = end - start;
      duration += t.count();
    }
  }
  analyseFetcher(ctx, this->processor_id_, duration, read_row_num, 0,
                 sizeof(aggregations_)+sizeof(kwdbts::TSAggregatorSpec_Aggregation) * aggregations_.size(), 1, 0);

  // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
  // aggregated.
  if (buckets_.empty() && group_cols_.empty()) {
    // retrun NULL
    CombinedGroupKey group_keys;
    group_keys.Reserve(1);
    group_keys.AddGroupKey(std::monostate(), roachpb::DataType::UNKNOWN);

    auto ptr = KNEW char[agg_row_size_];
    std::memset(ptr, 0, agg_row_size_);

    // return 0
    for (int i = 0; i < aggregations_.size(); i++) {
      if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT) {
        // set no null, default 0
        AggregateFunc::SetNotNull(ptr + agg_null_offset_, i);
      }
    }

    buckets_[group_keys] = ptr;
  }

  Return(KStatus::SUCCESS);
}

KStatus HashAggregateOperator::getAggResults(kwdbContext_p ctx,
                                             DataChunkPtr& results) {
  EnterFunc();
  k_uint32 BATCH_SIZE = results->Capacity();

  auto start = std::chrono::high_resolution_clock::now();
  // row indicates indicates the row position inserted into the current
  // DataChunk
  k_uint32 row = 0;
  while (total_read_row_ < buckets_.size()) {
    // filter
    if (nullptr != having_filter_) {
      k_int64 ret = having_filter_->ValInt();
      if (0 == ret) {
        iter_++;
        ++total_read_row_;
        continue;
      }
    }

    // limit
    if (limit_ && examined_rows_ >= limit_) {
      is_done_ = true;
      break;
    }

    // offset
    if (cur_offset_ > 0) {
      --cur_offset_;
      iter_++;
      ++total_read_row_;
      continue;
    }

    FieldsToChunk(GetRender(), GetRenderSize(), row, results);

    iter_++;
    ++examined_rows_;
    ++total_read_row_;
    results->AddCount();
    ++row;

    if (examined_rows_ % BATCH_SIZE == 0) {
      // BATCH_SIZE
      row = 0;
      break;
    }
  }
  auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
  if (fetchers != nullptr && fetchers->collected) {
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<int64_t, std::nano> duration = end - start;
    analyseFetcher(ctx, this->processor_id_, duration.count(), 0, 0,
                   0, 0, examined_rows_);
  }

  if (total_read_row_ == buckets_.size()) {
    is_done_ = true;
  }
  Return(KStatus::SUCCESS);
}

BaseOperator* HashAggregateOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }
  BaseOperator* iter = NewIterator<HashAggregateOperator>(*this, input, this->processor_id_);
  return iter;
}


///////////////// OrderedAggregateOperator //////////////////////

OrderedAggregateOperator::OrderedAggregateOperator(BaseOperator* input,
                                                   TSAggregatorSpec* spec,
                                                   TSPostProcessSpec* post,
                                                   TABLE* table,
                                                   int32_t processor_id)
    : BaseAggregator(input, spec, post, table, processor_id) {}

OrderedAggregateOperator::OrderedAggregateOperator(const OrderedAggregateOperator& other,
                                                    BaseOperator* input,
                                                    int32_t processor_id)
    : BaseAggregator(other, input, processor_id) {}

OrderedAggregateOperator::~OrderedAggregateOperator() {
  //  delete input
  if (is_clone_) {
    delete input_;
  }

  SafeDeleteArray(bucket_);
}

EEIteratorErrCode OrderedAggregateOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  code = input_->Init(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  bucket_ = KNEW char[agg_row_size_];
  std::memset(bucket_, 0, agg_row_size_);
  InitFirstLastTimeStamp(bucket_);

  Return(code);
}

EEIteratorErrCode OrderedAggregateOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  if (is_done_) {
    Return(EEIteratorErrCode::EE_END_OF_RECORD);
  }

  if (nullptr == chunk) {
    // init data chunk
    std::vector<ColumnInfo> col_info;
    col_info.reserve(output_fields_.size());
    for (auto field : output_fields_) {
      col_info.emplace_back(field->get_storage_length(), field->get_storage_type(), field->get_return_type());
    }
    chunk = std::make_unique<DataChunk>(col_info);
    if (chunk->Initialize() < 0) {
      chunk = nullptr;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  KStatus ret = accumulateRows(ctx, chunk);
  if (ret != KStatus::SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

KStatus OrderedAggregateOperator::getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk) {
  // Having filter
  k_int64 keep = 1;
  if (nullptr != having_filter_) {
    keep = having_filter_->ValInt();
  }

  while (keep) {
    // limit
    if (limit_ && examined_rows_ >= limit_) {
      is_done_ = true;
      break;
    }

    if (cur_offset_ > 0) {
      --cur_offset_;
      break;
    }

    chunk->AddCount();
    k_int32 row = chunk->NextLine();
    if (row < 0) {
      return KStatus::FAIL;
    }
    // insert one row into data chunk
    FieldsToChunk(GetRender(), GetRenderSize(), row, chunk);
    ++examined_rows_;

    keep = 0;
  }

  std::memset(bucket_, 0, agg_row_size_);
  has_agg_result = false;

  return KStatus::SUCCESS;
}

KStatus OrderedAggregateOperator::accumulateRows(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  int64_t duration = 0;
  int64_t read_row_num = 0;

  for (;;) {
    if (chunk_ == nullptr) {
      // batch_ is null，read a batch of data from sub operator
      code = input_->Next(ctx, chunk_);
      if (code != EEIteratorErrCode::EE_OK) {
        if (code == EEIteratorErrCode::EE_END_OF_RECORD ||
            code == EEIteratorErrCode::EE_TIMESLICE_OUT) {
          is_done_ = true;
          code = EEIteratorErrCode::EE_OK;
          break;
        }
        LOG_ERROR("Failed to fetch data from child operator, return code = %d.\n", code);
        Return(KStatus::FAIL);
      }

      // no data,continue
      if (chunk_->Count() == 0) {
        chunk_ = nullptr;
        continue;
      }
    }

    auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
    if (fetchers != nullptr && fetchers->collected) {
      goLock(fetchers->goMutux);
      chunk_->GetFvec().GetAnalyse(ctx);
      goUnLock(fetchers->goMutux);
      // analyse collection
      read_row_num += chunk_->Count();
    }

    auto start = std::chrono::high_resolution_clock::now();

    // dispose data
    while (processed_rows < chunk_->Count()) {
      // groupby col
      CombinedGroupKey group_keys;
      AggregateFunc::ConstructGroupKeys(chunk_, group_cols_, data_types_, processed_rows, group_keys);

      if (!(group_keys == last_group_key_)) {
        // bucket_ write to chunk
        if (has_agg_result) {
          if (getAggResult(ctx, chunk) != KStatus::SUCCESS) {
            Return(KStatus::FAIL);
          }
          // chunk is full
          if (chunk->Count() == chunk->Capacity()) {
            Return(KStatus::SUCCESS);
          }
        }

        // set new group key
        last_group_key_ = group_keys;
      }

      accumulateRowIntoBucket(ctx, bucket_, agg_null_offset_, chunk_, processed_rows);
      has_agg_result = true;
      ++processed_rows;
    }
    auto end = std::chrono::high_resolution_clock::now();

    if (fetchers != nullptr && fetchers->collected) {
      std::chrono::duration<int64_t, std::nano> t = end - start;
      duration += t.count();
    }

    // batch_ read finish，read next batch of data
    chunk_ = nullptr;
    processed_rows = 0;
  }

  // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
  auto all_start = std::chrono::high_resolution_clock::now();
  if (!has_agg_result && group_type_ == TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR) {
    // return NULL
    CombinedGroupKey group_keys;
    group_keys.Reserve(1);
    group_keys.AddGroupKey(std::monostate(), roachpb::DataType::UNKNOWN);

    // return 0, if agg type is COUNT_ROW or COUNT
    for (int i = 0; i < aggregations_.size(); i++) {
      if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT ||
          aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT) {
        // set not null ,default 0
        AggregateFunc::SetNotNull(bucket_ + agg_null_offset_, i);
      }
    }
    has_agg_result = true;
  }

  // dispos the result of last agg
  if (has_agg_result) {
    if (getAggResult(ctx, chunk) != KStatus::SUCCESS) {
      Return(KStatus::FAIL);
    }
    // chunk is full
    if (chunk->Count() == chunk->Capacity()) {
      Return(KStatus::SUCCESS);
    }
  }
  auto all_end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<int64_t, std::nano> all_t = all_end - all_start;
  duration += all_t.count();
  analyseFetcher(ctx, this->processor_id_, duration, read_row_num, 0, 0, 1, chunk->Count());

  Return(KStatus::SUCCESS);
}

BaseOperator* OrderedAggregateOperator::Clone() {
  BaseOperator* input = input_->Clone();
  if (input == nullptr) {
    input = input_;
  }

  BaseOperator* iter = NewIterator<OrderedAggregateOperator>(*this, input, processor_id_);
  return iter;
}

}  // namespace kwdbts
