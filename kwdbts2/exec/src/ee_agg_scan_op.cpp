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

#include "ee_agg_scan_op.h"

#include <memory>
#include <string>
#include <vector>

#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_kwthd_context.h"
#include "ee_aggregate_flow_spec.h"
#include "ee_pb_plan.pb.h"
#include "ee_scan_row_batch.h"
#include "ee_common.h"

namespace kwdbts {

EEIteratorErrCode AggTableScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode ret;
  do {
    ret = TableScanOperator::Init(ctx);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("RenderSize() failed\n");
      break;
    }

    agg_source_target_col_map_.resize(num_);

    // extract from agg spec
    auto agg_param_ = AggregatorSpecParam<TSAggregatorSpec>(this,
                                                            const_cast<TSAggregatorSpec*>(&aggregation_spec_),
                                                            const_cast<TSPostProcessSpec*>(&aggregation_post_),
                                                            table_);
    // get the size of renders
    agg_param_.RenderSize(ctx, &agg_num_);

    // resolve renders
    ret = agg_param_.ResolveRender(ctx, &agg_renders_, agg_num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveRender() failed\n");
      break;
    }

    // resolve output type (return type)
    ret = agg_param_.ResolveOutputType(ctx, agg_renders_, agg_num_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputType() failed\n");
      break;
    }

    // resolve output Field
    ret = agg_param_.ResolveOutputFields(ctx, agg_renders_, agg_num_, agg_output_fields_);
    if (ret != EEIteratorErrCode::EE_OK) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    extractTimeBucket(renders_, num_);
    if (interval_seconds_ == 0) {
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    ResolveAggFuncs(ctx);

    // construct the output column information for agg output.
    agg_output_col_info.reserve(agg_output_fields_.size());
    for (auto field : agg_output_fields_) {
      agg_output_col_info.emplace_back(field->get_storage_length(), field->get_storage_type(),
                                       field->get_return_type());
    }
    constructAggResults();
    if (agg_results_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }

    for (int i = 0; i < num_; ++i) {
      data_types_.push_back(renders_[i]->get_storage_type());
    }

    source.resize(num_);
  } while (0);
  Return(ret);
}

EEIteratorErrCode AggTableScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext* thd = current_thd;
  StorageHandler* handler = handler_;

  do {
    if (limit_ && examined_rows_ >= limit_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      is_done_ = true;
      break;
    }

    ScanRowBatchPtr data_handle;
    code = InitScanRowBatch(ctx, &data_handle);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // read data
    while (!is_done_) {
      code = handler->TsNext(ctx);
      if (EEIteratorErrCode::EE_OK != code) {
        is_done_ = true;
        break;
      }
      if (data_handle->count_ < 1) continue;
      total_read_row_ += data_handle->count_;

      if (nullptr == filter_) {
        examined_rows_ += data_handle->count_;
        break;
      }

      // filter
      for (int i = 0; i < data_handle->count_; ++i) {
        if (nullptr != filter_) {
          k_int64 ret = filter_->ValInt();
          if (0 == ret) {
            data_handle->NextLine();
            continue;
          }
        }

        data_handle->AddSelection();
        data_handle->NextLine();
        ++examined_rows_;
      }

      if (!data_handle->GetSelection()->empty()) {
        break;
      }
    }

    if (!is_done_) {
      // If the result set is unordered, it is necessary to perform secondary
      // HASH aggregation on the basis of AGG SCAN.
      if (handler->isDisorderedMetrics()) {
        agg_results_->setPassAgg(false);
      }
      // reset
      data_handle->ResetLine();
      if (data_handle->Count() > 0) {
        KStatus status = AddRowBatchData(ctx, data_handle.get(), renders_);

        if (status != KStatus::SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
      }
    } else {
      if (agg_results_ != nullptr) {
        agg_output_queue.push(std::move(agg_results_));
      }
    }
  } while (!is_done_ && agg_output_queue.empty());

  if (!agg_output_queue.empty()) {
    chunk = std::move(agg_output_queue.front());
    agg_output_queue.pop();
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      Return(EEIteratorErrCode::EE_OK);
    } else {
      Return(code);
    }
  } else {
    if (is_done_) {
      Return(EEIteratorErrCode::EE_END_OF_RECORD);
    } else {
      Return(code)
    }
  }
}

char* AggTableScanOperator::GetFieldDataPtr(Field* field, RowBatch* row_batch) {
  if (field->get_field_type() == Field::Type::FIELD_ITEM) {
    k_uint32 col_idx_in_rs = field->getColIdxInRs();
    k_uint32 storage_len = field->get_storage_length();
    roachpb::DataType storage_type = field->get_storage_type();
    roachpb::KWDBKTSColumn::ColumnType column_type = field->get_column_type();

    return static_cast<char *>(
      row_batch->GetData(col_idx_in_rs, 0 == field->get_num() ? storage_len + 8 : storage_len,
                          column_type, storage_type));
  }

  return field->get_ptr();
}

// TODO(liuwei): handle limit, offset and filter.
KStatus AggTableScanOperator::AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch, Field** renders) {
  EnterFunc();
  if (row_batch == nullptr) {
    Return(KStatus::FAIL);
  }

  k_int32 target_row = ((k_int32) agg_results_->Count()) - 1;
  k_uint32 start_line = 0, end_line = 0;
  k_uint32 row_batch_count = row_batch->Count();
  // cout << "row_batch->Count(): " << row_batch_count << endl;

  for (auto arg_idx : arg_idx_vec) {
    source[arg_idx].resize(row_batch_count);
  }

  for (k_uint32 row = 0; row < row_batch_count; ++row) {
    // check all the group by column, and increase the target_row number if it finds a different group.
    std::vector<GroupByColumnInfo> group_by_cols;
    group_by_cols.reserve(group_cols_.size());

    KTimestampTz time_bucket = 0;
    bool is_new_group = ProcessGroupCols(target_row, row_batch, row,
                                        group_by_cols, time_bucket, source);

    // new group or end of rowbatch
    if (is_new_group) {
      // process agg funcs/columns
      if (target_row >= 0 && end_line > start_line) {
        for (auto& func : funcs_) {
          func->addOrUpdate(agg_results_, target_row, source, start_line, end_line);
        }
      }

      if (agg_results_->isFull()) {
        agg_output_queue.push(std::move(agg_results_));
        // initialize a new agg result buffer.
        constructAggResults();
        if (agg_results_ == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          Return(KStatus::FAIL);
        }
        target_row = -1;
      }

      ++target_row;
      agg_results_->AddCount();
      for (int j = 0; j < group_cols_.size(); j++) {
        auto& col = group_by_cols[j];
        agg_results_->InsertData(target_row, col.col_index, col.data_ptr, col.len);
      }

      // start/end position for next batch
      start_line = row;
      end_line = row;
    }

    row_batch->NextLine();
    ++end_line;
  }

  // last batch
  if (end_line > start_line) {
    for (auto& func : funcs_) {
      func->addOrUpdate(agg_results_, target_row, source, start_line, end_line);
    }
  }

  start_line = 0;
  end_line = 0;

  Return(KStatus::SUCCESS);
}

k_bool AggTableScanOperator::ProcessGroupCols(k_int32& target_row, RowBatch* row_batch, k_uint32 row,
                          std::vector<GroupByColumnInfo>& group_by_cols, KTimestampTz& time_bucket,
                          std::vector<std::vector<DataSource>>& source) {
  bool is_new_group = false;
  k_uint32 current_line = target_row <= 0 ? 0 : target_row;

  for (auto arg_idx : arg_idx_vec) {
    auto* field = GetRender(arg_idx);

    source[arg_idx][row].src_ptr = GetFieldDataPtr(field, row_batch);
    source[arg_idx][row].is_null = field->isNullable() && field->is_nullable();

    auto storage_field = field->get_storage_type();
    if (storage_field == roachpb::DataType::VARBINARY ||
        storage_field == roachpb::DataType::NVARCHAR) {
      source[arg_idx][row].str_length = field->ValStrLength(source[arg_idx][row].src_ptr);
    }
  }

  for (k_uint32 col : group_cols_) {
    k_uint32 target_col = agg_source_target_col_map_[col];

    // maybe first row in group
    bool is_dest_null = agg_results_->IsNull(current_line, target_col);
    auto target_ptr = agg_results_->GetData(current_line, target_col);

    auto* field = GetRender(col);
    // auto* source_ptr = field->get_ptr();
    auto* source_ptr = GetFieldDataPtr(field, row_batch);

    // handle the null value in input data.
    if (field->isNullable() && field->is_nullable()) {
      continue;
    }

    switch (field->get_storage_type()) {
      case roachpb::DataType::BOOL: {
        processGroupByColumn<bool>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        // TODO(liuwei): k_uint64\k_int64
        if (col == col_idx_) {
          time_bucket = construct(field);
          source_ptr = reinterpret_cast<char*>(&time_bucket);
        }
        processGroupByColumn<k_int64>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::INT: {
        processGroupByColumn<k_int32>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::SMALLINT: {
        processGroupByColumn<k_int16>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::FLOAT: {
        processGroupByColumn<k_float32>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::DOUBLE: {
        processGroupByColumn<k_float64>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::VARBINARY: {
        processGroupByColumn<std::string>(source_ptr, target_ptr, target_col, is_dest_null, group_by_cols, is_new_group);
        break;
      }
      default: {
        break;
      }
    }
  }

  return is_new_group;
}

KStatus AggTableScanOperator::ResolveAggFuncs(kwdbContext_p ctx) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;

  // dispose agg func
  for (int i = 0; i < aggregations_.size(); ++i) {
    const auto& agg = aggregations_[i];
    k_int32 func_type = agg.func();
    k_uint32 argIdx;
    if (agg.col_idx_size() > 0) {
      argIdx = agg.col_idx(0);
    }

    for (int i = 0; i < agg.col_idx_size(); i++) {
      k_uint32 agg_arg_idx  = agg.col_idx(i);
      if (std::find(arg_idx_vec.begin(), arg_idx_vec.end(), agg_arg_idx)
            == arg_idx_vec.end()) {
        arg_idx_vec.push_back(agg_arg_idx);
      }
    }

    unique_ptr<ScanAggregateFunc> agg_func;
    switch (func_type) {
      case Sumfunctype::MAX: {
        k_uint32 len = output_fields_[argIdx]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<MaxScanAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MaxScanAggregate<k_int16>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MaxScanAggregate<k_int32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MaxScanAggregate<k_int64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MaxScanAggregate<k_float32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<MaxScanAggregate<k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MaxScanAggregate<std::string>>
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
        k_uint32 len = output_fields_[argIdx]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<MinScanAggregate<k_bool>>(i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<MinScanAggregate<k_int16>>(i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<MinScanAggregate<k_int32>>(i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<MinScanAggregate<k_int64>>(i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<MinScanAggregate<k_float32>>(i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func =
                make_unique<MinScanAggregate<k_double64>>(i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<MinScanAggregate<std::string>>(
                i, argIdx, len + STRING_WIDE);
            break;
          default:
            LOG_ERROR("unsupported data type for min aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::ANY_NOT_NULL: {
        agg_source_target_col_map_[argIdx] = i;
        k_uint32 len = output_fields_[argIdx]->get_storage_length();

        // skip to construct the ANY_NOT_NULL func for time_bucket column and group by columns.
        if (col_idx_ == argIdx) {
          break;
        }

        if (std::find(group_cols_.begin(), group_cols_.end(), argIdx)
            != group_cols_.end()) {
          break;
        }

        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::BOOL:
            agg_func = make_unique<AnyNotNullScanAggregate<k_bool>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<AnyNotNullScanAggregate<k_int16>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<AnyNotNullScanAggregate<k_int32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<AnyNotNullScanAggregate<k_int64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<AnyNotNullScanAggregate<k_float32>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<AnyNotNullScanAggregate<k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY:
            agg_func = make_unique<AnyNotNullScanAggregate<std::string>>
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
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        switch (output_fields_[argIdx]->get_storage_type()) {
          case roachpb::DataType::SMALLINT:
            agg_func = make_unique<SumScanAggregate<k_int16, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::INT:
            agg_func = make_unique<SumScanAggregate<k_int32, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::BIGINT:
            agg_func = make_unique<SumScanAggregate<k_int64, k_decimal>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::FLOAT:
            agg_func = make_unique<SumScanAggregate<k_float32, k_double64>>
                (i, argIdx, len);
            break;
          case roachpb::DataType::DOUBLE:
            agg_func = make_unique<SumScanAggregate<k_double64, k_double64>>
                (i, argIdx, len);
            break;
          default:
          LOG_ERROR("unsupported data type for sum aggregation\n");
            status = KStatus::FAIL;
            break;
        }
        break;
      }
      case Sumfunctype::COUNT: {
        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<CountScanAggregate>(i, argIdx, len);
        break;
      }
      case Sumfunctype::COUNT_ROWS: {
        k_uint32 len = sizeof(k_int64);
        agg_func = make_unique<CountRowScanAggregate>(i, len);
        break;
      }
      case Sumfunctype::LAST: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);
        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          auto last_func = make_unique<LastScanAggregate<true>>(i, argIdx, tsIdx, len + STRING_WIDE);
          agg_func = std::move(last_func);
        } else {
          agg_func = make_unique<LastScanAggregate<false>>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);
        agg_func = make_unique<LastTSScanAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::LAST_ROW: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<LastRowScanAggregate<true>>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<LastRowScanAggregate<false>>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::LASTROWTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);

        agg_func = make_unique<LastRowTSScanAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<FirstScanAggregate<true>>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<FirstScanAggregate<false>>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTTS: {
        k_uint32 len = sizeof(KTimestampTz);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);
        agg_func = make_unique<FirstTSScanAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::FIRST_ROW: {
        k_uint32 len = agg_output_fields_[i]->get_storage_length();
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(output_fields_[argIdx]->get_storage_type());
        agg_output_fields_[i]->set_storage_length(output_fields_[argIdx]->get_storage_length());

        if (IsStringType(agg_output_fields_[i]->get_storage_type())) {
          agg_func = make_unique<FirstRowScanAggregate<true>>(i, argIdx, tsIdx, len + STRING_WIDE);
        } else {
          agg_func = make_unique<FirstRowScanAggregate<false>>(i, argIdx, tsIdx, len);
        }
        break;
      }
      case Sumfunctype::FIRSTROWTS: {
        k_uint32 len = sizeof(k_uint64);
        k_uint32 tsIdx = agg.col_idx(1);

        agg_output_fields_[i]->set_storage_type(roachpb::DataType::TIMESTAMP);
        agg_output_fields_[i]->set_storage_length(len);

        agg_func = make_unique<FirstRowTSScanAggregate>(i, argIdx, tsIdx, len);
        break;
      }
      case Sumfunctype::STDDEV: {
        k_uint32 len = sizeof(k_int64);
        status = KStatus::FAIL;
        break;
      }
      case Sumfunctype::AVG: {
        k_uint32 len = sizeof(k_int64);
        status = KStatus::FAIL;
        break;
      }
      default:
      LOG_ERROR("unknown aggregation function type %d\n", func_type);
        status = KStatus::FAIL;
        break;
    }

    if (agg_func != nullptr) {
      if (agg.distinct()) {
        // the distinct operator is not supported by Agg Scan OP, report an error here.
        // should use the original HASH Agg instead.
        Return(FAIL);
      }

      funcs_.push_back(std::move(agg_func));
    }
  }
  Return(status);
}

BaseOperator* AggTableScanOperator::Clone() {
  BaseOperator* iter = NewIterator<AggTableScanOperator>(*this, input_, this->processor_id_);
  return iter;
}

}  // namespace kwdbts
