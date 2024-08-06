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

#pragma once

#include <set>
#include <memory>
#include <map>
#include <vector>
#include <queue>
#include <limits>
#include <string>

#include "kwdb_type.h"
#include "ee_scan_op.h"
#include "ee_aggregate_flow_spec.h"
#include "ee_scan_aggregate_func.h"

namespace kwdbts {
// group by col info
struct GroupByColumnInfo {
  k_uint32 col_index;
  char* data_ptr;
  k_uint32 len;
};

// AggTableScanOperator is used by agg op
class AggTableScanOperator : public TableScanOperator {
 public:
  AggTableScanOperator(TSReaderSpec* spec, TSPostProcessSpec* post,
                       TABLE* table, BaseOperator* input, int32_t processor_id) :
      TableScanOperator(spec, post, table, input, processor_id),
      table_reader_spec_(*spec),
      aggregation_spec_(spec->aggregator()),
      aggregation_post_(spec->aggregatorpost()) {
    for (k_int32 i = 0; i < spec->aggregator().aggregations_size(); ++i) {
      aggregations_.push_back(spec->aggregator().aggregations(i));
    }

    k_uint32 group_size_ = spec->aggregator().group_cols_size();
    for (k_int32 i = 0; i < group_size_; ++i) {
      k_uint32 group_col = spec->aggregator().group_cols(i);
      group_cols_.push_back(group_col);
    }
  }

  AggTableScanOperator(const AggTableScanOperator& other, BaseOperator* input, int32_t processor_id) :
      TableScanOperator(other, input, processor_id),
      table_reader_spec_(other.table_reader_spec_),
      aggregation_spec_(other.aggregation_spec_),
      aggregation_post_(other.aggregation_post_) {
    for (k_int32 i = 0; i < table_reader_spec_.aggregator().aggregations_size(); ++i) {
      aggregations_.push_back(table_reader_spec_.aggregator().aggregations(i));
    }

    k_uint32 group_size_ = other.table_reader_spec_.aggregator().group_cols_size();
    for (k_int32 i = 0; i < group_size_; ++i) {
      k_uint32 group_col = other.table_reader_spec_.aggregator().group_cols(i);
      group_cols_.push_back(group_col);
    }
  }

  ~AggTableScanOperator() override {
    if (agg_num_ > 0 && agg_renders_) {
      free(agg_renders_);
      agg_renders_ = nullptr;
    }

    for (auto field : agg_output_fields_) {
      SafeDeletePointer(field);
    }

    agg_num_ = 0;
  };
  // resolve spec
  EEIteratorErrCode PreInit(kwdbContext_p ctx) override;
  // call Next for get data
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  // clone the operator for parallel
  BaseOperator* Clone() override;

  // add data to trunk struct
  KStatus AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch,
                          Field** renders);
  // process group col
  k_bool ProcessGroupCols(k_int32& target_row, RowBatch* row_batch,
                          k_uint32 row,
                          std::vector<GroupByColumnInfo>& group_by_cols,
                          KTimestampTz& time_bucket,
                          std::vector<std::vector<DataSource>>& source);

  // resolve agg func
  KStatus ResolveAggFuncs(kwdbContext_p ctx);

  char* GetFieldDataPtr(Field* field, RowBatch* row_batch);

  [[nodiscard]] inline KTimestampTz construct(Field* filed) const {
    auto time_bucket_field = dynamic_cast<FieldFuncTimeBucket*>(filed);
    if (time_bucket_field != nullptr) {
      KTimestampTz original_timestamp = time_bucket_field->getOriginalTimestamp();
      return original_timestamp - original_timestamp % interval_seconds_;
    }
    return 0;
  }

  [[nodiscard]] bool hasTimeBucket() const {
    return interval_seconds_ != 0;
  }
  // timebucket
  void extractTimeBucket(Field** readers, k_uint32 render_num) {
    for (k_int32 i = 0; i < render_num; ++i) {
      Field* field = readers[i];
      auto time_bucket_field = dynamic_cast<FieldFuncTimeBucket*>(field);
      if (time_bucket_field != nullptr) {
        if (!time_bucket_field->field_is_nullable()) {
          interval_seconds_ = time_bucket_field->getIntervalSeconds();
          col_idx_ = time_bucket_field->get_num();
        }
      }
    }
  }
  // construct agg info
  inline void constructAggResults() {
    // initialize the agg output buffer.
    agg_results_ = std::make_unique<DataChunk>(agg_output_col_info);
    if (agg_results_->Initialize() < 0) {
      agg_results_ = nullptr;
      return;
    }
    agg_results_->setScanAgg(true);
    agg_results_->setPassAgg(true);
    agg_results_->SetAllNull();
  }

  template<typename T>
  void processGroupByColumn(char* source_ptr, char* target_ptr, uint32_t target_col,
                            bool is_dest_null, std::vector<GroupByColumnInfo>& group_by_cols, bool& is_new_group) {
    if constexpr(std::is_same_v<T, std::string>) {
      auto source_str = std::string_view{source_ptr};
      k_uint32 len = source_str.length();
      if (is_dest_null) {
        is_new_group = true;
      } else {
        auto string_val_ptr = target_ptr + STRING_WIDE;
        auto target_str = std::string_view{string_val_ptr};
        if (source_str != target_str) {
          is_new_group = true;
        }
      }
      group_by_cols.push_back({target_col, source_ptr, len});
    } else {
      if (is_dest_null) {
        is_new_group = true;
      } else {
        T src_val = *reinterpret_cast<T*>(source_ptr);
        T dest_val = *reinterpret_cast<T*>(target_ptr);

        if constexpr(std::is_same_v<T, std::float_t> || std::is_same_v<T, std::double_t>) {
          if (std::abs(src_val - dest_val) < std::numeric_limits<double>::epsilon()) {
            is_new_group = true;
          }
        } else {
          if (src_val != dest_val) {
            is_new_group = true;
          }
        }
      }
      group_by_cols.push_back({target_col, source_ptr, sizeof(T)});
    }
  }

 private:
  k_uint32 col_idx_{0};
  k_int64 interval_seconds_{0};

  // the list of input column's type
  std::vector<roachpb::DataType> data_types_;

  TSReaderSpec& table_reader_spec_;

  // group cols
  std::vector<k_uint32> group_cols_;

  // agg cols
  std::vector<k_uint32> agg_cols_;
  std::vector<k_uint32> normal_cols_;
  // storage agg funcs
  std::vector<unique_ptr<ScanAggregateFunc>> funcs_;

  // Aggregate spec
  std::vector<TSAggregatorSpec_Aggregation> aggregations_;
  const TSAggregatorSpec& aggregation_spec_;
  const TSPostProcessSpec& aggregation_post_;
  std::vector<k_uint32> agg_source_target_col_map_;

  Field** agg_renders_{nullptr};  // agg operator projection column
  k_uint32 agg_num_{0};           // the count of agg projection column

  std::vector<Field*> agg_output_fields_;  // the output field of agg operator

  std::vector<ColumnInfo> agg_output_col_info;  // construct agg output col
  DataChunkPtr agg_results_;  // agg result info
  std::queue<DataChunkPtr> agg_output_queue;

  bool is_done_{false};
  std::vector<k_uint32> arg_idx_vec;

  std::vector<std::vector<DataSource>> source;
};

}  //  namespace kwdbts
