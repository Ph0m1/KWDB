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

#include <memory>
#include <unordered_map>
#include <vector>

#include "ee_aggregate_flow_spec.h"
#include "ee_aggregate_func.h"
#include "ee_base_op.h"
#include "ee_combined_group_key.h"
#include "ee_data_chunk.h"
#include "ee_global.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {
// Base Agg OP
class BaseAggregator : public BaseOperator {
 public:
  BaseAggregator(BaseOperator* input, TSAggregatorSpec* spec, TSPostProcessSpec* post, TABLE* table, int32_t processor_id);
  BaseAggregator(const BaseAggregator&, BaseOperator* input, int32_t processor_id);
  virtual ~BaseAggregator();

  /*
    Inherited from BseIterator's virtual function
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;  // init spec param
  EEIteratorErrCode Reset(kwdbContext_p ctx) override;    // Reset
  KStatus Close(kwdbContext_p ctx) override;  // close and free ctx

  virtual KStatus ResolveAggFuncs(kwdbContext_p ctx);  // make agg func
  virtual void ResolveGroupByCols(kwdbContext_p ctx);  // resolve agg cols

  void InitFirstLastTimeStamp(DatumRowPtr ptr);

  friend class AggregatorResolve;

 protected:
  virtual void CalculateAggOffsets();  // calculate buffer offset

  // accumulaten for agg
  KStatus accumulateRowIntoBucket(kwdbContext_p ctx, DatumRowPtr bucket, k_uint32 agg_null_offset,
                                  DataChunkPtr& chunk, k_uint32 line);

  TSAggregatorSpec* spec_;
  TSPostProcessSpec* post_;
  AggregatorSpecParam<TSAggregatorSpec> param_;
  BaseOperator* input_;

  // AggregationFunc is an interface to an aggregate function object (including
  // the AddOrUpdate method) that inherits a class such as MaxAggregate
  std::vector<unique_ptr<AggregateFunc>> funcs_;

  // This inputs column references (FieldNum)
  std::vector<Field*>& input_fields_;
  // the list of The inputs column's type
  std::vector<roachpb::DataType> data_types_;

  // group col
  std::vector<k_uint32> group_cols_;

  // agg spec
  std::vector<TSAggregatorSpec_Aggregation> aggregations_;

  // Having filter
  Field* having_filter_{nullptr};  // filter

  // agg row width
  k_uint32 agg_row_size_{0};  // row width
  k_uint32 agg_null_offset_{0};

  std::vector<k_uint32> func_offsets_;

  TSAggregatorSpec_Type group_type_;
  k_bool is_done_{false};

  k_uint32 offset_{0};
  k_uint32 limit_{0};

  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};   // for limit examine
  k_uint32 total_read_row_{0};  // total read
};

/**
 * @brief
 *        HashAggregateOperator
 * @author
 */
class HashAggregateOperator : public BaseAggregator {
  using map_iterator = std::unordered_map<CombinedGroupKey, DatumRowPtr, GroupKeyHasher>::iterator;

 public:
  /**
   * @brief Construct a new Aggregate Operator object
   *
   * @param input
   * @param spec
   * @param post
   * @param table
   */
  HashAggregateOperator(BaseOperator* input, TSAggregatorSpec* spec,
                        TSPostProcessSpec* post, TABLE* table, int32_t processor_id);
  HashAggregateOperator(const HashAggregateOperator&, BaseOperator* input, int32_t processor_id);
  ~HashAggregateOperator() override;

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode Start(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  BaseOperator* Clone() override;

  friend class FieldAggNum;

 protected:
  // accumulate one batch
  KStatus accumulateBatch(kwdbContext_p ctx, DataChunkPtr& chunk);
  // accumulate one row
  KStatus accumulateRow(kwdbContext_p ctx, DataChunkPtr& chunk, k_uint32 line);
  // accumulate rows
  virtual KStatus accumulateRows(kwdbContext_p ctx);
  virtual KStatus getAggResults(kwdbContext_p ctx, DataChunkPtr& chunk);

  std::unordered_map<CombinedGroupKey, DatumRowPtr, GroupKeyHasher> buckets_;
  map_iterator iter_;
};

/**
 * @brief
 *        OrderedAggregateOperator
 * @author
 */
class OrderedAggregateOperator : public BaseAggregator {
 public:
  OrderedAggregateOperator(BaseOperator* input, TSAggregatorSpec* spec,
                            TSPostProcessSpec* post, TABLE* table, int32_t processor_id);
  OrderedAggregateOperator(const OrderedAggregateOperator&, BaseOperator* input, int32_t processor_id);
  ~OrderedAggregateOperator() override;

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode Start(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  BaseOperator* Clone() override;

  friend class FieldAggNum;

 protected:
  // void insertOneRow(kwdbContext_p ctx, DataChunkPtr& chunk, DatumPtr data, k_uint32 index);
  KStatus getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk);
  KStatus accumulateRows(kwdbContext_p ctx, DataChunkPtr& chunk);

  DatumRowPtr bucket_{nullptr};
  DataChunkPtr chunk_{nullptr};  // the next result of the sub operator
  CombinedGroupKey last_group_key_;
  k_bool has_agg_result{false};
  k_uint32 processed_rows{0};
};

}  // namespace kwdbts
