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
#include <vector>
#include <unordered_set>

#include "ee_base_op.h"
#include "ee_distinct_flow_spec.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "ee_combined_group_key.h"

namespace kwdbts {

/**
 * @brief DistinctOperator
 * @author
 */
class DistinctOperator : public BaseOperator {
 public:
  /**
   * @brief Construct a new Aggregate Operator object
   *
   * @param input
   * @param spec
   * @param post
   * @param table
   */
  DistinctOperator(BaseOperator* input, DistinctSpec* spec, TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  DistinctOperator(const DistinctOperator&, BaseOperator* input, int32_t processor_id);

  virtual ~DistinctOperator();

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode PreInit(kwdbContext_p ctx) override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

 protected:
  // resolve distinct cols
  KStatus ResolveDistinctCols(kwdbContext_p ctx);

  void encodeDistinctCols(DataChunkPtr& chunk, k_uint32 line, CombinedGroupKey& distinct_keys);

  DistinctSpec* spec_;
  TSPostProcessSpec* post_;
  BaseOperator* input_{nullptr};
  DistinctSpecParam param_;

  // distinct column
  std::vector<k_uint32> distinct_cols_;
  unordered_set<CombinedGroupKey, GroupKeyHasher> seen;

  // This layer inputs column references (FieldNum)
  std::vector<Field*>& input_fields_;

  k_uint32 offset_{0};
  k_uint32 limit_{0};

  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
};

}  // namespace kwdbts
