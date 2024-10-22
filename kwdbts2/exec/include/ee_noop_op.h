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

#include <vector>

#include "ee_base_op.h"
#include "ee_noop_flow_spec.h"
#include "ee_row_batch.h"

namespace kwdbts {

class TSPostProcessSpec;
class TSNoopSpec;

/**
 * @brief
 *   loop operator
 */
class NoopOperator : public BaseOperator {
 public:
  /**
   * @brief Construct a new Noop Iterator object
   *
   * @param input
   */
  NoopOperator(TsFetcherCollection* collection, BaseOperator *input, TSNoopSpec *spec, TSPostProcessSpec *post,
               TABLE *table, int32_t processor_id);

  NoopOperator(const NoopOperator& other, BaseOperator* input, int32_t processor_id);

  virtual ~NoopOperator() {
    if (is_clone_) {
      delete input_;
    }
  }

  k_uint32 GetRenderSize() {
    return num_ == 0 ? input_->GetRenderSize() : num_;
  }

  Field *GetRender(int i) override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  /**
   * @brief
   *            init
   * @return int     0 - success, other - failed
   */
  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  /**
   * @brief
   *            read data
   * @return int    0 - success, other - faile
   */
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  /**
   * @brief
   *            close
   * @return int    0 - success, other - failed
   */
  KStatus Close(kwdbContext_p ctx) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;
  BaseOperator* Clone() override;

 protected:
  EEIteratorErrCode ResolveFilter(kwdbContext_p ctx,
                                  const RowBatchPtr &row_batch);
  void ResolveLimitOffset(kwdbContext_p ctx, const RowBatchPtr &row_batch);
  void make_noop_data_chunk(kwdbContext_p ctx, DataChunkPtr *chunk, k_uint32 capacity);

 private:
  BaseOperator *input_;  // input iterator
  k_uint32 limit_{0};
  k_uint32 offset_{0};
  NoopPostResolve param_;
  TSPostProcessSpec *post_{nullptr};
  Field *filter_{nullptr};
  k_uint32 examined_rows_{0};
};

}  // namespace kwdbts
