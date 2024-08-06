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

#include "ee_flow_param.h"

namespace kwdbts {

class DistinctSpecParam : public BasePostResolve {
 public:
  DistinctSpecParam(BaseOperator *input, DistinctSpec *spec,
                    TSPostProcessSpec *post, TABLE *table)
      : BasePostResolve(input, post, table), spec_(spec) {}
  virtual ~DistinctSpecParam();

  EEIteratorErrCode ResolveOrderColumns(kwdbContext_p ctx);
  EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;
  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;
  EEIteratorErrCode ResolveRender(kwdbContext_p ctx, Field ***render,
                                 k_uint32 num) override;

  EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                 k_uint32 num) override;
  EEIteratorErrCode ResolveNotOrderField(kwdbContext_p ctx, Field *renders);

 protected:
  EEIteratorErrCode ResolveDistinctCol(kwdbContext_p ctx, Field **render,
                                       k_bool is_render);
  EEIteratorErrCode MallocArray(kwdbContext_p ctx);

 public:
  DistinctSpec *spec_{nullptr};      // distinct spec
  Field **distinct_field_{nullptr};  // distinct field
  k_uint32 order_field_num_{0};      // order field num
  Field **order_field_{nullptr};
  Field **not_order_field_{nullptr};
  k_uint32 not_order_field_num_{0};
  k_uint32 current_not_order_index_{0};
};

}  // namespace kwdbts
