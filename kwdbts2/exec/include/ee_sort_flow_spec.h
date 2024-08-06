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
// Created by liguoliang on 2022/07/18.
#pragma once

#include <memory>
#include <vector>
#include "ee_flow_param.h"

namespace kwdbts {

class SortSpecParam : public BasePostResolve {
 public:
  SortSpecParam(BaseOperator *input, TSSorterSpec *spec, TSPostProcessSpec *post, TABLE *table)
      : BasePostResolve(input, post, table), spec_(spec) {}

  virtual ~SortSpecParam();

  EEIteratorErrCode ResolveSortSpec(kwdbContext_p ctx);
  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;
  EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                  k_uint32 num) override;
  EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;

 public:
  TSSorterSpec *spec_{nullptr};
  k_int32 order_size_{0};
  Field **order_fields_{nullptr};
  k_int32 *order_field_types_{nullptr};
  std::vector<k_uint32> col_idxs_;
  std::vector<k_int32> order_types_;
};

};  // namespace kwdbts
