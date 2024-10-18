// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details. Created by lxp on 2022/07/18.

#pragma once

#include <list>
#include <map>
#include <memory>
#include <vector>

#include "ee_flow_param.h"

namespace kwdbts {

class WindowSpecParam : public BasePostResolve {
 public:
  WindowSpecParam(BaseOperator *input, WindowerSpec *spec,
                  TSPostProcessSpec *post, TABLE *table)
      : BasePostResolve(input, post, table), spec_(spec) {
    func_size_ = spec_->windowfns_size();
  }

  virtual ~WindowSpecParam() {
    if (window_field_) {
      for (k_uint32 i = 0; i < func_size_; i++) {
        SafeDeletePointer(window_field_[i]);
      }
      SafeFreePointer(window_field_);
    }
  }
  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;
  EEIteratorErrCode ResolveRender(kwdbContext_p ctx, Field ***render,
                                  k_uint32 num) override;
  EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                 k_uint32 num) override;
  EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;
  EEIteratorErrCode ResolveFilter(kwdbContext_p ctx, Field **field,
                                  bool is_tagFilter) override;
  void ResolveSetOutputFields(std::vector<Field *> &output_fields);
  void ResolveFilterFields(std::vector<Field *> &fields);
  void ResolveWinFuncFields(std::vector<Field *> &output_fields);

 protected:
  EEIteratorErrCode ResolveWindowFuncCol(kwdbContext_p ctx, Field **renders,
                                         k_uint32 num);
  EEIteratorErrCode ResolvePartitonByCol(kwdbContext_p ctx, Field **renders,
                                         k_uint32 num);
  EEIteratorErrCode MallocArray(kwdbContext_p ctx);

 protected:
  k_bool is_has_filter_{0};

 public:
  WindowerSpec *spec_{nullptr};
  Field **window_field_{nullptr};
  std::vector<Field *> output_fields_;
  k_uint32 func_size_{0};
  std::map<k_uint32, k_uint32> map_winfunc_index;
  std::map<k_uint32, Field *> map_filter_index;
};

}  // namespace kwdbts
