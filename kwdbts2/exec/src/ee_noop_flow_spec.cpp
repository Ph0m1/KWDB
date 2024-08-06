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

#include "ee_noop_flow_spec.h"

#include "ee_field.h"
#include "lg_api.h"

namespace kwdbts {

void NoopPostResolve::RenderSize(kwdbContext_p ctx,
                                              k_uint32 *num) {
  *num = renders_size_;
}

EEIteratorErrCode NoopPostResolve::ResolveReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
    Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    if (nullptr == *field) {
      *field = input_->OutputFields()[i - 1];
    } else {
      (*field)->next_ = input_->OutputFields()[i - 1];
    }
  }

  Return(code);
}

EEIteratorErrCode NoopPostResolve::HandleRender(kwdbContext_p ctx,
                                                Field **render, k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // resolve render
  for (k_int32 i = 0; i < renders_size_; ++i) {
    std::string str = post_->renders(i);
    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, str, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve tree
    Field *field = ResolveBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      (render)[i] = field;
    }
  }

  Return(code);
}

}  // namespace kwdbts
