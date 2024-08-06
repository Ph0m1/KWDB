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

#include "ee_sort_flow_spec.h"
#include "ee_field.h"
#include "lg_api.h"

namespace kwdbts {

SortSpecParam::~SortSpecParam() {
  if (nullptr != order_fields_) {
    SafeFreePointer(order_fields_);
  }

  if (nullptr != order_field_types_) {
    SafeFreePointer(order_field_types_);
  }

  order_size_ = 0;
}

EEIteratorErrCode SortSpecParam::ResolveSortSpec(kwdbContext_p ctx) {
  EnterFunc();
  if (!spec_->has_output_ordering()) {
    LOG_ERROR("order by clause must has a order field");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  const TSOrdering &Ordering = spec_->output_ordering();
  order_size_ = Ordering.columns_size();

  order_fields_ = static_cast<Field **>(malloc(order_size_ * sizeof(Field *)));
  if (nullptr == order_fields_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("new order field failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  order_field_types_ = static_cast<k_int32 *>(malloc(order_size_ * sizeof(k_int32)));
  if (nullptr == order_field_types_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("new order field type failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  for (k_int32 i = 0; i < order_size_; i++) {
    k_int32 idx = Ordering.columns(i).col_idx();
    TSOrdering_Column_Direction direction = Ordering.columns(i).direction();
    if (outputcols_size_ > 0) {
      order_fields_[i] = outputcols_[idx];
    } else {
      order_fields_[i] = (input_->GetRender())[idx];
    }

    order_field_types_[i] = direction;
    col_idxs_.push_back(idx);
    order_types_.push_back(direction);
  }

  Return(EEIteratorErrCode::EE_OK);
}

void SortSpecParam::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcols_size_ > 0) {
    *num = outputcols_size_;
  } else {
    *num = 0;
  }
}

EEIteratorErrCode SortSpecParam::HandleRender(kwdbContext_p ctx, Field **render,
                                              k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  std::vector<Field *>& input_fields = input_->OutputFields();
  // resolve renders
  if (0 == renders_size_) {
    for (k_uint32 i = 0; i < input_fields.size(); ++i) {
      render[i] = input_fields[i];
    }
    Return(code);
  }

  for (k_int32 i = 0; i < renders_size_; ++i) {
    std::string str = post_->renders(i);
    // binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, str, &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve binary tree
    Field *field = ResolveBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      render[i] = field;
    }
  }

  Return(code);
}

EEIteratorErrCode SortSpecParam::ResolveReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
    Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  std::vector<Field *>& input_fields = input_->OutputFields();
  for (auto i : virtualField->args_) {
    if (nullptr == *field) {
      *field = input_fields[i - 1];
    } else {
      (*field)->next_ = input_fields[i - 1];
    }
  }

  Return(code);
}

}  // namespace kwdbts
