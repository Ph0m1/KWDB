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

#include "ee_distinct_flow_spec.h"

#include "ee_field.h"
#include "lg_api.h"

namespace kwdbts {

DistinctSpecParam::~DistinctSpecParam() {
  SafeFreePointer(distinct_field_);
  SafeFreePointer(order_field_);
  SafeFreePointer(not_order_field_);
  order_field_num_ = 0;
  not_order_field_num_ = 0;
  current_not_order_index_ = 0;
}

EEIteratorErrCode DistinctSpecParam::ResolveOrderColumns(kwdbContext_p ctx) {
  EnterFunc();
  order_field_num_ = spec_->ordered_columns_size();
  if (0 == order_field_num_) {
    LOG_DEBUG("DISTINCT don't have order columns");
    Return(EEIteratorErrCode::EE_OK);
  }

  order_field_ =
      static_cast<Field **>(malloc(order_field_num_ * sizeof(Field *)));
  if (nullptr == order_field_) {
    LOG_ERROR("malloc memory failed, size %lu",
              order_field_num_ * sizeof(Field *));
    Return(EEIteratorErrCode::EE_ERROR);
  }

  for (k_uint32 i = 0; i < order_field_num_; ++i) {
    k_uint32 col = spec_->ordered_columns(i);
    order_field_[i] = outputcols_[col];
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode DistinctSpecParam::ResolveReference(
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

void DistinctSpecParam::RenderSize(kwdbContext_p ctx,
                                                k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcols_size_ > 0) {
    *num = outputcols_size_;
  } else {
    *num = input_->OutputFields().size();
  }

  not_order_field_num_ = spec_->distinct_columns_size() - order_field_num_;
}

EEIteratorErrCode DistinctSpecParam::ResolveRender(kwdbContext_p ctx,
                                                 Field ***render,
                                                 k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // malloc render
  if (num > 0) {
    *render = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == *render) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("renders_ malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(*render, 0, num * sizeof(Field *));
  }

  // outputcols_size_ > 0,  assign it to the render
  if (outputcols_size_ > 0) {
    outputcols_ =
        static_cast<Field **>(malloc(outputcols_size_ * sizeof(Field *)));
    if (nullptr == outputcols_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("outputcols_ malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(outputcols_, 0, outputcols_size_ * sizeof(Field *));

    for (k_int32 i = 0; i < outputcols_size_; ++i) {
      k_uint32 tab = post_->outputcols(i);
      Field *field = input_->OutputFields()[tab];
      if (nullptr == field) {
        Return(EEIteratorErrCode::EE_ERROR);
      }
      outputcols_[i] = field;
      if (renders_size_ == 0) {
        (*render)[i] = field;
      }
    }
  }

  // renders_size_ > 0
  if (renders_size_ > 0) {
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
        (*render)[i] = field;
      }
    }
  }

  // renders_size_ == 0 && outputcols_size_ == 0，use the input column of the
  // previous operator as the output column
  if (0 == renders_size_ && 0 == outputcols_size_) {
    for (k_int32 i = 0; i < num; ++i) {
      (*render)[i] = input_->OutputFields()[i];
    }
  } else {
    // dispose field return_type
    code = ResolveOutputType(ctx, *render, num);
    if (EEIteratorErrCode::EE_OK != code) {
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode DistinctSpecParam::HandleRender(kwdbContext_p ctx,
                                                  Field **render,
                                                  k_uint32 num) {
  EnterFunc();

  if (not_order_field_num_ > 0) {
    not_order_field_ =
        static_cast<Field **>(malloc(not_order_field_num_ * sizeof(Field *)));
    if (nullptr == not_order_field_) {
      LOG_ERROR("malloc not_order_field_ failed, size : %lu",
                not_order_field_num_ * sizeof(Field *));
      Return(EEIteratorErrCode::EE_ERROR);
    }

    memset(not_order_field_, 0, not_order_field_num_ * sizeof(Field *));
  }

  // renders
  EEIteratorErrCode code =
      ResolveDistinctCol(ctx, render, renders_size_ == 0 ? 1 : 0);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }

  for (k_uint32 i = 0; i < renders_size_; ++i) {
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
    } else {
      render[i] = field;
    }
  }

  Return(code);
}

EEIteratorErrCode DistinctSpecParam::ResolveNotOrderField(kwdbContext_p ctx,
                                                          Field *field) {
  EnterFunc();
  if (0 == not_order_field_num_) {
    Return(EEIteratorErrCode::EE_OK);
  }

  k_uint32 i = 0;
  for (; i < order_field_num_; ++i) {
    if (field == order_field_[i]) {
      break;
    }
  }

  if (i == order_field_num_) {
    not_order_field_[current_not_order_index_] = field;
    ++current_not_order_index_;
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode DistinctSpecParam::ResolveDistinctCol(kwdbContext_p ctx,
                                                        Field **render,
                                                        k_bool is_render) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  Field **out = render;
  // malloc
  if (!is_render) {
    code = MallocArray(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      Return(code);
    }
    out = distinct_field_;
  }
  std::vector<Field *> &output_fields = input_->OutputFields();
  k_int32 count = spec_->distinct_columns_size();
  for (k_int32 i = 0; i < count; ++i) {
    k_int32 num = spec_->distinct_columns(i);
    out[i] = output_fields[num];
    ResolveNotOrderField(ctx, out[i]);
  }

  Return(code);
}

EEIteratorErrCode DistinctSpecParam::MallocArray(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_int32 num = spec_->distinct_columns_size();
  distinct_field_ = static_cast<Field **>(malloc(num * sizeof(Field *)));
  if (nullptr == distinct_field_) {
    LOG_ERROR("distinct_field_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  memset(distinct_field_, 0, num * sizeof(Field *));

  Return(code);
}

}  // namespace kwdbts
