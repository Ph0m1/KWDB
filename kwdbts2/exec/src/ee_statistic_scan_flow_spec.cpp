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

#include "ee_statistic_scan_flow_spec.h"

#include "ee_field.h"
#include "lg_api.h"

namespace kwdbts {
EEIteratorErrCode StatisticSpecResolve::ResolveRender(kwdbContext_p ctx,
                                                      Field ***render,
                                                      k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  k_int32 col_size = spec_->cols_size();
  k_int32 agg_type_col = spec_->aggtypes_size();

  if (col_size != agg_type_col) {
    LOG_ERROR(
        "col size don't equal aggtypes size, col size - %d\taggtypes size %d",
        col_size, agg_type_col);
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (0 == col_size) {
    LOG_ERROR("this plan don't have statistic col, please check physics plan");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  outputcols_ = static_cast<Field **>(malloc(col_size * sizeof(Field *)));
  if (nullptr == outputcols_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("outputcols_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(outputcols_, 0, col_size * sizeof(Field *));
  outputcols_size_ = col_size;
  if (num > 0) {
    *render = static_cast<Field **>(malloc(num * sizeof(Field *)));
    if (nullptr == *render) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("renders_ malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(*render, 0, num * sizeof(Field *));
  }

  for (k_int32 i = 0; i < col_size; ++i) {
    k_uint32 tab = spec_->cols(i);
    //  LOG_DEBUG("scan outputcols : %d = %u\n", i, tab);
    Field *field = table_->GetFieldWithColNum(tab);
    if (nullptr == field) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
    k_int32 agg_type = spec_->aggtypes(i);
    outputcols_[i] = field;

    if (renders_size_ == 0) {
      Field *new_field = nullptr;
      code = NewAggBaseField(ctx, &new_field, field, agg_type, i);
      if (EEIteratorErrCode::EE_OK != code) {
        Return(code);
      }
      new_field->setColIdxInRs(i);
      (*render)[i] = new_field;
    }
  }

  // resolve render
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
    } else {
      (*render)[i] = field;
    }
  }

  Return(code);
}

EEIteratorErrCode StatisticSpecResolve::ResolveScanCols(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_int32 col_size = spec_->cols_size();
  table_->scan_cols_.reserve(col_size);
  k_bool is_contain_first_last = false;
  k_bool is_contain_sum_count = false;
  k_bool is_contain_max_min = false;
  for (k_int32 i = 0; i < col_size; ++i) {
    k_uint32 tab = spec_->cols(i);
    Field *field = table_->GetFieldWithColNum(tab);
    if (nullptr == field) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
    k_int32 agg_type = spec_->aggtypes(i);
    table_->scan_agg_types_.push_back((Sumfunctype)agg_type);

    if (field->get_num() >= table_->min_tag_id_ &&
        (agg_type == Sumfunctype::LAST || agg_type == Sumfunctype::FIRST ||
         (agg_type >= Sumfunctype::LAST_ROW &&
          agg_type <= Sumfunctype::FIRSTROWTS))) {
      is_contain_first_last = true;
    }

    if (field->get_num() >= table_->min_tag_id_ &&
        (agg_type == Sumfunctype::SUM || agg_type == Sumfunctype::COUNT ||
         agg_type == Sumfunctype::COUNT_ROWS)) {
      is_contain_sum_count = true;
    }

    if (field->get_num() < table_->min_tag_id_) {
      table_->scan_cols_.push_back(field->get_num());
      table_->scan_real_agg_types_.push_back((Sumfunctype)agg_type);
    }

    if (agg_type == Sumfunctype::MIN || agg_type == Sumfunctype::MAX) {
      is_contain_max_min = true;
    }
  }

  if (is_contain_sum_count) {
    is_insert_ts_index_ = 1;
    table_->scan_cols_.insert(table_->scan_cols_.begin(), 0);
    table_->scan_real_agg_types_.insert(table_->scan_real_agg_types_.begin(),
                                        Sumfunctype::COUNT);
  }
  if (is_contain_first_last) {
    is_insert_ts_index_++;
    is_insert_ts_index_++;
    table_->scan_cols_.insert(table_->scan_cols_.begin(), 0);
    table_->scan_cols_.insert(table_->scan_cols_.begin(), 0);
    table_->scan_real_agg_types_.insert(table_->scan_real_agg_types_.begin(),
                                        Sumfunctype::LAST);
    table_->scan_real_agg_types_.insert(table_->scan_real_agg_types_.begin(),
                                        Sumfunctype::FIRST);
  }

  if (!is_contain_sum_count && !is_contain_first_last && is_contain_max_min) {
    is_insert_ts_index_++;
    table_->scan_cols_.insert(table_->scan_cols_.begin(), 0);
    table_->scan_real_agg_types_.insert(table_->scan_real_agg_types_.begin(),
                                        Sumfunctype::COUNT);
  }

  Return(code);
}

void StatisticSpecResolve::RenderSize(kwdbContext_p ctx,
                                                   k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else {
    *num = spec_->cols_size();
  }
}

EEIteratorErrCode StatisticSpecResolve::ResolveReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
    Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    int column = i - 1;
    Field *org_field = outputcols_[i - 1];
    bool is_fix_idx = false;
    if (org_field->get_column_type() !=
        ::roachpb::KWDBKTSColumn_ColumnType::
            KWDBKTSColumn_ColumnType_TYPE_DATA) {
      column = org_field->get_num();
      if (table_->scan_agg_types_[i - 1] == Sumfunctype::FIRSTTS ||
          table_->scan_agg_types_[i - 1] == Sumfunctype::FIRSTROWTS) {
        column = 0;
        is_fix_idx = true;
      } else if (table_->scan_agg_types_[i - 1] == Sumfunctype::LASTTS ||
                 table_->scan_agg_types_[i - 1] == Sumfunctype::LASTROWTS) {
        column = 1;
        is_fix_idx = true;
      } else if (table_->scan_agg_types_[i - 1] == Sumfunctype::COUNT ||
                 table_->scan_agg_types_[i - 1] == Sumfunctype::COUNT_ROWS ||
                 table_->scan_agg_types_[i - 1] == Sumfunctype::SUM) {
        column = is_insert_ts_index_ - 1;
        is_fix_idx = true;
      }
      statistic_last_tag_index_++;
    }

    if (org_field->get_column_type() ==
        ::roachpb::KWDBKTSColumn_ColumnType::
            KWDBKTSColumn_ColumnType_TYPE_DATA) {
      column = column - statistic_last_tag_index_;
      if (is_insert_ts_index_) {
        column += is_insert_ts_index_;
      }
      is_fix_idx = true;
    }

    NewAggBaseField(ctx, field, org_field, table_->scan_agg_types_[i - 1],
                    column);
    if (is_fix_idx) {
      (*field)->setColIdxInRs(column);
    }
  }
  Return(code);
}

EEIteratorErrCode StatisticSpecResolve::NewAggBaseField(kwdbContext_p ctx,
                                                        Field **field,
                                                        Field *org_field,
                                                        k_int32 agg_type,
                                                        k_uint32 num) {
  EnterFunc();
  Field *field_tag = nullptr;
  k_bool is_has_tag = false;
  switch (agg_type) {
    case Sumfunctype::AVG:
    case Sumfunctype::STDDEV:
    // case Sumfunctype::SUM:
    case Sumfunctype::VARIANCE: {
      *field = new FieldDouble(num, roachpb::DataType::DOUBLE, sizeof(k_double64));
      (*field)->set_column_type(org_field->get_column_type());
      break;
    }
    case Sumfunctype::SUM: {
      if (org_field->get_column_type() ==
          ::roachpb::KWDBKTSColumn::ColumnType::
              KWDBKTSColumn_ColumnType_TYPE_DATA) {
        *field = new FieldSumInt(num, org_field->get_storage_type(), sizeof(k_double64));
      } else {
        is_has_tag = true;
        field_tag = org_field->field_to_copy();
        if (nullptr != field_tag) {
          field_tag->set_num(org_field->get_num());
          field_tag->setColIdxInRs(org_field->getColIdxInRs());
        }
        *field = new FieldSumStatisticTagSum(field_tag);
        if (nullptr != *field) {
          (*field)->set_num(num);
        }
      }
      break;
    }
    case Sumfunctype::COUNT:
    case Sumfunctype::SUM_INT:
    case Sumfunctype::COUNT_ROWS:
    case Sumfunctype::LASTTS:
    case Sumfunctype::LASTROWTS:
    case Sumfunctype::FIRSTTS:
    case Sumfunctype::FIRSTROWTS: {
      if (org_field->get_column_type() !=
              ::roachpb::KWDBKTSColumn::ColumnType::
                  KWDBKTSColumn_ColumnType_TYPE_DATA &&
          agg_type == Sumfunctype::COUNT) {
        is_has_tag = true;
        field_tag =
            new FieldSumInt(org_field->get_num(), org_field->get_storage_type(),
                            org_field->get_storage_length());
        if (nullptr != field_tag) {
          field_tag->set_column_type(org_field->get_column_type());
          field_tag->setColIdxInRs(org_field->getColIdxInRs());
        }

        *field = new FieldSumStatisticTagCount(field_tag);
        if (nullptr != *field) {
          (*field)->set_num(num);
        }
      } else {
        *field =
            new FieldLonglong(num, roachpb::DataType::BIGINT, sizeof(k_int64));
        if (agg_type == Sumfunctype::COUNT) {
          (*field)->set_field_statistic(true);
        }
      }
      break;
    }
    case Sumfunctype::MAX:
    case Sumfunctype::MIN:
    case Sumfunctype::FIRST:
    case Sumfunctype::LAST:
    case Sumfunctype::LAST_ROW:
    case Sumfunctype::ANY_NOT_NULL:
    case Sumfunctype::FIRST_ROW: {
      *field = org_field->field_to_copy();
      if (nullptr != *field) {
        (*field)->set_num(num);
        (*field)->setNullable(true);
      }
      break;
    }
    default: {
      LOG_ERROR("unknow agg type %d", agg_type);
      break;
    }
  }

  k_bool is_err = false;
  if (is_has_tag) {
    if (nullptr != field_tag) {
      new_fields_.insert(new_fields_.end(), field_tag);
      field_tag->table_ = table_;
    } else {
      is_err = true;
    }
  }

  if (nullptr != field) {
    new_fields_.insert(new_fields_.end(), *field);
    (*field)->table_ = table_;
  } else {
    is_err = true;
  }

  if (is_err) {
    LOG_ERROR("new agg base field failed");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(EEIteratorErrCode::EE_OK);
}

}  // namespace kwdbts
