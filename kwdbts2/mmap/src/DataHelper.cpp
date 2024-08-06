// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include "DataHelper.h"
#include "BigObjectUtils.h"
#include "VarString.h"
#include "BigObjectApplication.h"


DataHelper::DataHelper() {
  data_ = nullptr;
}

DataHelper::DataHelper(BigTable *bt, vector<AttributeInfo> &attr_info,
  bool is_internal_data, const string & time_format) {
  data_ = nullptr;
  setHelper(bt, attr_info, is_internal_data, time_format);
}

DataHelper::~DataHelper() {}

void DataHelper::setHelper(BigTable *bt,
  vector<AttributeInfo> &attr_info, bool is_internal_data,
  const string & time_format) {
  bt_ = bt;
  data_size_ = 0;
  time_format_ = time_format;
  attr_info_ = &attr_info;

  to_str_handler_.clear();
  to_data_handler_.clear();
  for (size_t i = 0; i < (*attr_info_).size(); ++i) {
    if ((*attr_info_)[i].isFlag(AINFO_DROPPED)) {
      to_str_handler_.push_back(nullptr);
      to_data_handler_.push_back(nullptr);
    } else {
      to_str_handler_.push_back(
          std::move(getDataToStringHandler((*attr_info_)[i], DICTIONARY)));

      to_data_handler_.push_back(
          std::move(
              getStringToDataHandler(bt_, i, (*attr_info_)[i],
                                     (*attr_info_)[i].encoding, time_format_)));
#if defined(IOT_MODE)
      if (to_data_handler_.back()->isLatest()) {
        latest_cols_.push_back(i);
        to_data_handler_[i]->setColumn(i);
      }
#endif
    }
    data_size_ += getDataTypeSize((*attr_info_)[i]);
  }
  if (is_internal_data) {
    internal_data_.reserve(data_size_);
    internal_data_.resize(data_size_);
    data_ = &(internal_data_[0]);
  }
}

#if defined(IOT_MODE)
void DataHelper::setLatestTable(timestamp64 ts, BigTable *bt) {
  for (size_t i = 0; i < to_data_handler_.size(); ++i) {
    to_data_handler_[i]->setLatestTable(ts, bt);
  }
}

void DataHelper::setLatestRow(uint64_t row) {
  for (size_t i = 0; i < latest_cols_.size(); ++i) {
    to_data_handler_[latest_cols_[i]]->setRow(row);
  }
}
#endif


void DataHelper::swap(DataHelper &rhs) {
    std::swap(internal_data_, rhs.internal_data_);
    std::swap(data_, rhs.data_);
    std::swap(to_str_handler_, rhs.to_str_handler_);
    std::swap(to_data_handler_, rhs.to_data_handler_);
    std::swap(time_format_, rhs.time_format_);
    std::swap(data_size_, rhs.data_size_);
}

void DataHelper::updateType(int col) {
  to_str_handler_[col] = std::move(
    getDataToStringHandler((*attr_info_)[col], DICTIONARY));
  to_data_handler_[col] = std::move(
    getStringToDataHandler(bt_, col, (*attr_info_)[col], DICTIONARY,
      time_format_));
}

void DataHelper::addHelper(AttributeInfo &a_info,
  bool is_internal_data, const std::string & time_format) {
  to_str_handler_.push_back(
    std::move(getDataToStringHandler(a_info, a_info.encoding)));

  to_data_handler_.push_back(
    std::move(
      getStringToDataHandler(bt_, bt_->numColumn() - 1,
        a_info, a_info.encoding, time_format_)));

  data_size_ += getDataTypeSize(a_info);

  if (is_internal_data) {
    internal_data_.reserve(data_size_);
    internal_data_.resize(data_size_);
    data_ = &(internal_data_[0]);
  }
}

void DataHelper::setStringFile(int col, void *sf) {
  StringToVARSTRING *to_var_str =
    (StringToVARSTRING *)((to_data_handler_[col]).get());
  to_var_str->setStringFile((MMapStringFile *)sf);
  VARSTRINGToString *to_str =
	(VARSTRINGToString *)((to_str_handler_[col]).get());
  to_str->setStringFile((MMapStringFile *)sf);
}

