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

#pragma once

#include <vector>
#include <iostream>
#include <limits>
#include <string.h>
#include "data_type.h"
#include "data_value_handler.h"
#include "settings.h"


using namespace std;

class DataHelper {
protected:
  BigTable *bt_;
  void *data_;
  vector<char> internal_data_;
  vector<AttributeInfo> *attr_info_;
  vector<DataToStringPtr> to_str_handler_;
  vector<StringToDataPtr> to_data_handler_;
  vector<int> latest_cols_;
  std::string time_format_;
  int data_size_;

public:
  DataHelper();

  DataHelper(BigTable *bt, vector<AttributeInfo> &attr_info,
    bool is_internal_data = true, const string & time_format =
        kwdbts::EngineOptions::dateTimeFormat());

  virtual ~DataHelper();

  void setHelper(BigTable *bt,
    vector<AttributeInfo> &attr_info, bool is_internal_data = true,
    const std::string & time_format = kwdbts::EngineOptions::dateTimeFormat());

  void *getData() const { return data_; }

  bool isNull(int col, void *data)
  { return to_str_handler_[col]->isNull(data); }

  // set VARSTRING file
  void setStringFile(int col, void *sf);

  inline void * columnAddr(int col, void *data) const
  { return (void *) ((intptr_t)data + (*attr_info_)[col].offset); }

  // Returns  a string for a column data
  string columnToString(int col, void *data) const
  { return to_str_handler_[col]->toString(data); }

  // Returns length of the string converted from a column data
  // the converted string is copied into memory pointed by str.
  int columnToString(int col, void *data, char *str) const
  { return to_str_handler_[col]->toString(data, str); }

  // string to column indirectly
  inline int stringToColumn(int col, char *str, void *data)
  { return to_data_handler_[col]->toData(str, columnAddr(col, data)); }

};

