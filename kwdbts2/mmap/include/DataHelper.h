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


#ifndef DATAHELPER_H_
#define DATAHELPER_H_

#include <vector>
#include <iostream>
#include <limits>
#include <string.h>
#include "BigObjectConst.h"
#include "default.h"
#include "DataType.h"
#include "DataValueHandler.h"
#include "BigObjectConfig.h"
//#include "action/Action.h"


using namespace std;
using namespace bigobject;

class DataHelper {
protected:
  BigTable *bt_;
  void *data_;
  vector<char> internal_data_;
  vector<AttributeInfo> *attr_info_;
  vector<DataToStringPtr> to_str_handler_;
  vector<StringToDataPtr> to_data_handler_;
#if defined(IOT_MODE)
  vector<int> latest_cols_;
#endif
  std::string time_format_;
  int data_size_;

public:
  DataHelper();

  DataHelper(BigTable *bt, vector<AttributeInfo> &attr_info,
    bool is_internal_data = true, const string & time_format =
      BigObjectConfig::dateTimeFormat());

  virtual ~DataHelper();

  void setHelper(BigTable *bt,
    vector<AttributeInfo> &attr_info, bool is_internal_data = true,
    const std::string & time_format = BigObjectConfig::dateTimeFormat());

  void addHelper(AttributeInfo &a_info,
    bool is_internal_data,
    const std::string & time_format = BigObjectConfig::dateTimeFormat());

#if defined(IOT_MODE)
  void setLatestTable(timestamp64 ts, BigTable *bt);
  void setLatestRow(uint64_t row);
#endif

  void swap(DataHelper &rhs);

  void updateType(int col);

  void setData(void *data) { data_ = data; }

  void *getData() const { return data_; }

  bool isNull(int col, void *data)
  { return to_str_handler_[col]->isNull(data); }

  // set VARSTRING file
  void setStringFile(int col, void *sf);

  inline void * columnAddr(int col) const
  { return (void *) ((intptr_t)data_ + (*attr_info_)[col].offset); }

  inline void * columnAddr(int col, void *data) const
  { return (void *) ((intptr_t)data + (*attr_info_)[col].offset); }

  // Returns  a string for a column data
  string columnToString(int col, void *data) const
  { return to_str_handler_[col]->toString(data); }

  // Returns length of the string converted from a column data
  // the converted string is copied into memory pointed by str.
  int columnToString(int col, void *data, char *str) const
  { return to_str_handler_[col]->toString(data, str); }

  void toColumn (int col, void *value, void *data)
  { memcpy(columnAddr(col, data), value, (*attr_info_)[col].size); }

  // string to column indirectly
  inline int stringToColumn(int col, char *str, void *data)
  { return to_data_handler_[col]->toData(str, columnAddr(col, data)); }

  void *dataAddrWithRdLock(int col, void *data)
  { return to_str_handler_[col]->dataAddrWithRdLock(data); }

  void unlockColumn(int col)
  { to_str_handler_[col]->unlock(); }
};


#endif /* DATAHELPER_H_ */
