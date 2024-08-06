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


#ifndef DATAVALUEHANDLER_H_
#define DATAVALUEHANDLER_H_

#include <inttypes.h>
#include <memory>
#include <sstream>
#include <string.h>
#include <limits>
#include <float.h>
#include "BigObjectConst.h"
#include "DataType.h"
#include "default.h"
#include "BigObjectError.h"

using namespace std;
using namespace bigobject;

class BigTable;

typedef IDTYPE (*valueToIDTYPEHandler)(void *data);
typedef int64_t (*valueToINT64Handler)(void *data);
typedef double (*valueToDOUBLEHandler)(void *data);

valueToIDTYPEHandler getValueToIDTYPEHandler(int type);
valueToINT64Handler getValueToINT64Handler(int type);
valueToDOUBLEHandler getValueToDOUBLEHandler(int type);

// the input data string to be inserted into record (row) buffer
struct DataToken {
  int token;                // lexical token value
  int col;                  // column # in table
  char *str;                // token string
  void *rec;                // record (row) buffer
};

class DataToString {
protected:
  vector<char> str_;          // internal string buffer
  int size_;
  int max_len_;
public:
  DataToString(int max_len, int size = 0): max_len_(max_len) { size_ = size; }

  virtual ~DataToString();

  int size() { return size_; }

  virtual bool isNull (void *data);

  virtual string toString(void *data);

  void updateMaxLen(int new_len) { max_len_ = new_len; }

  // convert data to string
  // @return: return the length of string
  virtual int toString(void *data, char *str) { return 0; }

  virtual void * dataAddrWithRdLock(void *data);

  virtual void unlock();
};

typedef std::unique_ptr<DataToString> DataToStringPtr;

DataToStringPtr getDataToStringHandler(const AttributeInfo &col_info,
  int encoding = DICTIONARY);

class StringToData {
protected:
  bool has_default_;
public:
  StringToData();

  virtual ~StringToData();

#if defined(IOT_MODE)
  virtual bool isLatest() const;
  virtual void setColumn(int col);
  virtual void setLatestTable(timestamp64 ts, BigTable *bt);
  virtual void setRow(uint64_t row);
#endif

  bool hasDefault() { return has_default_; }

  void setDefaultValueAction(Action *dv_act);

  virtual int toData(void *addr); // push default value

  // @param   str: input string. str is NOT "const char *" since it may be
  //          modified. E.g. it is over maximum allowed string length.
  // @return  0: OK
  //         -1: no space left
  int toDefaultData(void * addr);     // push

  int toDefaultBinaryData(void * addr);   // push

  virtual int toData(char *str, void * addr);     // push

//  // return -1 if input is invalid
//  virtual int toData2(char *str, void * addr);     // push

  virtual int toBinaryData(char *str, void * addr);   // push

  virtual int updateData(char *s, void *data);

  virtual int updateBinaryData(char *s, void *data);

  virtual void noPushToData(char *str, void *addr);   // no push

  virtual void clear();
};

typedef std::unique_ptr<StringToData> StringToDataPtr;

StringToDataPtr getStringToDataHandler(BigTable *bt, int col,
  const AttributeInfo &col_info,
  int encoding = DICTIONARY, const std::string & time_format = "");

int PointToWKB(Point *pt, char *s);

template <typename T1, typename T2, bool need_to_string>
int convertFixDataToData(const char* src, char* dst, int dst_len)  {
  T1 tmp;
  memcpy(&tmp, src, sizeof(tmp));
  int len = 0;
  if (need_to_string) {
    std::ostringstream oss;
    oss.clear();
    if (std::is_same<T1, float>::value) {
      oss.precision(7);
    } else if (std::is_same<T1, double>::value) {
      oss.precision(14);
    }
    oss.setf(std::ios::fixed);
    oss << tmp;
    if (oss.str().length() > dst_len) {
      return -1;
    }
    snprintf(dst, dst_len, "%s", oss.str().c_str());
    len = oss.str().length();
  }else {
    *reinterpret_cast<T2*>(dst) = tmp;
    len = dst_len;
  }
  return len;
}

template <int32_t to_type>
int convertStringToFixData(const char* src, char* dst, int src_len) {
  char* end_val = nullptr;
  const char* end = src + src_len;
  switch (to_type)
  {
    case DATATYPE::INT16 : {
      long value = std::strtol(src, &end_val, 10);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > INT16_MAX || value < INT16_MIN) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<int16_t*>(dst) = static_cast<int16_t>(value);
      break;
    }
    case DATATYPE::INT32 : {
      long value = std::strtol(src, &end_val, 10);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > INT32_MAX || value < INT32_MIN) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<int32_t*>(dst) = static_cast<int32_t>(value);
      break;
    }
    case DATATYPE::INT64 : {
      long long value;
      size_t end_pos = 0;
      try {
        // use stoll avoid no exceptions
        value = std::stoll(src, &end_pos);
      } catch (std::invalid_argument const& ex) {
        return -2;
      } catch (std::out_of_range const& ex) {
        return -1;
      }
      if (end_pos < src_len) {
        return -2;
      }
      
      *reinterpret_cast<int64_t*>(dst) = value;
      break;
    }
    case DATATYPE::FLOAT : {
      float value = std::strtof(src, &end_val);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > numeric_limits<float>::max() || value < numeric_limits<float>::lowest()) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<float*>(dst) = value;
      break;
    }
    case DATATYPE::DOUBLE : {
      double value = std::strtod(src, &end_val);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > numeric_limits<double>::max() || value < numeric_limits<double>::lowest()) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<double*>(dst) = value;
      break;
    }
    default:
      return -3;
  }
  return 0;
}

typedef int (*CONVERT_DATA_FUNC)(const char* src, char* dst, int dst_len);

CONVERT_DATA_FUNC getConvertFunc(int32_t old_data_type, int32_t new_data_type,
                                int32_t new_length, bool& is_digit_data, ErrorInfo& err_info);

#endif /* DATAVALUEHANDLER_H_ */
