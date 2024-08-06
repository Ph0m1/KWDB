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


#include <arpa/inet.h>
#include <cstdio>
#include <sstream>
#include "DataValueHandler.h"
#include "DateTime.h"
#include "BigObjectUtils.h"
#include "VarString.h"
#include "string/mmapstring.h"
#include "lib/HyperLogLog.h"


using namespace std;
using namespace bigobject;

template<typename T>
IDTYPE valueToIDTYPEINT(void *data) {
#if defined(__UNALIGNED_ACCESS__)
  return (IDTYPE) *((T *) data);
#else
  T tmp;
  memcpy(&tmp , data , sizeof(T));
  return (IDTYPE)tmp;
#endif
}

template<typename T>
int64_t valueToINT64(void *v) {
#if defined(__UNALIGNED_ACCESS__)
  return (int64_t) *((T *) v);
#else
  T tmp;
  memcpy(&tmp , v , sizeof(T));
  return (int64_t)tmp;
#endif
}

int64_t valueToINT64Default(void *data) { return 0; }

template<typename T>
double valueToDOUBLE(void *v) {
#if defined(__UNALIGNED_ACCESS__)
  return (double) *((T *) v);
#else
  T tmp;
  memcpy(&tmp , v , sizeof(T));
  return (double)tmp;
#endif
}

double valueToDOUBLEDefault(void *data)
{
    return 0.0;
}

IDTYPE DateTime64ToIDTYPE32(void *data)
{
    DateTime64 dt64(data);
    struct tm ts;
    dt64.getTimeStructure(ts, data);
    DateTime32 dt32(ts);
    return dt32.toIDTYPE();
}

IDTYPE valueToIDTYPE32FLOAT(void *data)
{
#if defined(__UNALIGNED_ACCESS__)
    return (int32_t) *(reinterpret_cast<int32_t *> ((float*) data));
#else
    float tmp;
    memcpy(&tmp , data , sizeof(float));
    return (int32_t) *(reinterpret_cast<int32_t *> (&tmp));
#endif
}

IDTYPE valueToIDTYPE64FLOAT(void *data)
{
#if defined(__UNALIGNED_ACCESS__)
    double v = (double) (*(float *) data);
    return (IDTYPE) *(reinterpret_cast<int64_t *> (&v));
#else
    float tmp;
    memcpy(&tmp , data , sizeof(float));
    double v = (double)tmp;
    return (IDTYPE) *(reinterpret_cast<int64_t *> (&v));
#endif
}

IDTYPE valueToIDTYPE32DOUBLE(void *data)
{
#if defined(__UNALIGNED_ACCESS__)
    float v = (float) (*(double *) data);
    return (IDTYPE) *(reinterpret_cast<int32_t *> (&v));
#else
    double tmp;
    memcpy(&tmp , data , sizeof(double));
    float v = (float)tmp;
    return (IDTYPE) *(reinterpret_cast<int32_t *> (&v));
#endif
}

IDTYPE valueToIDTYPE64DOUBLE(void *data)
{
#if defined(__UNALIGNED_ACCESS__)
    return (int64_t) *(reinterpret_cast<int64_t *> ((double*) data));
#else
    double tmp;
    memcpy(&tmp , data , sizeof(double));

    return (int64_t) *(reinterpret_cast<int64_t *> (&tmp));
#endif
}

IDTYPE valueToIDTYPEDefault(void *data)
{
    return 0;
}

valueToIDTYPEHandler getValueToIDTYPEHandler(int type)
{
    if (sizeof(IDTYPE) == sizeof(int32_t)) {
        switch (type) {
        case STRING:
        case INT32:
        case VARSTRING:
        case ROWID:
        case TIMESTAMP:
            return &valueToIDTYPEINT<int32_t> ;
        case INT64:
            return &valueToIDTYPEINT<int64_t> ;
        case INT16:
            return &valueToIDTYPEINT<int16_t> ;
        case INT8:
        case BYTE:
            return &valueToIDTYPEINT<int8_t> ;
        case FLOAT:
            return &valueToIDTYPE32FLOAT;
        case DOUBLE:
            return &valueToIDTYPE32DOUBLE;
        case WEEK:
            return &valueToIDTYPEINT<int32_t> ;
        case DATE32:
        case DATETIMEDOS:
        case DATETIME32:
            return &valueToIDTYPEINT<int32_t> ;
        case DATETIME64:
            return &DateTime64ToIDTYPE32;
        }
    } else { // int64_t
        switch (type) {
        case INT32:
        case TIMESTAMP:
            return &valueToIDTYPEINT<int32_t> ;
        case STRING:
        case INT64:
        case VARSTRING:
        case ROWID:
            return &valueToIDTYPEINT<int64_t> ;
        case INT16:
            return &valueToIDTYPEINT<int16_t> ;
        case INT8:
        case BYTE:
            return &valueToIDTYPEINT<int8_t> ;
        case FLOAT:
            return &valueToIDTYPE64FLOAT;
        case DOUBLE:
            return &valueToIDTYPE64DOUBLE;
        case WEEK:
            return &valueToIDTYPEINT<int64_t> ;
        case DATE32:
        case DATETIMEDOS:
        case DATETIME32:
            return &valueToIDTYPEINT<int32_t> ;
        case DATETIME64:
            return &valueToIDTYPEINT<int64_t> ;
        }

    }
    return &valueToIDTYPEDefault;
}

valueToINT64Handler getValueToINT64Handler(int type)
{
    switch (type) {
    case STRING:
    case INT32:
    case INT32_ARRAY: return &valueToINT64<int32_t> ;
    case INT64:
    case INT64_ARRAY: return &valueToINT64<int64_t> ;
    case INT16:
    case INT16_ARRAY: return &valueToINT64<int16_t> ;
    case INT8:
    case BYTE:
    case INT8_ARRAY:
    case BYTE_ARRAY: return &valueToINT64<int8_t> ;
    case FLOAT:
    case FLOAT_ARRAY: return &valueToINT64<float> ;
    case DOUBLE:
    case DOUBLE_ARRAY: return &valueToINT64<double> ;
    case WEEK:
    case DATE32:
    case DATETIMEDOS:
    case DATETIME32: return &valueToINT64<int32_t> ;
    case DATETIME64: return &valueToINT64<int64_t> ;
    }
    return &valueToINT64Default;
}

valueToDOUBLEHandler getValueToDOUBLEHandler(int type)
{
    switch (type) {
    case STRING:
    case INT32:
    case INT32_ARRAY: return &valueToDOUBLE<int32_t> ;
    case INT64:
    case INT64_ARRAY: return &valueToDOUBLE<int64_t> ;
    case INT16:
    case INT16_ARRAY: return &valueToDOUBLE<int16_t> ;
    case INT8:
    case BYTE:
    case INT8_ARRAY:
    case BYTE_ARRAY: return &valueToDOUBLE<int8_t> ;
    case FLOAT:
    case FLOAT_ARRAY: return &valueToDOUBLE<float> ;
    case DOUBLE:
    case DOUBLE_ARRAY: return &valueToDOUBLE<double> ;
    case WEEK:
    case DATE32:
    case DATETIMEDOS:
    case DATETIME32: return &valueToDOUBLE<int32_t> ;
    case DATETIME64: return &valueToDOUBLE<int64_t> ;
    case ROWID:
#ifndef ID64
        return &valueToDOUBLE<int32_t>;
#else
        return &valueToDOUBLE<int64_t>;
#endif
    }
    return &valueToDOUBLEDefault;
}

inline bool isEmpty(char *s) {
  //to-do(allison&thomas): distinguish  and ''
  if (s == NULL || s == nullptr) {
    return false;
  }

  return ((strcmp(s, "") == 0) || (*s == 0x0));
}

bool isEqualIgnoreCase(char *src, const char* dest) {
  if (src == NULL || src == nullptr) {
    return false;
  }
  size_t size = strlen(dest);
  if (strlen(src) != size) {
    return false;
  }

  const int CHAR_DIFF = 'a' - 'A';
  int diff = -1;
  for (size_t i = 0; i < size; ++i) {
    diff = abs(src[i] - dest[i]);
    if (diff != 0 && diff != CHAR_DIFF) {
      return false;
    }
  }

  return true;
}

class BYTEToString: public DataToString {
public:
    BYTEToString(): DataToString(1, sizeof(int8_t)) {};

    virtual ~BYTEToString() {};

    virtual string toString(void *data) {
        char c = *((char *)data);
        if (c)
            return string(1, *((char *)data));
        else
            return bigobject::s_emptyString();
    }

    virtual int toString(void *data, char *str) {
//#if defined(__UNALIGNED_ACCESS__)
        *str++ = *((char *)data);
        *str = 0;
        return 1;
//#else
//        char tmp;
//    memcpy(&tmp , data , sizeof(int8_t));
//    *str++ = tmp;
//        *str = 0;
//        return 1;
//#endif
    }
};

class INT8DataToString: public DataToString {
public:
  INT8DataToString(int max_len): DataToString(max_len, sizeof(int8_t)) {}
  virtual ~INT8DataToString() {}

  virtual bool isNull (void *data) {
    if (BigObjectConfig::zeroIfNull())
      return false;
    return *(int8_t *)data == std::numeric_limits<int8_t>::min();
  }

  virtual string toString(void *data) {
    if (*(int8_t *)data == std::numeric_limits<int8_t>::min())
      return string(cstr_null);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int8_t *)data));
#else
    int8_t tmp;
    memcpy(&tmp , data , sizeof(int8_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(int8_t *)data == std::numeric_limits<int8_t>::min()) {
       strcpy(str, s_NULL().c_str());
       return 4;
     }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((int8_t *)data));
    return strlen(str);
#else
    int8_t tmp;
    memcpy(&tmp , data , sizeof(int8_t));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

class INT16DataToString: public DataToString {
public:
  INT16DataToString(int max_len): DataToString(max_len, sizeof(int16_t)) {}
  virtual ~INT16DataToString() {}

  virtual bool isNull (void *data) {
    if (BigObjectConfig::zeroIfNull())
       return false;
    return *(int16_t *)data == std::numeric_limits<int16_t>::min();
  }

  virtual string toString(void *data) {
    if (*(int16_t *)data == std::numeric_limits<int16_t>::min())
      return string(cstr_null);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int16_t *)data));
#else
    int16_t tmp;
    memcpy(&tmp , data , sizeof(int16_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(int16_t *)data == std::numeric_limits<int16_t>::min()) {
       strcpy(str, s_NULL().c_str());
       return 4;
     }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((int16_t *)data));
    return strlen(str);
#else
    int16_t tmp;
    memcpy(&tmp , data , sizeof(int16_t));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

template <typename T>
class IntegerDataToStringZN: public DataToString {
public:
  IntegerDataToStringZN(int max_len): DataToString(max_len, sizeof(T)) {}
  virtual ~IntegerDataToStringZN() {}

  virtual string toString(void *data) {
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((T *)data));
#else
    int32_t tmp;
    memcpy(&tmp , data , sizeof(T));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((T *)data));
    return strlen(str);
#else
    int32_t tmp;
    memcpy(&tmp , data , sizeof(T));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

template <typename T>
class IntegerDataToString: public DataToString {
public:
  IntegerDataToString(int max_len): DataToString(max_len, sizeof(T)) {}
  virtual ~IntegerDataToString() {}

  virtual bool isNull (void *data) {
    if (BigObjectConfig::zeroIfNull())
       return false;
    return *(T *)data == std::numeric_limits<T>::min();
  }

  virtual string toString(void *data) {
    if (*(T *)data == std::numeric_limits<T>::min())
      return string(cstr_null);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((T *)data));
#else
    T tmp;
    memcpy(&tmp , data , sizeof(T));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(T *)data == std::numeric_limits<T>::min()) {
      strcpy(str, s_NULL().c_str());
      return 4;
    }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((T *)data));
    return strlen(str);
#else
    T tmp;
    memcpy(&tmp , data , sizeof(T));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

class INT64DataToStringZN: public DataToString {
public:
  INT64DataToStringZN(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~INT64DataToStringZN() {};

  virtual string toString(void *data) {
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int64_t *)data));
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%ld", *((int64_t *)data));
    return strlen(str);
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    sprintf(str, "%ld", tmp);
    return strlen(str);
#endif
  }
};

class UINT64DataToStringZN: public DataToString {
public:
  UINT64DataToStringZN(int max_len): DataToString(max_len, sizeof(uint64_t)) {}
  virtual ~UINT64DataToStringZN() {}

  virtual string toString(void *data) {
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((uint64_t *)data));
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%lu", *((uint64_t *)data));
    return strlen(str);
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(uint64_t));
    sprintf(str, "%ld", tmp);
    return strlen(str);
#endif
  }
};

class INT64DataToString: public DataToString {
public:
  INT64DataToString(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~INT64DataToString() {};

  virtual bool isNull (void *data) {
    if (BigObjectConfig::zeroIfNull())
       return false;
    return *(int64_t *)data == std::numeric_limits<int64_t>::min();
  }

  virtual string toString(void *data) {
    if (*(int64_t *)data == std::numeric_limits<int64_t>::min())
      return string(cstr_null);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int64_t *)data));
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(int64_t *)data == std::numeric_limits<int64_t>::min()) {
      strcpy(str, s_NULL().c_str());
      return 4;
    }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%ld", *((int64_t *)data));
    return strlen(str);
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    sprintf(str, "%ld", tmp);
    return strlen(str);
#endif
  }
};


class TimeStamp64ToString: public DataToString {
protected:
  TimeStamp64DateTime ts64_;
//  DateTime64 date_;
public:
  TimeStamp64ToString(int max_len): DataToString(max_len, sizeof(int64_t))
  { ts64_.setPrecision(max_len); }
  virtual ~TimeStamp64ToString() {}

  virtual string toString(void *data) {
//#if defined(__UNALIGNED_ACCESS__)
//    return intToString(*((uint64_t *)data));
//#else
//    int64_t tmp;
//    memcpy(&tmp , data , sizeof(int64_t));
//    return intToString(tmp);
//#endif
    return ts64_.toString(data);
  }

   virtual int toString(void *data, char *str) {
#if defined(KAIWU) && defined(IOT_MODE)
     if (BigObjectConfig::iot_mode) {
       if (*((int64_t*) data) < 0) {
         sprintf(str, "-%lu", abs(*((int64_t*) data)));
       } else {
         sprintf(str, "%lu", *((int64_t*) data));
       }

       return strlen(str);
     } else
#endif
       return ts64_.BasicDateTime::strformat(data, str);
  }
};


class TimeStamp64LSNToString: public DataToString {
 public:
  TimeStamp64LSNToString() : DataToString(16, 16) {
    ts64_.setPrecision(3);
  }
  virtual ~TimeStamp64LSNToString() {}

  virtual bool isNull (void *data) { return false; }

  virtual string toString(void *data) {
    TimeStamp64LSN* ts_lsn = (TimeStamp64LSN*)data;
    std::stringstream ss;
    ss << ts64_.toString(&ts_lsn->ts64) << "," << ts_lsn->lsn;
    return ss.str();
  }
 protected:
  TimeStamp64DateTime ts64_;
};


template <typename T>
class FloatDataToString: public DataToString {
protected:
  vector<char> str_;
  DoubleFormatToString dbl_to_str_;
public:
  FloatDataToString(int max_len, int prec, bool is_rm_tz):
    DataToString(max_len, sizeof(T)), dbl_to_str_(prec, is_rm_tz)
  { str_.resize(max_len + 1); }
  virtual ~FloatDataToString() {}

  virtual bool isNull (void *data) { return !(isfinite(*((T*)data))); }

  virtual string toString(void *data) {
    if (isfinite(*((T*)data))) {
      dbl_to_str_.toString((char *)str_.data(), (double)(*((T*) data)));
      return string(str_.data());
    } else {
    	// so it can handle inf
      if (isinf(*((T*)data))) {
        if (*((T*)data) >= 0) {
          return '"' + string(cstr_inf) + '"';
        } else {
          return '"' + string(cstr_n_inf) + '"';
        }
      }
      return string(cstr_null);       // "null" for json string.
  }
}

  virtual int toString(void *data, char *str) {
#if defined(IOT_MODE)
    return dbl_to_str_.toString(str, (double)(*((T*) data)));
#else
    T v = *((T*)data);
    if (isfinite(v)) {
      return dbl_to_str_.toString(str, (double)(*((T*) data)));
    }
    return BOEVALUE;
#endif

  }
};

DataToString * getDataToString(int type, int max_len) {
#if defined(IOT_MODE)
    switch(type) {
      case INT8: return new IntegerDataToStringZN<int8_t>(max_len);
      case INT16: return new IntegerDataToStringZN<int16_t>(max_len);
      case INT32: return new IntegerDataToStringZN<int32_t>(max_len);
      case INT64: return new INT64DataToStringZN(max_len);
    }
#else
    if (BigObjectConfig::zeroIfNull()) {
        switch(type) {
          case INT8: return new IntegerDataToStringZN<int8_t>(max_len);
          case INT16: return new IntegerDataToStringZN<int16_t>(max_len);
          case INT32: return new IntegerDataToStringZN<int32_t>(max_len);
          case INT64: return new INT64DataToStringZN(max_len);
        }
      } else {
        switch(type) {
          case INT8: return new IntegerDataToString<int8_t>(max_len);
          case INT16: return new IntegerDataToString<int16_t>(max_len);
          case INT32: return new IntegerDataToString<int32_t>(max_len);
          case INT64: return  new INT64DataToString(max_len);
        }
      }
#endif
  return new DataToString(max_len); // useless
}

class RAWSTRINGToString: public DataToString
{
public:
    RAWSTRINGToString(): DataToString(sizeof(void *)) {};

    virtual ~RAWSTRINGToString() {};

    virtual string toString(void *data)
    { return string((const char *)data); }
};

class CHARToString: public DataToString
{
public:
    CHARToString(int max_len, int size): DataToString(max_len, size) {};

    virtual ~CHARToString() {};

    virtual string toString(void *data) {
        return ::toString((const char *)data, size_);
    }

    virtual int toString(void *data, char *str) {
        return mmap_strlcpy(str, (const char*) data, max_len_);
    }
};

#if defined(USE_SMART_INDEX)
class IndexToString: public IDTYPEToString
{
public:
    IndexToString(void *ns) : IDTYPEToString(ns) {};

    virtual ~IndexToString() {};

    virtual string toString(void *data)
    {
  return ns_->indexToString(*((IDTYPE *) data));
    }
};
#endif

class WEEKTIMEToString: public DataToString
{
public:
    WEEKTIMEToString(int max_len): DataToString(max_len, sizeof(int32_t)) {};

    virtual ~WEEKTIMEToString() {};

    virtual string toString(void *data)
    {
#if defined(__UNALIGNED_ACCESS__)
  int year = *((int *) data) >> 16;
  int week_num = *((int *) data) & WEEKNUMBERMASK;
#else
  int year;
  memcpy(&year , data , sizeof(int));
  year = year >> 16;
  int week_num;
  memcpy(&week_num , data , sizeof(int));
  week_num = week_num & WEEKNUMBERMASK;
#endif
  return intToString(year) + 'W' + intToString(week_num);
    }

    virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
        int year = *((int *) data) >> 16;
        int week_num = *((int *) data) & WEEKNUMBERMASK;
#else
        int year;
        memcpy(&year , data , sizeof(int));
        year = year >> 16;
        int week_num;
        memcpy(&week_num , data , sizeof(int));
        week_num = week_num & WEEKNUMBERMASK;
#endif
        sprintf(str, "%dW%d", year, week_num);
        return strlen(str);
    }
};

class TimeToString: public DataToString {
protected:
  Time time_;
public:
  TimeToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~TimeToString() {}
  virtual bool isNull (void *data) { return !time_.isValidTime(data); }
  virtual string toString(void *data) { return time_.toString(data); }
  virtual int toString(void *data, char *str)
  { return time_.BasicDateTime::strformat(data, str); }
};

class Time64ToString: public DataToString {
protected:
  Time *time_;
public:
  Time64ToString(int max_len, int precision): DataToString(max_len, sizeof(int64_t)) {
      time_ = new Time64(precision);
  }
  virtual ~Time64ToString() { delete time_; }
  virtual string toString(void *data) { return time_->toString(data); }
  virtual int toString(void *data, char *str)
  { return time_->BasicDateTime::strformat(data, str); }
};

class Date32ToString: public DataToString {
private:
  Date32 date_;
public:
  Date32ToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~Date32ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return BOEVALUE;    // NULL
  }
};

class DateTimeDOSToString: public DataToString {
private:
  DateTimeDOS date_;
public:
  DateTimeDOSToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~DateTimeDOSToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return BOEVALUE;
  }
};

class DateTime32ToString: public DataToString {
private:
  DateTime32 date_;
public:
  DateTime32ToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}

  virtual ~DateTime32ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return BOEVALUE;    // NULL
  }
};

class DateTime64ToString: public DataToString {
private:
  DateTime64 date_;
public:
  DateTime64ToString(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~DateTime64ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return BOEVALUE;  // NULL
  }
};


class ROWIDToString: public DataToString
{
public:
    ROWIDToString(): DataToString(sizeof(IDTYPE)) {};

    virtual ~ROWIDToString() {};

    virtual string toString(void *data) {
        stringstream ss;
#if defined(__UNALIGNED_ACCESS__)
        ss << '#' << *((IDTYPE *) data);
#else
        IDTYPE tmp;
        memcpy(&tmp , data , sizeof(IDTYPE));
        ss << '#' << tmp;
#endif
        return ss.str();
    }
};

class HLLToString: public DataToString {
protected:
    uint64_t getCount(void *data) { return (uint64_t)(HLLCount(data)); }
public:
    HLLToString(int max_len): DataToString(max_len, sizeof(int64_t)) {};
    virtual ~HLLToString() {};

    virtual string toString(void *data) {
        uint64_t cnt64 = getCount(data);
        return intToString(cnt64);
    }

    virtual int toString(void *data, char *str) {
        uint64_t cnt64 = getCount(data);
        sprintf(str, "%lu", cnt64);
        return strlen(str);
    }
};


class IPV4ToString: public DataToString {
  size_t socket_len_;   // length includes trailing NULL
public:
  IPV4ToString(int max_len): DataToString(max_len, sizeof(int32_t))
  { str_.resize(max_len); socket_len_ = max_len + 1; }
  virtual ~IPV4ToString() {};
  virtual string toString(void *data) {
    inet_ntop(AF_INET, data, str_.data(), socket_len_);
    return string(str_.data());
  }
  virtual int toString(void *data, char *str) {
    inet_ntop(AF_INET, data, str, socket_len_);
    return strlen(str);
  }
};


class IPV6ToString: public DataToString {
  size_t socket_len_;   // length includes trailing NULL
public:
  IPV6ToString(int max_len): DataToString(INET6_ADDRSTRLEN, sizeof(struct in6_addr))
{ str_.resize(INET6_ADDRSTRLEN); socket_len_= INET6_ADDRSTRLEN + 1; }
  virtual ~IPV6ToString() {};
  virtual string toString(void *data) {
    inet_ntop(AF_INET6, data, str_.data(), socket_len_);
    return string(str_.data());
  }
  virtual int toString(void *data, char *str) {
    inet_ntop(AF_INET6, data, str, socket_len_);
    return strlen(str);
  }
};


class BINARYToString: public DataToString {
protected:
  string s_;
public:
  BINARYToString(int max_len): DataToString(max_len, max_len)
  { s_.resize(max_len_ * 2); }
  virtual ~BINARYToString() {};

  virtual string toString(void *data) {
    char *s = (char *)s_.data();
    binaryToHexString((unsigned char *)data, s, max_len_);
    return string(s);
  }

  virtual int toString(void *data, char *str) {
    memcpy(str, data, max_len_);
    return max_len_;
  }
};

int PointToWKB(Point *pt, char *s) {
  if (pt->isNull())
    return -1;        // NULl
  *s++ = 1;  // little-endian
  *(int32_t *)s = wkbPoint;   // wkbPoint
  s += sizeof(int32_t);
  memcpy(s, pt, sizeof(Point));
  return 21;    // sizeof(Point) + 5;
}

class WKBPointDataToString: public DataToString {
protected:
  vector<char> str_;
  PointToString pts_;
public:
  WKBPointDataToString(int max_len): DataToString(max_len, sizeof(double) * 2),
  pts_(BigObjectConfig::precision()) { str_.resize(max_len + 1); }
  virtual ~WKBPointDataToString() {};

  virtual bool isNull (void *data) { return ((Point *)data)->isNull(); }

  virtual string toString(void *data) {
    if (((Point *)data)->isNull()) {
      return string(cstr_null);
    } else {
      char *s = (char *)str_.data();
      *s++ = '(';
      s = pts_.toString(s, (Point*)data);
      *s++ = ')';
      *s = '\0';
      return string(str_.data());
    }
  }

  virtual int toString(void *data, char *str)
  { return PointToWKB((Point *)data, str); }
};

class PointDataToString: public WKBPointDataToString {
public:
  PointDataToString(int max_len): WKBPointDataToString(max_len) {}
  virtual ~PointDataToString() {};

  virtual bool isNull (void *data) { return ((Point *)data)->isNull(); }

  virtual int toString(void *data, char *str) {
    if (((Point *)data)->isNull()) {
      return -1;   // NULL
    } else {
      *(int32_t *)str = 0;    // srid
      str += sizeof(int32_t);
      return WKBPointDataToString::toString(data, str) + sizeof(int32_t);
    }
  }
};



int setPrecision(const AttributeInfo &col_info, bool &is_rm_tz) {
//  if (col_info.extra < 0) {
    is_rm_tz = true;
    return BigObjectConfig::precision();
//  }
//  is_rm_tz = false;
//  return col_info.extra;
}

#if defined(KAIWU)
int setFloatPrecision(const AttributeInfo &col_info, bool &is_rm_tz) {
//  if (col_info.extra < 0) {
    is_rm_tz = true;
    return BigObjectConfig::float_precision();
//  }
//  is_rm_tz = false;
//  return col_info.extra;
}
#endif

class NULLToString: public DataToString {
public:
  NULLToString(): DataToString(0, 0) {}
  virtual ~NULLToString() {}
  virtual bool isNull (void *data) { return true; }
  virtual string toString(void *data) { return "NULL"; }
  virtual int toString(void *data, char *str) { return -1; }
};


DataToStringPtr getDataToStringHandler(const AttributeInfo &col_info,
  int encoding) {
  DataToString *vs;
  int max_len = dataTypeMaxTextLen(col_info);

  switch (col_info.type) {
    //case STRING:
    //  vs = new IDTYPEToString(max_len, ns);
    //  break;
    case BYTE_ARRAY:
    case BYTE:
      vs = new BYTEToString();
      break;
    case TIMESTAMP:
      vs = new IntegerDataToStringZN<int32_t>(max_len);
      break;
    case INT8_ARRAY:
    case INT8:
    case INT16_ARRAY:
    case INT16:
    case INT32_ARRAY:
    case INT32:
    case INT64_ARRAY:
    case INT64:
      vs = getDataToString(col_info.type, max_len);
      break;
    case TIMESTAMP64:
      vs = new TimeStamp64ToString(col_info.max_len);
//      vs = new UINT64DataToStringZN(max_len);
      break;
    case FLOAT_ARRAY:
    case FLOAT: {
      bool is_rm_tz;
#if defined(KAIWU)
      int prec = setFloatPrecision(col_info, is_rm_tz);
#else
      int prec = setPrecision(col_info, is_rm_tz);
#endif
      vs = new FloatDataToString<float> (max_len, prec, is_rm_tz);
      break;
    }
    case DOUBLE_ARRAY:
    case DOUBLE: {
      bool is_rm_tz;
      int prec = setPrecision(col_info, is_rm_tz);
      vs = new FloatDataToString<double> (max_len, prec, is_rm_tz);
      break;
    }
    case TIMESTAMP64_LSN:
      vs = new TimeStamp64LSNToString();
      break;
    case WEEK:
      vs = new WEEKTIMEToString(max_len);
      break;
    case DATE32:
      vs = new Date32ToString(max_len);
      break;
    case DATETIMEDOS:
      vs = new DateTimeDOSToString(max_len);
      break;
    case DATETIME32:
      vs = new DateTime32ToString(max_len);
      break;
    case DATETIME64:
      vs = new DateTime64ToString(max_len);
      break;
    case VARSTRING:
      vs = new VARSTRINGToString(max_len);
      break;
    case ROWID:
#ifndef ID64
      vs = new IntegerDataToString<int32_t>(max_len);
#else
      vs = new INT64DataToString(max_len);
#endif
      break;
//    case RAWSTRING:
    case STRING_CONST:
    case CHAR:
      vs = new CHARToString(max_len, col_info.size);
      break;
    case HYPERLOGLOG:
      vs = new HLLToString(col_info.size);
      break;
    case IPV4: vs = new IPV4ToString(max_len); break;
    case IPV6: vs = new IPV6ToString(max_len); break;
    case TIME: vs = new TimeToString(max_len); break;
    case TIME64: vs = new Time64ToString(max_len, col_info.max_len); break;
    //case TDIGEST: vs = new TDIGESTToString(max_len); break;
    case BINARY: vs = new BINARYToString(max_len); break;
    case VARBINARY:
      vs = new VARBINARYToString(max_len); break;
    case POINT: vs = new PointDataToString(max_len); break;
    //case LINESTRING:
    //  vs = new LineStringToString(max_len, (NameService *)ns); break;
    //case POLYGON:
    //  vs = new PolygonToString(max_len, (NameService *)ns); break;
    //case MULTIPOLYGON:
    //  vs = new MultiPolygonToString(max_len, (NameService *)ns); break;
    //case GEOMCOLLECT:
    //  vs = new GeomCollectToString(max_len, (NameService *)ns); break;
    //case GEOMETRY:
    //  vs = new GeometryToString(max_len, (NameService *)ns); break;
    //case WKB_POINT: vs = new WKBPointDataToString(max_len); break;
    //case WKB_LINESTRING:
    //  vs = new WKB_LineStringToString(max_len, (NameService *)ns); break;
    //case WKB_POLYGON:
    //  vs = new WKB_PolygonToString(max_len, (NameService *)ns); break;
    //case WKB_MULTIPOLYGON:
    //  vs = new WKB_MultiPolygonToString(max_len, (NameService *)ns); break;
    //case WKB_GEOMCOLLECT:
    //  vs = new WKB_GeomCollectToString(max_len, (NameService *)ns); break;
    //case WKB_GEOMETRY:
    //  vs = new WKB_GeometryToString(max_len, (NameService *)ns); break;
    case NULL_TYPE:
      vs = new NULLToString(); break;
    default:
      vs = new DataToString(max_len);
  }
  return DataToStringPtr(vs);
}

////////////////////////////////////////////////////////////////////
// DataToString

DataToString::~DataToString() {}

bool DataToString::isNull (void *data) { return false; }

string DataToString::toString(void *data) { return s_emptyString(); }

void * DataToString::dataAddrWithRdLock(void *data)
{ return (void *)s_emptyString().data(); }

void DataToString::unlock() {}

////////////////////////////////////////////////////////////////////
// StringToData

StringToData::StringToData() {
  has_default_ = false;
}

StringToData::~StringToData() { }

#if defined(IOT_MODE)
bool StringToData::isLatest() const { return false; }
void StringToData::setColumn(int col) {}
void StringToData::setLatestTable(timestamp64, BigTable *bt) {}
void StringToData::setRow(uint64_t row) {}
#endif

void StringToData::setDefaultValueAction(Action *dv_act) {
  return;
}

int StringToData::toData(void *addr)
{ return toData((char *)s_emptyString().c_str(), addr); }

int StringToData::toDefaultData(void * addr) {

  return 0;
}

int StringToData::toDefaultBinaryData(void * addr) {
  return 0;
}

int StringToData::toData(char *str, void * addr) {
  return 0;
}

//int StringToData::toData2(char *str, void * addr) { return -1; }

int StringToData::toBinaryData(char *str, void * addr) { return 0; }

int StringToData::updateData(char *s, void *data) { return toData(s, data); }

int StringToData::updateBinaryData(char *s, void *data)
{ return toBinaryData(s, data); }

void StringToData::noPushToData(char *str, void *addr) { toData(str, addr); }

void StringToData::clear() {}


// integer to string:  NULL is converted to Zero
template<typename T, int d_type>
class IntegerStringToDataZN: public StringToData {
public:
  IntegerStringToDataZN() {
  }
  virtual ~IntegerStringToDataZN() {}
  virtual int toData(char *str, void *data) {
#if defined(__UNALIGNED_ACCESS__)
#if defined(KAIWU)
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    if (isEqualIgnoreCase(str, "true")) {
      str = const_cast<char*>("1\0");
    } else if (isEqualIgnoreCase(str, "false")) {
      str = const_cast<char*>("0\0");
    }
    char *end_ptr;
    errno = 0;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
      return BOEDATA;
    }
    if ((v < std::numeric_limits<T>::min())
      || (v > std::numeric_limits<T>::max()))
      return BOERANGE;
    *((T*)data) = (T)v;
    return 0;
#else
    *((T*)data) = (T)(atoi(str));
#endif
#else
    T tmp = (T)(atoi(str));
    memcpy(data,&tmp, sizeof(T) );
#endif
    return 0;
  }
};

// integer to string:  NULL is converted to minimum integer number
template<typename T, int d_type>
class IntegerStringToData: public StringToData {
public:
  IntegerStringToData() {
  }
  virtual ~IntegerStringToData() {}
  virtual int toData(char *str, void *data) {
#if defined(__UNALIGNED_ACCESS__)
#if defined(KAIWU)
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    if (isEqualIgnoreCase(str, "true")) {
      strcpy(str, "1");
    } else if (isEqualIgnoreCase(str, "false")) {
      strcpy(str, "0");
    }
    char *end_ptr;
    errno = 0;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
      return BOEDATA;
    }
    if ((v < std::numeric_limits<T>::min())
      || (v > std::numeric_limits<T>::max()))
      return BOERANGE;
    *((T*)data) = (T)v;
    return 0;
#else
    if (isNULL(str)) {
      *((T*)data) = std::numeric_limits<T>::min();
    } else {
      *((T*)data) = (T)(atoi(str));
    }
#endif
#else
    T tmp;
    if (isNULL(str)) {
      tmp = std::numeric_limits<T>::min();
    } else {
      tmp = (T)(atoi(str));
    }
    memcpy(data,&tmp, sizeof(T) );
#endif
    return 0;
  }

};

class LongStringToData: public StringToData {
public:
  LongStringToData(): StringToData() {
  }
  virtual ~LongStringToData() {}

  virtual int toData(char *str, void *data) {
#if defined(KAIWU)
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    errno = 0;
    char *end_ptr;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
      return BOEDATA;
    }
    if ((v < std::numeric_limits<int64_t>::min())
      || (v > std::numeric_limits<int64_t>::max()))
      return BOERANGE;
    *((int64_t*)data) = (int64_t)v;
    return 0;
#else
    if (isNULL(str)) {
      *((int64_t *)data) = std::numeric_limits<int64_t>::min();
    } else {
// avoid alignment exception in arm cpu (raspberry pi)
#if defined(__UNALIGNED_ACCESS__)
      *((int64_t *)data) = std::atol((const char *)str);
#else
      int64_t tmp = (int64_t)(atol(str));
      memcpy(data, &tmp, sizeof(int64_t) );
#endif

    }
    return 0;
#endif
  }

//  virtual int toData2(char *str, void *data) {
//#if !defined(KAIWU)
//    if (isNULL(str)) {
//      *((int64_t *)data) = std::numeric_limits<int64_t>::min();
//      return 0;
//    }
//#endif
//    errno = 0;
//    char *end_ptr;
//    int64_t v = strtol(str, &end_ptr, 10);
//    if (errno == ERANGE || *end_ptr != '\0') {
//      return -1;
//    }
//    *((int64_t*)data) = v;
//    return 0;
//  }
};

class LongStringToDataZN: public StringToData {
public:
  LongStringToDataZN(): StringToData() {
  }
  virtual ~LongStringToDataZN() {}
  virtual int toData(char *str, void *data) {
#if defined(__UNALIGNED_ACCESS__)
#if defined(KAIWU)
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    errno = 0;
    char *end_ptr;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
      return BOEDATA;
    }
    if ((v < std::numeric_limits<int64_t>::min())
      || (v > std::numeric_limits<int64_t>::max()))
      return BOERANGE;
    *((int64_t*)data) = (int64_t)v;
    return 0;
#else
    *((int64_t *)data) = std::atol((const char *)str);
#endif
#else
      int64_t tmp = (int64_t)(atol(str));
      memcpy(data, &tmp, sizeof(int64_t) );
#endif
    return 0;
  }
};

class StringToTIMESTAMP64: public StringToData {
public:
  StringToTIMESTAMP64(): StringToData() {
  }
  virtual ~StringToTIMESTAMP64() {}

  virtual int toData(char *str, void *data) {
#if defined(__UNALIGNED_ACCESS__)
    *((uint64_t *)data) = strtoul((const char *)str, NULL, 0);
#else
      uint64_t tmp = strtoul((const char *)str, NULL, 0);
      memcpy(data, &tmp, sizeof(uint64_t) );
#endif
    return 0;
  }
};

class FLOATStringToData: public StringToData {
protected:
#if defined(IOT_MODE)
  timestamp64 ts_;
  BigTable *latest_bt_;
  int col_;
  bool isfinite_;      // will be set for each input data
#endif
public:
  FLOATStringToData() {

#if defined(IOT_MODE)
    latest_bt_ = nullptr;
#endif
  }
  virtual ~FLOATStringToData() {}
#if defined(IOT_MODE)
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual void setLatestTable(timestamp64 ts, BigTable *bt) {
    ts_= ts;
    latest_bt_ = bt;
  }
  virtual void setRow(uint64_t row) {
    if (latest_bt_ && isfinite_) {
      latest_bt_->mutexLock();
      latest_bt_->setColumnValue(col_, 0, &ts_);
      latest_bt_->setColumnValue(col_, 1, &row);
      latest_bt_->mutexUnlock();
    }
  }
#endif
  virtual int toData(char *str, void *data) {
#if !defined(KAIWU)
    if (isNULL(str)) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      *((float*) data) = std::numeric_limits<float>::quiet_NaN();
      return 0;
    }
#endif
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
 #if defined(IOT_MODE)
       isfinite_ = false;
 #endif
       return BOEDATA;
     }
    if ((v < std::numeric_limits<float>::lowest())
      || (v > std::numeric_limits<float>::max())) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOERANGE;
    }
    *((float*) data) = (float) v;
#if defined(IOT_MODE)
    isfinite_ = true;
#endif
    return 0;
  }
};

class DOUBLEStringToData: public StringToData {
protected:
#if defined(IOT_MODE)
  timestamp64 ts_;
  BigTable *latest_bt_;
  int col_;
  bool isfinite_;      // will be set for each input data
#endif
public:
  DOUBLEStringToData() {
#if defined(IOT_MODE)
    latest_bt_ = nullptr;
#endif
  }
  virtual ~DOUBLEStringToData() {}
#if defined(IOT_MODE)
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual void setLatestTable(timestamp64 ts, BigTable *bt) {
    ts_= ts;
    latest_bt_ = bt;
  }
  virtual void setRow(uint64_t row) {
    if (latest_bt_ && isfinite_) {
      latest_bt_->mutexLock();
      latest_bt_->setColumnValue(col_, 0, &ts_);
      latest_bt_->setColumnValue(col_, 1, &row);
      latest_bt_->mutexUnlock();
    }
  }
#endif
  virtual int toData(char *str, void *data) {
#if !defined(KAIWU)
    if (isNULL(str)) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      *((double*) data) = std::numeric_limits<double>::quiet_NaN();
      return 0;
    }
#endif
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOERANGE;
    }
    if (*end_ptr != '\0') {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOEDATA;
    }

    *((double*) data) = (double) v;
#if defined(IOT_MODE)
    isfinite_ = true;
#endif
    return 0;
  }
};

template<typename T, int d_type>
class FloatStringToData: public StringToData {
protected:
#if defined(IOT_MODE)
  timestamp64 ts_;
  BigTable *latest_bt_;
  int col_;
  bool isfinite_;      // will be set for each input data
#endif
public:
  FloatStringToData() {
#if defined(IOT_MODE)
    latest_bt_ = nullptr;
#endif
  }
  virtual ~FloatStringToData() {}
#if defined(IOT_MODE)
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual void setLatestTable(timestamp64 ts, BigTable *bt) {
    ts_= ts;
    latest_bt_ = bt;
  }
  virtual void setRow(uint64_t row) {
    if (latest_bt_ && isfinite_) {
      latest_bt_->mutexLock();
      latest_bt_->setColumnValue(col_, 0, &ts_);
      latest_bt_->setColumnValue(col_, 1, &row);
      latest_bt_->mutexUnlock();
    }
  }
#endif
  virtual int toData(char *str, void *data) {
#if !defined(KAIWU)
    if (isNULL(str)) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      *((T*) data) = std::numeric_limits<T>::quiet_NaN();
      return 0;
    }
#endif
    if (isEmpty(str) && !has_default_) {
      return BOEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE || *end_ptr != '\0') {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOERANGE;
    }
    if ((v < std::numeric_limits<T>::lowest())
      || (v > std::numeric_limits<T>::max())) {
#if defined(IOT_MODE)
      isfinite_ = false;
#endif
      return BOERANGE;
    }
    *((T*) data) = (T) v;
#if defined(IOT_MODE)
    isfinite_ = true;
//    isfinite_ = isfinite(*((T*) data));
#endif
    return 0;

  }

};

template<typename T, int d_type>
class FloatStringToDataZN: public FloatStringToData<T, d_type> {
public:
  FloatStringToDataZN(): FloatStringToData<T, d_type>() {}
  virtual ~FloatStringToDataZN() {}
  virtual int toData(char *str, void *data) {
#if defined(__UNALIGNED_ACCESS__)
#if defined(KAIWU)
    if (isEmpty(str)) {
      return BOEDATA;
    }
    char *end_ptr;
    errno = 0;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE)
      return BOERANGE;
    if (*end_ptr != '\0')
      return BOEDATA;
    *((T*)data) = (T)v;
    return 0;
#else
    *((T *)data) = (T)(std::atof(str));
#endif
#if defined(IOT_MODE)
    this->isfinite_ = isfinite(*((T*)data));
#endif
#else
    T tmp = (T)(std::atof(str));
    memcpy(data,&tmp, sizeof(T) );
#endif
    return 0;
  }
};

StringToData *getStringToData(int type) {
  if (BigObjectConfig::zeroIfNull()) {
    switch(type) {
      case INT8: return new IntegerStringToDataZN<int8_t, INT8>();
      case INT16: return new IntegerStringToDataZN<int16_t, INT16>();
      case INT32: return new IntegerStringToDataZN<int32_t,INT32>();
      case INT64: return new LongStringToDataZN();
      case FLOAT: return new FloatStringToDataZN<float, FLOAT>();
      case DOUBLE: return new FloatStringToDataZN<double, DOUBLE>();
    }
  } else {
    switch(type) {
      case INT8: return new IntegerStringToData<int8_t, INT8>();
      case INT16: return new IntegerStringToData<int16_t, INT16>();
      case INT32: return new IntegerStringToData<int32_t, INT32>();
      case INT64: return new LongStringToData();
//      case FLOAT: return new FloatStringToData<float, FLOAT>();
//      case DOUBLE: return new FloatStringToData<double, DOUBLE>();
      case FLOAT: return new FLOATStringToData();
      case DOUBLE: return new DOUBLEStringToData();
    }
  }
  return new StringToData(); // useless
}

class StringToIDTYPE: public StringToData {
protected:
  int max_len_;
public:
  explicit StringToIDTYPE(int max_len) {
    max_len_ = (max_len <= 0) ? DEFAULT_STRING_MAX_LEN : max_len;
  }

  virtual ~StringToIDTYPE() {}

  virtual int toData(char *str, void *data) {
#if defined(KAIWU)
    if (strlen(str) > max_len_)
      return BOEVALUE;
#endif
//
//    IDTYPE id = ns_->pushStringToId(str, max_len_);
//
//    if (id != (IDTYPE)(-1)) {
//#if defined(__UNALIGNED_ACCESS__)
//      *(IDTYPE*)data = id;
//#else
//      memcpy(data, &id, sizeof(IDTYPE) );
//#endif
//      return 0;
//    }
    return BOENAMESERVICE;
  }

  virtual void noPushToData(char *str, void *data) {}
};


class StringToCHAR: public StringToData {
protected:
  int size_;
public:
  StringToCHAR(int s): size_(s) {
    string str;
    str.resize(size_, 0); // TODO: default value \x0 or ' '?
  }

  virtual ~StringToCHAR() {}

  virtual int toData(char *str, void *data) {
#if defined(KAIWU)
    if (strlen(str) > size_) {
      return BOEVALUE;
    }

#endif
    int cpy_len = CHARcpy((char *)data, str, size_);
    if (cpy_len > 0) {
      unsigned char c = ((unsigned char*) data)[cpy_len - 1] & 0xC0;
      if (c == 0xC0) { // 0x1100---- | 0x1110----
        cpy_len--;
      } else if (c == 0x80) {
        if (cpy_len < 2)
          cpy_len = 0;
        else if ((((unsigned char*) data)[cpy_len - 2] & 0xE0) == 0xE0) {
          cpy_len -= 2;     // 0x1110---- 0x10------
        }
      }
    }
    if (cpy_len < size_)
      memset(offsetAddr(data, cpy_len), 0, size_ - cpy_len);
    return 0;
  }
};

#if defined (USE_SMART_INDEX)
class StringToINDEX: public StringToIDTYPE
{
private:
    string dim_attr_;

public:
    StringToINDEX(void *ns, const string &dim_attr) :
  StringToIDTYPE(ns)
    {
  dim_attr_ = dim_attr;
    }

    virtual ~StringToINDEX() {};

    virtual void toData(const std::string & str, void *data)
    {
        IDTYPE tmp = ns_->pushStringToIndex(dim_attr_, str);
        *(IDTYPE*)data = tmp;
    }
};
#endif

class StringToWEEKTIME: public StringToData {
public:
  StringToWEEKTIME() {
  }
  virtual ~StringToWEEKTIME() {}
  virtual int toData(char *str, void *data) {
    int32_t year_week = toYearWeek(str);
    *((int32_t *)data) = year_week;
    return 0;
  }
};

template <typename T>
class StringToDateTime: public StringToData {
protected:
  T dt_;
public:
  StringToDateTime(int prec) {
    dt_.setPrecision(prec);
  }
  virtual ~StringToDateTime() {};
  virtual int toData(char *str, void *data) {
    if (!dt_.setDateTime(str, data)) {
      return BOEDATEVALUE;
    }
    return 0;
  }
//  virtual int toData2(char *str, void *data) {
//    if (!dt_.setDateTime(str, data)) {
//      return -1;
//    }
//    return 0;
//  }
};

class StringToBINARY: public StringToData {
protected:
  int max_len_;
public:
  StringToBINARY(int max_len): max_len_(max_len) {
    string str;
    str.resize(max_len_, 0);
  }
  virtual ~StringToBINARY() {}
  virtual int toData(char *str, void *data) {
    int len = unHex(str, (unsigned char *)data, max_len_);
    if (len >= 0)
      memset(offsetAddr(data, len), 0, max_len_ - len);
    return len;
  }
  virtual int toData2(char *str, void *data) {
    int len = unHex(str, (unsigned char *)data, max_len_);
    if (len >= 0)
      memset(offsetAddr(data, len), 0, max_len_ - len);
    return len;
  }
};


StringToDataPtr getStringToDataHandler(BigTable *bt, int col,
  const AttributeInfo &col_info, int encoding,
  const string &time_format) {
  StringToData *vs;
  switch (col_info.type) {
    case STRING:
#if !defined(USE_SMART_INDEX)
      vs = new StringToIDTYPE(col_info.max_len);
#else
      vs =
        (encoding == DICTIONARY) ? new StringToIDTYPE(ns) :
          new StringToINDEX(ns, dim_attr);
#endif
      break;
    case CHAR:
      vs = new StringToCHAR(col_info.max_len);
      break;
    case VARSTRING:
      vs = new StringToVARSTRING(bt, col, col_info.max_len);
      break;
    case TIMESTAMP:
      vs = new IntegerStringToDataZN<int32_t, INT32>;
      break;
    case TIMESTAMP64:
//      vs = new StringToTIMESTAMP64();
      vs = new StringToDateTime<TimeStamp64DateTime>(col_info.max_len);
      break;
    case INT8_ARRAY:
    case INT8:
    case INT16_ARRAY:
    case INT16:
    case INT32_ARRAY:
    case INT32:
    case INT64_ARRAY:
    case INT64:
    case FLOAT_ARRAY:
    case FLOAT:
    case DOUBLE_ARRAY:
    case DOUBLE:
      vs = getStringToData(col_info.type);
      break;
    case WEEK:
      vs = new StringToWEEKTIME;
      break;
    case DATE32:
      vs = new StringToDateTime<Date32>(col_info.max_len);
      break;
    case DATETIMEDOS:
      vs = new StringToDateTime<DateTimeDOS>(col_info.max_len);
      break;
    case DATETIME32:
      vs = new StringToDateTime<DateTime32>(col_info.max_len);
      break;
    case DATETIME64:
      vs = new StringToDateTime<DateTime64>(col_info.max_len);
      break;
    case ROWID:
#ifndef ID64
      vs = new IntegerStringToData<int32_t, INT32>;
#else
      vs = new IntegerStringToData<int64_t>;
#endif
      break;
    case BINARY: vs = new StringToBINARY(col_info.max_len); break;
    case VARBINARY:
      vs = new StringToVARBINARY(bt, col, col_info.max_len); break;
    //case LINESTRING:
    //case WKB_LINESTRING:
    //  vs = new StringToLINESTRING(bt, col, col_info.max_len); break;
    //case POLYGON:
    //case WKB_POLYGON:
    //  vs = new StringToPolygon(bt, col, col_info.max_len); break;
    //case MULTIPOLYGON:
    //case WKB_MULTIPOLYGON:
    //  vs = new StringToMultiPolygon(bt, col, col_info.max_len); break;
    //case GEOMCOLLECT:
    //case WKB_GEOMCOLLECT:
    //  vs = new StringToGeomCollect(bt, col, col_info.max_len); break;
    //case GEOMETRY:
    //case WKB_GEOMETRY:
    //  vs = new StringToGeometry(bt, col, col_info.max_len); break;
    default:
      vs = new StringToData;
  }

  return StringToDataPtr(vs);
//    }

}

CONVERT_DATA_FUNC getConvertFunc(int32_t old_data_type, int32_t new_data_type,
                                int32_t new_length, bool& is_digit_data, ErrorInfo& err_info) {
  is_digit_data = false;
  err_info.errcode = 0;
  switch (old_data_type)  {
  case DATATYPE::INT16:
    is_digit_data = true;
    if (new_data_type == DATATYPE::INT32) {
      return convertFixDataToData<int16_t, int32_t, false>;
    } else if (new_data_type == DATATYPE::INT64) {
      return convertFixDataToData<int16_t, int32_t, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int16_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::INT32:
    is_digit_data = true;
    if (new_data_type == DATATYPE::INT64) {
      return convertFixDataToData<int32_t, int64_t, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int32_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::INT64:
    is_digit_data = true;
    if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int64_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::FLOAT:
    is_digit_data = true;
    if (new_data_type == DATATYPE::DOUBLE) {
      return convertFixDataToData<float, double, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<float, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::DOUBLE:
    is_digit_data = true;
    if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<double, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::CHAR:
    if (new_data_type == DATATYPE::VARSTRING ||
        new_data_type == DATATYPE::BINARY ||
        new_data_type == DATATYPE::CHAR) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::BINARY:
    if (new_data_type == DATATYPE::VARSTRING ||
        new_data_type == DATATYPE::CHAR ||
        new_data_type == DATATYPE::BINARY) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::VARSTRING:
    if (new_data_type == DATATYPE::INT16 ) {
      return convertStringToFixData<DATATYPE::INT16>;   
    } else if (new_data_type == DATATYPE::INT32) {
      return convertStringToFixData<DATATYPE::INT32>;
    } else if (new_data_type == DATATYPE::INT64) {
      return convertStringToFixData<DATATYPE::INT64>;
    } else if (new_data_type == DATATYPE::FLOAT) {
      return convertStringToFixData<DATATYPE::FLOAT>;
    } else if (new_data_type == DATATYPE::DOUBLE) {
      return convertStringToFixData<DATATYPE::DOUBLE>;
    } else if (new_data_type == DATATYPE::CHAR ||
               new_data_type == DATATYPE::BINARY ||
               new_data_type == DATATYPE::VARSTRING) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::VARBINARY:
  default:
    err_info.errcode = -1;
    break;
  }
  return nullptr;
}
