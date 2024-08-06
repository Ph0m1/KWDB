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

#include "ee_field_func.h"

#include <time.h>

#include <cmath>
#include <exception>
#include <iomanip>
#include <random>
#include <sstream>

#include "ee_crc32.h"
#include "ee_field_common.h"
#include "ee_fnv.h"
#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

void FieldFuncOp::CalcStorageType() {
  for (k_int32 i = 0; i < arg_count_; ++i) {
    if (roachpb::DataType::FLOAT == args_[i]->get_storage_type() ||
        roachpb::DataType::DOUBLE == args_[i]->get_storage_type()) {
      sql_type_ = roachpb::DataType::DOUBLE;
      storage_type_ = roachpb::DataType::DOUBLE;
      storage_len_ = sizeof(k_double64);
      return;
    }
  }
  sql_type_ = roachpb::DataType::BIGINT;
  storage_type_ = roachpb::DataType::BIGINT;
  storage_len_ = sizeof(k_int64);
}

k_int64 FieldFuncPlus::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->is_nullable()) {
        if (args_[i]->get_field_type() == FIELD_INTERVAL) {
          if (i == 0) {
            val = args_[1]->ValInt();
            val = args_[i]->ValInt(&val, KFALSE);
            ++i;
          } else {
            val = args_[i]->ValInt(&val, KFALSE);
          }
        } else {
          k_int64 arg_val = args_[i]->ValInt();
          if (I64_SAFE_ADD_CHECK(val, arg_val)) {
            val += arg_val;
          } else {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                          "integer out of range");
            return 0;
          }
        }
      }
    }

    return val;
  }
}

k_double64 FieldFuncPlus::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else if (storage_type_ == roachpb::DataType::BIGINT) {
    return ValInt();
  } else {
    k_double64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->is_nullable()) val += args_[i]->ValReal();
    }

    return val;
  }
}

String FieldFuncPlus::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValStr(ptr);
  } else {
    std::string val = "";
    for (size_t i = 0; i < arg_count_; ++i) {
      if (!args_[i]->is_nullable()) {
        String s1 = args_[i]->ValStr();
        val += std::string(s1.getptr(), s1.length_);
      }
    }

    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%s", val.c_str());
    s.length_ = strlen(s.ptr_);
    return s;
    // return val;
  }
}

Field *FieldFuncPlus::field_to_copy() {
  FieldFuncPlus *field = new FieldFuncPlus(*this);

  return field;
}

k_int64 FieldFuncMinus::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 0;
    val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      if (args_[i]->get_field_type() == FIELD_INTERVAL) {
        val = args_[i]->ValInt(&val, KTRUE);
      } else {
        val -= args_[i]->ValInt();
      }
    }
    return val;
  }
}

k_double64 FieldFuncMinus::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = 0;
    val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      val -= args_[i]->ValReal();
    }
    return val;
  }
}

String FieldFuncMinus::ValStr() { return String(""); }

Field *FieldFuncMinus::field_to_copy() {
  FieldFuncMinus *field = new FieldFuncMinus(*this);

  return field;
}

k_int64 FieldFuncMult::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = 1;
    for (size_t i = 0; i < arg_count_; ++i) {
      val *= args_[i]->ValInt();
    }

    return val;
  }
}

k_double64 FieldFuncMult::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = 1;
    for (size_t i = 0; i < arg_count_; ++i) {
      val *= args_[i]->ValReal();
    }

    return val;
  }
}

String FieldFuncMult::ValStr() { return String(""); }

Field *FieldFuncMult::field_to_copy() {
  FieldFuncMult *field = new FieldFuncMult(*this);

  return field;
}

k_int64 FieldFuncDivide::ValInt() { return ValReal(); }

k_double64 FieldFuncDivide::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      if (newval == 0) {
        return val /= newval;
      }
      val /= newval;
    }

    return val;
  }
}

String FieldFuncDivide::ValStr() { return String(""); }

Field *FieldFuncDivide::field_to_copy() {
  FieldFuncDivide *field = new FieldFuncDivide(*this);

  return field;
}

k_bool FieldFuncDivide::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->is_nullable()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      if (FLT_EQUAL(args_[0]->ValReal(), 0.0)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                      "division undefined");
        return false;
      }
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                    "division by zero");
      return false;
    }
  }

  return false;
}

k_int64 FieldFuncDividez::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 v = args_[i]->ValInt();
      if (v == 0) {
        return 0;
      }
      val /= args_[i]->ValInt();
    }
    return val;
  }
}

k_double64 FieldFuncDividez::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();

    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 v = args_[i]->ValInt();
      if (FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
        return 0.0;
      }
      val /= args_[i]->ValReal();
    }
    val = std::floor(val);
    return val;
  }
}

String FieldFuncDividez::ValStr() { return String(""); }

Field *FieldFuncDividez::field_to_copy() {
  FieldFuncDividez *field = new FieldFuncDividez(*this);

  return field;
}

k_bool FieldFuncDividez::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->is_nullable()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO,
                                    "division by zero");
      return false;
    }
  }

  return false;
}

k_int64 FieldFuncRemainder::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val1 = args_[0]->ValInt();
    k_int64 val2 = args_[1]->ValInt();
    if (val2 == 0) {
      return val1;
    }
    k_int64 val = val1 ^ val2;

    return val;
  }
}

k_double64 FieldFuncRemainder::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_int64 val1 = args_[0]->ValReal();
    k_int64 val2 = args_[1]->ValReal();
    if (val2 == 0) {
      return val1;
    }
    k_int64 val = val1 ^ val2;

    return val;
  }
}

String FieldFuncRemainder::ValStr() { return String(""); }

Field *FieldFuncRemainder::field_to_copy() {
  FieldFuncRemainder *field = new FieldFuncRemainder(*this);

  return field;
}

k_int64 FieldFuncPercent::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      if (newval == 0) {
        return val /= newval;
      }
      val %= args_[i]->ValInt();
    }

    return val;
  }
}

k_double64 FieldFuncPercent::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_double64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_double64 newval = args_[i]->ValReal();
      val = std::fmod(val, newval);
    }

    return val;
  }
  // char *ptr = get_ptr();
  // if (ptr) {
  //   return FieldFuncOp::ValInt(ptr);
  // } else {
  //   k_double64 x = args_[0]->ValReal();
  //   for (size_t i = 1; i < arg_count_; ++i) {
  //     k_double64 y = args_[i]->ValReal();
  //     y = std::abs(y);
  //     k_double64 yfr = 0.0f;
  //     k_int32 yexp = 0;
  //     yfr = std::frexp(newval, &yexp);
  //     k_double64 r = x;
  //     if (x < 0) {
  //       r = -x;
  //     }

  //     while(r >= y) {
  //       k_double64 rfr = 0.0f;
  //       k_int32 rexp = 0;
  //       rfr = std::frexp(newval, &rexp);
  //       if (rfr < yfr) {
  //         rexp = rexp - 1;
  //       }
  //     }
  //   }

  //   return val;
  // }
}

String FieldFuncPercent::ValStr() { return String(""); }

Field *FieldFuncPercent::field_to_copy() {
  FieldFuncPercent *field = new FieldFuncPercent(*this);

  return field;
}

k_bool FieldFuncPercent::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->is_nullable()) {
      return true;
    } else if (i != 0 && FLT_EQUAL(args_[i]->ValReal(), 0.0)) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DIVISION_BY_ZERO, "zero modulus");
      return false;
    }
  }

  return false;
}

k_int64 FieldFuncPower::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 res = 0;
    k_int64 val = args_[0]->ValInt();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 tmp = args_[i]->ValInt();
      res = pow(val, tmp);
    }

    return res;
  }
}

k_double64 FieldFuncPower::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValReal(ptr);
  } else {
    k_int64 res = 0;
    k_int64 val = args_[0]->ValReal();
    for (size_t i = 1; i < arg_count_; ++i) {
      k_int64 tmp = args_[i]->ValReal();
      res = pow(val, tmp);
    }

    return res;
  }
}

String FieldFuncPower::ValStr() { return String(""); }

Field *FieldFuncPower::field_to_copy() {
  FieldFuncPower *field = new FieldFuncPower(*this);

  return field;
}

k_int64 FieldFuncMod::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_bool first = true;
    k_int64 val = 0;
    for (size_t i = 0; i < arg_count_; ++i) {
      if (first) {
        val = args_[i]->ValInt();
        first = false;
      } else if (!first) {
        val %= args_[i]->ValInt();
      }
    }

    return val;
  }
}

k_double64 FieldFuncMod::ValReal() { return ValInt(); }

String FieldFuncMod::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncMod::field_to_copy() {
  FieldFuncMod *field = new FieldFuncMod(*this);

  return field;
}

k_int64 FieldFuncAndCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        result &= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncAndCal::ValReal() { return ValInt(); }

String FieldFuncAndCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncAndCal::field_to_copy() {
  FieldFuncAndCal *field = new FieldFuncAndCal(*this);

  return field;
}

k_int64 FieldFuncOrCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        result |= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncOrCal::ValReal() { return ValInt(); }

String FieldFuncOrCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncOrCal::field_to_copy() {
  FieldFuncOrCal *field = new FieldFuncOrCal(*this);

  return field;
}

k_int64 FieldFuncNotCal::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      k_int64 val = args_[i]->ValInt();
      result = ~val;
    }

    return result;
  }
}

k_double64 FieldFuncNotCal::ValReal() { return ValInt(); }

String FieldFuncNotCal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncNotCal::field_to_copy() {
  FieldFuncNotCal *field = new FieldFuncNotCal(*this);

  return field;
}

k_int64 FieldFuncLeftShift::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        if (val < 0 || val >= 64) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "shift argument out of range");
          return 0;
        }
        result <<= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncLeftShift::ValReal() { return ValInt(); }

String FieldFuncLeftShift::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncLeftShift::field_to_copy() {
  FieldFuncLeftShift *field = new FieldFuncLeftShift(*this);

  return field;
}

k_int64 FieldFuncRightShift::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    k_int64 result = 1;

    for (size_t i = 0; i < arg_count_; ++i) {
      if (i == 0) {
        result = args_[i]->ValInt();
      } else {
        k_int64 val = args_[i]->ValInt();
        if (val < 0 || val >= 64) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                        "shift argument out of range");
          return 0;
        }
        result >>= val;
      }
    }

    return result;
  }
}

k_double64 FieldFuncRightShift::ValReal() { return ValInt(); }

String FieldFuncRightShift::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncRightShift::field_to_copy() {
  FieldFuncRightShift *field = new FieldFuncRightShift(*this);

  return field;
}

k_int64 FieldFuncTimeBucket::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncOp::ValInt(ptr);
  } else {
    try {
      int64_t intervalSeconds;
      std::string timestring = {args_[1]->ValStr().getptr(),
                                args_[1]->ValStr().length_};
      std::string intervalStr = timestring.substr(0, timestring.size() - 1);
      intervalSeconds = stoll(intervalStr);
      intervalSeconds *= 1000;

      auto newTime = args_[0]->ValInt();
      auto timebucket = newTime - newTime % intervalSeconds;
      return timebucket;
    } catch (const std::exception &e) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "second arg is error");
      return 0;
    }
  }
}

k_double64 FieldFuncTimeBucket::ValReal() { return ValInt(); }

String FieldFuncTimeBucket::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncTimeBucket::field_to_copy() {
  FieldFuncTimeBucket *field = new FieldFuncTimeBucket(*this);

  return field;
}

k_bool FieldFuncTimeBucket::field_is_nullable() {
  double intervalSeconds;
  std::string timestring = {args_[1]->ValStr().getptr(),
                            args_[1]->ValStr().length_};
  if (timestring.back() == 's') {
    std::string intervalStr = timestring.substr(0, timestring.size() - 1);
    try {
      intervalSeconds = stod(intervalStr);
      if (floor(intervalSeconds) != intervalSeconds) {
        EEPgErrorInfo::SetPgErrorInfo(
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            "Second arg should be an integral seconds");
        return false;
      }
    } catch (const std::exception &e) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    "Second arg should be an integral seconds");
      return false;
    }
  } else {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        "Second arg should be in seconds, format: '10s'");
    return false;
  }

  if (intervalSeconds <= 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                  "Second arg should be a positive seconds");
    return false;
  }

  return false;
}

k_int64 FieldFuncCurrentDate::ValInt() {
  time_t t = time(nullptr);
  struct tm ltm;
  localtime_r(&t, &ltm);
  ltm.tm_hour = 0;
  ltm.tm_min = 0;
  ltm.tm_sec = 0;
  return k_int64(mktime(&ltm) * 1000);
}

k_double64 FieldFuncCurrentDate::ValReal() { return ValInt(); }

String FieldFuncCurrentDate::ValStr() {
  char buffer[80];
  time_t t = time(nullptr);
  struct tm ltm;
  localtime_r(&t, &ltm);
  strftime(buffer, 80, "%Y-%m-%d", &ltm);
  String s(80);
  snprintf(s.ptr_, 80 + 1, "%s", buffer);
  s.length_ = strlen(buffer);
  return s;
}

Field *FieldFuncCurrentDate::field_to_copy() {
  FieldFuncCurrentDate *field = new FieldFuncCurrentDate(*this);

  return field;
}

k_int64 FieldFuncCurrentTimeStamp::ValInt() {
  auto duration_since_epoch =
      std::chrono::system_clock::now().time_since_epoch();
  auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                            duration_since_epoch)
                            .count();
  ms_since_epoch /= 1000;
  time_t t = time(nullptr);
  struct tm ltm;
  localtime_r(&t, &ltm);
  ms_since_epoch -= ltm.tm_gmtoff - time_zone * 3600;
  // add precision handle
  if (arg_count_ > 0) {
    k_int64 prec = 3 - args_[0]->ValInt();
    if (prec < 0) {
      prec = 0;
    } else if (prec > 3) {
      prec = 3;
    }
    int64_t mask = pow(10, prec);
    ms_since_epoch = ms_since_epoch / mask * mask;
  }
  return ms_since_epoch;
}

k_double64 FieldFuncCurrentTimeStamp::ValReal() { return ValInt(); }

String FieldFuncCurrentTimeStamp::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncCurrentTimeStamp::field_to_copy() {
  FieldFuncCurrentTimeStamp *field = new FieldFuncCurrentTimeStamp(*this);

  return field;
}

k_int64 FieldFuncNow::ValInt() {
  auto duration_since_epoch =
      std::chrono::system_clock::now().time_since_epoch();
  auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                            duration_since_epoch)
                            .count();
  return ms_since_epoch / 1000;
}

k_double64 FieldFuncNow::ValReal() { return ValInt(); }

String FieldFuncNow::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncNow::field_to_copy() {
  FieldFuncNow *field = new FieldFuncNow(*this);

  return field;
}

k_int64 FieldFuncDateTrunc::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    k_int64 ti = args_[1]->ValInt();
    k_int64 tim = ti / 1000;
    struct tm ltm;
    if (this->return_type_ == KWDBTypeFamily::TimestampTZFamily) {
      tim += time_zone * 3600;
    }
    gmtime_r(&tim, &ltm);
    std::string code = {args_[0]->ValStr().getptr(),
                        args_[0]->ValStr().length_};
    if (code == "microsecond") {
      return ti * 1000;
    } else if (code == "millisecond" || code == "milliseconds" ||
               code == "ms") {
      return ti;
    } else if (code == "second" || code == "seconds" || code == "s") {
      return ti - ti % 1000;
    } else if (code == "minute" || code == "minutes" || code == "m") {
      return ti - ti % (1000 * 60);
    }
    ltm.tm_sec = 0;
    ltm.tm_min = 0;
    while (KTRUE) {
      if (code == "hour" || code == "hours") {
        break;
      }
      ltm.tm_hour = 0;
      if (code == "day" || code == "days" || code == "d") {
        break;
      }
      if (code == "week" || code == "weeks" || code == "w") {
        if (ltm.tm_wday == 0) {
          // Sunday
          ltm.tm_mday -= 6;
        } else {
          ltm.tm_mday -= ltm.tm_wday - 1;
        }
        break;
      }
      ltm.tm_mday = 1;
      if (code == "month" || code == "months" || code == "mon") {
        break;
      }
      if (code == "quarter") {
        if (ltm.tm_mon <= 2) {
          ltm.tm_mon = 0;
        } else if (ltm.tm_mon <= 5) {
          ltm.tm_mon = 3;
        } else if (ltm.tm_mon <= 8) {
          ltm.tm_mon = 6;
        } else {
          ltm.tm_mon = 9;
        }
        break;
      }
      ltm.tm_mon = 0;
      if (code == "year" || code == "years" || code == "y") {
        break;
      }
      if (code == "decade" || code == "decades") {
        ltm.tm_year -= ltm.tm_year % 12;
      } else if (code == "centuries" || code == "century") {
        ltm.tm_year -= ltm.tm_year % 100;
      } else if (code == "millennia" || code == "millennium" ||
                 code == "millenniums") {
        ltm.tm_year -= ltm.tm_year % 1000;
      } else {
        return 0;
      }
      break;
    }
    if (this->return_type_ == KWDBTypeFamily::TimestampTZFamily) {
      return (mktime(&ltm) + ltm.tm_gmtoff - time_zone * 3600) * 1000;
    }
    return (mktime(&ltm) + ltm.tm_gmtoff) * 1000;
  }
}

k_double64 FieldFuncDateTrunc::ValReal() { return ValInt(); }

String FieldFuncDateTrunc::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncDateTrunc::field_to_copy() {
  FieldFuncDateTrunc *field = new FieldFuncDateTrunc(*this);

  return field;
}

k_int64 FieldFuncExtract::ValInt() { return ValReal(); }

k_double64 FieldFuncExtract::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValReal(ptr);
  } else {
    k_int64 ti = args_[1]->ValInt();
    k_int64 tim = ti / 1000;
    k_double64 ms = static_cast<double>(ti % 1000);
    k_int64 res = 0;
    struct tm ltm;
    if (args_[1]->get_return_type() == KWDBTypeFamily::TimestampTZFamily) {
      tim += time_zone * 3600;
    }
    gmtime_r(&tim, &ltm);
    KString item = {args_[0]->ValStr().getptr(), args_[0]->ValStr().length_};
    if (item ==
        "millisecond") {  //  second field, in milliseconds, containing decimals
      return ms;
    } else if (item == "second") {  // containing decimals，in
                                    // secondes，containing decimals
      res = ltm.tm_sec;
      return static_cast<double>(res) + ms / 1000;
    } else if (item == "epoch") {  // seconds since 1970-01-01 00:00:00 UTC
      res = tim;
    } else if (item == "minute") {  // minute (0-59)
      res = ltm.tm_min;
    } else if (item == "hour") {  // hour (0-23)
      res = ltm.tm_hour;
    } else if (item == "day") {  // on the day of the month (1-31)
      res = ltm.tm_mday;
    } else if (item == "dow" ||
               item == "dayofweek") {  // what day of the week, Sunday (0) to
                                       // Saturday (6)
      res = ltm.tm_wday;
    } else if (item == "isodow") {  // Day of the week based on ISO 8601, Monday
                                    // (1) to Sunday (7)
      if (ltm.tm_wday == 0) {
        res = 7;
      } else {
        res = ltm.tm_wday;
      }
    } else if (item == "doy" ||
               item == "dayofyear") {  // on which day of the year, ranging from
                                       // 1 to 366
      res = ltm.tm_yday + 1;
    } else if (item == "julian") {
      if (ltm.tm_mon > 2) {
        ltm.tm_mon++;
        ltm.tm_year += 4800;
      } else {
        ltm.tm_mon += 13;
        ltm.tm_year += 4799;
      }

      k_int64 century = ltm.tm_year / 100;
      res = ltm.tm_year * 365 - 32167;
      res += ltm.tm_year / 4 - century + century / 4;
      res += 7834 * ltm.tm_mon / 256 + ltm.tm_yday;
      res += ltm.tm_hour * 3600 + ltm.tm_min * 60 + ltm.tm_sec;
      return static_cast<double>(res) + ms / 1000;
    } else if (item == "week") {  // ISO 8601
      k_int32 weekday = 0;
      if (ltm.tm_wday == 0) {
        weekday = 7;
      } else {
        weekday = ltm.tm_wday;
      }
      k_int64 wd = ltm.tm_yday + 4 - weekday;
      res = (wd / 7) + 1;
    } else if (item == "month") {  // month，1-12
      res = ltm.tm_mon + 1;
    } else if (item == "quarter") {  // quarter
      if (ltm.tm_mon <= 2) {
        res = 1;
      } else if (ltm.tm_mon <= 5) {
        res = 2;
      } else if (ltm.tm_mon <= 8) {
        res = 3;
      } else {
        res = 4;
      }
    } else if (item == "year") {  // yes
      res = ltm.tm_year + 1900;
    } else if (item == "isoyear") {  // year based on ISO 8601
      res = ltm.tm_year + 1900;
      // The last week of each year is the week of the last Thursday of the year
      k_int32 weekday = 0;
      if (ltm.tm_wday == 0) {
        weekday = 7;
      } else {
        weekday = ltm.tm_wday;
      }
      if (ltm.tm_yday < 7 && weekday > 4) {
        res -= 1;
      }
      if (ltm.tm_yday > 362 && weekday < 4) {
        res += 1;
      }
    } else if (item == "decade") {  // year/10
      res = (ltm.tm_year + 1900) / 10;
    } else if (item == "century") {  // century
      res = (ltm.tm_year + 1900) / 100 + 1;
    } else if (item == "millennium") {  // millennium
      res = (ltm.tm_year + 1900) / 1000 + 1;
    } else if (item == "timezone") {  // time zone offset from UTC, in seconds
      res = ltm.tm_gmtoff;
    } else if (item == "timezone_minute") {  // The minute portion of the time
                                             // zone offset
      res = ltm.tm_gmtoff / 60;
    } else if (item ==
               "timezone_hour") {  // The hourly portion of the time zone offset
      res = ltm.tm_gmtoff / 60 * 60;
    }

    return static_cast<double>(res);
  }
}

String FieldFuncExtract::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncExtract::field_to_copy() {
  FieldFuncExtract *field = new FieldFuncExtract(*this);

  return field;
}

k_int64 FieldFuncTimeOfDay::ValInt() {
  auto duration_since_epoch =
      std::chrono::system_clock::now().time_since_epoch();
  auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                            duration_since_epoch)
                            .count();
  return ms_since_epoch / 1000;
}

k_double64 FieldFuncTimeOfDay::ValReal() { return ValInt(); }

String FieldFuncTimeOfDay::ValStr() {
  time_t timestamp = std::time(nullptr);
  timestamp += time_zone * 3600;
  const int kArraySize = this->storage_len_;
  String s(kArraySize);
  tm *t = std::gmtime(&timestamp);
  t->tm_gmtoff = time_zone * 3600;
  std::strftime(s.ptr_, kArraySize, "%a %b %d %H:%M:%S.xxx %Y %z", t);
  std::string formattedTime(s.ptr_);
  auto pos = formattedTime.find("xxx");
  if (pos != std::string::npos) {
    formattedTime.replace(pos, 3, "%d");
  }
  auto duration_since_epoch =
      std::chrono::system_clock::now().time_since_epoch().count();
  std::snprintf(s.ptr_, kArraySize, formattedTime.c_str(),
                (duration_since_epoch / 1000000) % 1000);

  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncTimeOfDay::field_to_copy() {
  FieldFuncTimeOfDay *field = new FieldFuncTimeOfDay(*this);

  return field;
}

k_int64 FieldFuncExpStrftime::ValInt() { return 0; }

k_double64 FieldFuncExpStrftime::ValReal() { return ValInt(); }

Field *FieldFuncExpStrftime::field_to_copy() {
  FieldFuncExpStrftime *field = new FieldFuncExpStrftime(*this);

  return field;
}
String FieldFuncExpStrftime::ValStr() {
  k_int64 ti = args_[0]->ValInt();
  k_int64 tim = ti / 1000;
  struct tm ltm;
  localtime_r(&tim, &ltm);
  const int kArraySize = this->storage_len_;
  String s(kArraySize);
  try {
    std::strftime(
        s.ptr_, kArraySize,
        std::string(args_[1]->ValStr().getptr(), args_[1]->ValStr().length_).c_str(),
        &ltm);
  } catch (const std::exception &e) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_DATETIME_FORMAT,
                                  "Strftime transfer error");
  }
  s.length_ = strlen(s.ptr_);
  return s;
}

k_int64 FieldFuncRandom::ValInt() { return ValReal(); }

k_double64 FieldFuncRandom::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValReal(ptr);
  } else {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<k_double64> distribution(0, 1);

    return distribution(gen);
  }
}

String FieldFuncRandom::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncRandom::field_to_copy() {
  FieldFuncRandom *field = new FieldFuncRandom(*this);

  return field;
}

k_int64 FieldFuncWidthBucket::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    k_int64 expr = args_[0]->ValInt();
    k_int64 min = args_[1]->ValInt();
    k_int64 max = args_[2]->ValInt();
    k_int64 buckets_num = args_[3]->ValInt();

    if ((min < max && expr > max) || (min > max && expr < max)) {
      return buckets_num + 1;
    }

    if ((min < max && expr < min) || (min > max && expr > min)) {
      return 0;
    }

    k_double64 width = static_cast<k_double64>((max - min) / buckets_num);
    k_int64 difference = expr - min;
    return floor(difference / width) + 1;
  }
}

k_double64 FieldFuncWidthBucket::ValReal() { return ValInt(); }

String FieldFuncWidthBucket::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncWidthBucket::field_to_copy() {
  FieldFuncWidthBucket *field = new FieldFuncWidthBucket(*this);

  return field;
}

k_int64 FieldFuncAge::ValInt() {
  if (arg_count_ == 1) {
    auto duration_since_epoch =
        std::chrono::system_clock::now().time_since_epoch();
    auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                              duration_since_epoch)
                              .count();
    return ms_since_epoch / 1000 - args_[0]->ValInt();
  }
  return args_[0]->ValInt() - args_[1]->ValInt();
}

k_double64 FieldFuncAge::ValReal() { return ValInt(); }

Field *FieldFuncAge::field_to_copy() {
  FieldFuncAge *field = new FieldFuncAge(*this);

  return field;
}
String FieldFuncAge::ValStr() {
  std::string str;
  if (arg_count_ == 1) {
    auto duration_since_epoch =
        std::chrono::system_clock::now().time_since_epoch();
    auto ms_since_epoch = std::chrono::duration_cast<std::chrono::microseconds>(
                              duration_since_epoch)
                              .count();
    str = std::to_string(ms_since_epoch / 1000 - args_[0]->ValReal());
  }
  str = std::to_string(args_[0]->ValInt() - args_[1]->ValInt());
  String s(str.length());
  snprintf(s.ptr_, str.length() + 1, "%s", str.c_str());
  s.length_ = str.length();
  return s;
}

k_int64 FieldFuncCase::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    condition_met = KTRUE;
    for (size_t i = 0; i < arg_count_; i++) {
      k_int64 val = args_[i]->ValInt();
      if (args_[i]->is_condition_met()) {
        return val;
      }
    }
    condition_met = KFALSE;
    return 0;
  }
}

k_double64 FieldFuncCase::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  }
  condition_met = KTRUE;
  for (size_t i = 0; i < arg_count_; i++) {
    k_double64 val = args_[i]->ValReal();
    if (args_[i]->is_condition_met()) {
      return val;
    }
  }
  condition_met = KFALSE;
  return 0;
}

String FieldFuncCase::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    condition_met = KTRUE;
    for (size_t i = 0; i < arg_count_; i++) {
      String val = args_[i]->ValStr();
      if (args_[i]->is_condition_met()) {
        return val;
      }
    }
    condition_met = KFALSE;
    return String("");
  }
}

Field *FieldFuncCase::field_to_copy() {
  FieldFuncCase *field = new FieldFuncCase(*this);

  return field;
}

k_bool FieldFuncCase::is_nullable() {
  if (arg_count_ == 1) {
    k_int64 Val_left = args_[0]->ValInt();
    if (!args_[0]->is_condition_met()) {
      return KTRUE;
    }
    return args_[0]->is_nullable();
  }
  return args_[0]->is_nullable() || args_[1]->is_nullable();
}

k_bool FieldFuncCase::is_condition_met() { return condition_met; }

k_int64 FieldFuncThen::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    if (args_[0]->ValInt()) {
      condition_met = KTRUE;
      return args_[1]->ValInt();
    }
    condition_met = KFALSE;
    return 0;
  }
}

k_double64 FieldFuncThen::ValReal() {
  if (args_[0]->ValReal()) {
    condition_met = KTRUE;
    return args_[1]->ValReal();
  }
  condition_met = KFALSE;
  return 0;
}

String FieldFuncThen::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    String s1 = args_[0]->ValStr();
    if (s1.getptr() == "1") {
      condition_met = KTRUE;
      return args_[1]->ValStr();
    }
    condition_met = KFALSE;
    return String("");
  }
}

k_bool FieldFuncThen::is_condition_met() { return condition_met; }

Field *FieldFuncThen::field_to_copy() {
  FieldFuncThen *field = new FieldFuncThen(*this);

  return field;
}

k_bool FieldFuncThen::is_nullable() {
  if (arg_count_ > 1) {
    if (args_[0]->ValInt()) {
      return args_[1]->is_nullable();
    }
  }
  return KFALSE;
}

k_int64 FieldFuncElse::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    return args_[0]->ValInt();
  }
}

k_double64 FieldFuncElse::ValReal() { return args_[0]->ValReal(); }

String FieldFuncElse::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    return args_[0]->ValStr();
  }
}

k_bool FieldFuncElse::is_condition_met() { return KTRUE; }

Field *FieldFuncElse::field_to_copy() {
  FieldFuncElse *field = new FieldFuncElse(*this);

  return field;
}

k_int64 FieldFuncCrc32C::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = kwdb_crc32_castagnoli(valstr.data(), valstr.length());
  }

  return (k_int64)val;
}

k_double64 FieldFuncCrc32C::ValReal() { return ValInt(); }

String FieldFuncCrc32C::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncCrc32C::field_to_copy() {
  FieldFuncCrc32C *field = new FieldFuncCrc32C(*this);

  return field;
}
k_int64 FieldFuncCrc32I::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = kwdb_crc32_ieee(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncCrc32I::ValReal() { return ValInt(); }

String FieldFuncCrc32I::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncCrc32I::field_to_copy() {
  FieldFuncCrc32I *field = new FieldFuncCrc32I(*this);

  return field;
}
k_int64 FieldFuncFnv32::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1_hash32(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncFnv32::ValReal() { return ValInt(); }

String FieldFuncFnv32::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv32::field_to_copy() {
  FieldFuncFnv32 *field = new FieldFuncFnv32(*this);
  return field;
}
k_int64 FieldFuncFnv32a::ValInt() {
  char *ptr = get_ptr();
  k_uint32 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1a_hash32(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}
k_double64 FieldFuncFnv32a::ValReal() { return ValInt(); }

String FieldFuncFnv32a::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv32a::field_to_copy() {
  FieldFuncFnv32a *field = new FieldFuncFnv32a(*this);
  return field;
}
k_int64 FieldFuncFnv64::ValInt() {
  char *ptr = get_ptr();
  k_uint64 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1_hash64(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}

k_double64 FieldFuncFnv64::ValReal() { return ValInt(); }

String FieldFuncFnv64::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv64::field_to_copy() {
  FieldFuncFnv64 *field = new FieldFuncFnv64(*this);
  return field;
}

k_int64 FieldFuncFnv64a::ValInt() {
  char *ptr = get_ptr();
  k_uint64 val = 0;
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    string valstr = "";
    for (k_int32 i = 0; i < arg_count_; i++) {
      String s = args_[i]->ValStr();
      valstr += std::string(s.ptr_, s.length_);
    }
    val = fnv1a_hash64(valstr.data(), valstr.length());
  }
  return (k_int64)val;
}

k_double64 FieldFuncFnv64a::ValReal() { return ValInt(); }

String FieldFuncFnv64a::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncFnv64a::field_to_copy() {
  FieldFuncFnv64a *field = new FieldFuncFnv64a(*this);
  return field;
}
k_int64 FieldFuncCoalesce::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  }
  if (args_[0]->is_nullable()) {
    return args_[1]->ValInt();
  }
  return args_[0]->ValInt();
}

k_double64 FieldFuncCoalesce::ValReal() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValReal(ptr);
  }
  if (args_[0]->is_nullable()) {
    return args_[1]->ValReal();
  }
  return args_[0]->ValReal();
}

String FieldFuncCoalesce::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  }
  if (args_[0]->is_nullable()) {
    String s1 = args_[1]->ValStr();
    std::string destStr = std::string(s1.ptr_, s1.length_);
    if (this->storage_type_ == roachpb::DataType::BINARY) {
      String s0 = args_[0]->ValStr();
      std::string orgStr = std::string(s0.ptr_, s0.length_);
      k_int32 storageLen = this->storage_len_;
      k_int32 destStrLen = destStr.length();
      if (destStrLen < storageLen) {
        orgStr.replace(0, destStrLen, destStr);
        String s(orgStr.length());
        snprintf(s.getptr(), storage_len_, "%s", orgStr.c_str());
        s.length_ = orgStr.length();
        return s;
      }
      if (destStrLen > storageLen) {
        destStr = destStr.substr(0, storageLen);
      }
    }
    String s(destStr.length());
    snprintf(s.getptr(), storage_len_, "%s", destStr.c_str());
    s.length_ = destStr.length();
    return s;
  }
  return args_[0]->ValStr();
}

Field *FieldFuncCoalesce::field_to_copy() {
  FieldFuncCoalesce *field = new FieldFuncCoalesce(*this);

  return field;
}

k_bool FieldFuncCoalesce::is_nullable() { return false; }

}  // namespace kwdbts
