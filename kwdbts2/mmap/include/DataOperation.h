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

#ifndef DATAOPERATION_H_
#define DATAOPERATION_H_

#include <string>
#include <vector>
#include "BigObjectError.h"
#include "BigObjectConst.h"


using namespace std;

enum DATA_OPERATION {
  SUM,                    // result is either INT64 or DOUBLE
  COUNT,
  MAX,
  MIN,
  AVG,
  CUM_SUM,
  CUM_DIFF,
  NORM,
  VARIANCE,
  VAR_POP,
  STD,
  STD_POP,
//    COMB_VARIANCE,
//    COMB_VAR_SAMP,
//    COMB_STD,
//    COMB_STD_SAMP,
  CORR,
  CORR_SAMP,
  DIST,
  NORM_DIST,
  APPROX_DIST,              // approximate distinct count
  APPROX_NORM_DIST,         // approximate normalized distinct count
  HLL_MERGE,                // hyperloglog merge
  GROUP_CONCAT,
  GROUP_CONCAT_DIST,
  MEDIAN,                   // quantile(0.5)
  QUANTILE,                 // quantile
  APPROX_MEDIAN,            // approximate quantile(0.5)
  FIRST_OP,
  LAST_OP,
  LAST_TS_OP,
  LAST_ROW_OP,
  LAST_ROW_TS_OP,
  APPROX_QUANTILE,          // approximate quantile
  ZERO,
  POST_DIV,                 // division in post action
  DEF_FUNC = 100,
//  INTEGER_CONST = 20000,
//  REAL_CONST = 20001,
//  STRING_CONST = 20002,
  USER_FUNC = 30000,
//  QM_OP = 40000,
  CASE_OP     = 80000,
  WHEN_OP     = 80001,
  CAST_OP     = 80002,
  BETWEEN_OP  = 80003,
  INTERVAL    = 80100,
  NULL_OP = 89000,
  FUNC = 200000,
  NOT = 300000,
  AND = 300001,
  OR = 300002,
  NEG = 300003,
  ADD = 300004,
  SUB = 300005,
  MUL = 300006,
  DIV = 300007,
  MOD = 300008,
  BIT_AND = 300100,
  BIT_OR = 300101,
  BIT_XOR = 300102,
  BIT_SHIFT_L = 300103,
  BIT_SHIFT_R = 300104,
  EQ = 400000,
  NE,
  LT,
  LE,
  GT,
  GE,
  IS,
  ISN,
  INOP = 500009,
  INNER_JOIN_OP = 500010,
  GROUP_CONCAT_MERGE,
  NOOP = 800000,        // attribute
  DYNAMIC_OP,           // Question mark, used in prepared statement.
  ASTERISK = 1000000,
  COMMA,
  EXTRACT,
};

#define isAggregateOP(x) ((x < APPROX_QUANTILE))

#define isArithmeticOP(x) ((x >= NOT) && (x <= INNER_JOIN_OP))
//#define is_CombinedVarianceOP(x) ((x >= COMB_VARIANCE && x <= COMB_STD_SAMP))

#define isBinaryOP(x) ((x >= AND) && (x <= GE))


using namespace bigobject;

// flags used in class SelectField
//
// SF_DUP_IN_LIST: e.g. AVG(x), STD(x). AVG(x) is required to get STD(x).
//   select field AVG(x) will be marked with SF_DUP_IN_LIST.
//   for cluster query, one-pass STD(x) will compute AVG(x), thus there is no
//   need to compute AVG(x) twice.
//#define SF_CLIENT           0x00000001  // required select field in cluster
#define SF_NATIVE_ALIAS     0x00000002  // user specified alias
#define SF_IN_GROUPBY       0x00000010  // select field appeared in group by
#define SF_IN_RESULT        0x00000100  // select field in result table.
#define SF_DUP_IN_LIST      0x00001000  // select field is duplicate in list
#define SF_DUP_AVG_IN_LIST  0x00002000  // avg is duplicate in list
#define SF_IN_HAVING        0x00010000
//#define is_client_sf(x)     ((x & SF_CLIENT))
#define is_native_alias(x)  ((x & SF_NATIVE_ALIAS))
#define is_sf_in_result(x)  ((x & SF_IN_RESULT))
#define is_sf_duplicate(x)  ((x & SF_DUP_IN_LIST))


#endif /* DATAOPERATION_H_ */
