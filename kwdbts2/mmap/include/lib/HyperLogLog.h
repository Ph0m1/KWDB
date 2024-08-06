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


#ifndef INCLUDE_LIB_HYPERLOGLOG_H_
#define INCLUDE_LIB_HYPERLOGLOG_H_


//////////////////////////////////
// HpyerLogLog for distinct count
// accuracy = 1.04/sqrt(2^p)
// 32-bit hash
//#define HLL32
#if defined(HLL32)
#define HLL_P               10
#define HLL_BITS            5
#define HLL_REG_SHIFT       (32-HLL_P)
#else
#define HLL_P               14
#define HLL_BITS            6
#define HLL_REG_SHIFT       (64-HLL_P)
#endif
#define HLL_REG_COUNT       (1<<HLL_P)
#define HLL_SENTRY_BIT      (1<<(HLL_P-1))
#define HLL_REGISTER_MASK   ((1<<HLL_BITS)-1)

int hllSize(int p);

void initHyperLogLog();

size_t HLLCount(void *p);

//////////////////////////////////////
// 64bit HpyerLogLog using 64-bit hash
#define HLL64_P               14
#define HLL64_BITS            6
#define HLL64_REG_SHIFT       (64-HLL64_P)
#define HLL64_SENTRY_BIT      (1<<(HLL64_P-1))
#define HLL64_REGISTER_MASK   ((1<<HLL64_BITS)-1)

#endif /* INCLUDE_LIB_HYPERLOGLOG_H_ */
