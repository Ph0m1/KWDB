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


#ifndef DEFAULT_H_
#define DEFAULT_H_

#include <string>

#define DIMENSION_SEPARATOR '.'
#define GROUP_SEPARATOR     ':'

std::string defaultNameServiceURL();

//#if defined(__aarch64__)
//#define IOT_MODE
//#endif

//#if !defined(NDEBUG) || defined(IOT)
//#define IOT_MODE
//#endif

//#if !defined(NDEBUG)
//#define IOT_MODE
//#endif

//#define KAIWU

#define MIX_NULL
// #define V8_ONLY


#endif /* DEFAULT_H_ */
