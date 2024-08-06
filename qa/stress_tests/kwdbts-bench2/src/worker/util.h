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
/*
 * @author jiadx
 * @date 2022/10/15
 * @version 1.0
 */
#pragma once
#include "status.h"

namespace kwdbts {
#define log_ERROR(...) \
  logError(__FILE__, __LINE__, __VA_ARGS__)
#define log_INFO(...) \
  logINFO(__FILE__, __LINE__, __VA_ARGS__)

#define AssertStatus(x, msg, errcode) { \
  if ( x != kwdbts::KStatus::SUCCESS ) { \
      std::cerr << msg << errcode << std::endl; \
      return errcode; \
  } }

#define AssertError(errcode, ...)  \
  assertERROR(__FILE__, __LINE__, errcode, __VA_ARGS__)

void logError(const char* file, uint32_t line, const char* fmt, ...);

void logINFO(const char* file, uint32_t line, const char* fmt, ...);

void printf_chars(const char* header, const char* msg, int size);

KBStatus assertERROR(const char* file, uint32_t line, KBStatus code, const char* fmt, ...);

pid_t getProcessPidByName(const char *proc_name);

void exec_cmd(const char *cmd, std::string& result);
}