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
#include <stdarg.h>
#include <cstdint>
#include <cstdio>
#include "status.h"

namespace kwdbts {
/*#define EnterFunc()
#define ReturnVoid() return;
#define Return(x) return x;
#define AssertNotNull(x) //assert( (x) != nullptr)
 */

void logError(const char *file, uint32_t line, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fprintf(stderr, "\n");
  fflush(stderr);
}

void logINFO(const char *file, uint32_t line, const char *fmt, ...) {
  {
    va_list args;
    va_start(args, fmt);
    vfprintf(stdout, fmt, args);
    va_end(args);
  }
  fprintf(stdout, "\n");
  fflush(stdout);
}

void printf_chars(const char *header, const char *msg, int size) {
  fprintf(stdout, "%s(%d):", header, size);
  for (int i = 0; i < size; i++) {
    fprintf(stdout, "%d,", msg[i]);
  }
  fprintf(stdout, "\n");
  fflush(stdout);
}

KBStatus assertERROR(const char* file, uint32_t line, KBStatus code, const char* fmt, ...) {
  if (code.isOK()) return code;
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fprintf(stderr, ":: errorcode=%s\n", code.message().c_str());
  return code;
}
pid_t getProcessPidByName(const char *proc_name)
{
  FILE *fp;
  char cmd[200] = {'\0'};
  pid_t pid = -1;
  sprintf(cmd, "pidof %s", proc_name);

  if((fp = popen(cmd, "r")) != NULL)
  {
    char buf[256];
    if(fgets(buf, 255, fp) != NULL)
    {
      pid = atoi(buf);
    }
  }

  printf("pid = %d \n", pid);

  pclose(fp);
  return pid;
}

void exec_cmd(const char *cmd, std::string& result) {
  char buf_ps[1024];
  char ps[1024]={0};
  FILE *ptr;
  strcpy(ps, cmd);
  if ((ptr = popen(ps, "r")) != nullptr) {
    while (fgets(buf_ps, 1024, ptr) != nullptr) {
      result = buf_ps;
      if (result.size() > 1024)
        break;
    }
    pclose(ptr);
    ptr = nullptr;
  } else {
    printf("popen %s error", ps);
  }
}

}