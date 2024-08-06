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

#include "cm_func.h"

namespace kwdbts {

#ifdef K_DEBUG
// caller to make sure ctx is a valid pointer
void ReturnIntl(kwdbContext_p ctx) {
  char k_stack_var;
  k_uint32 stack_size = reinterpret_cast<char*>(&(k_stack_var)) - reinterpret_cast<char*>(ctx);
  // computing stack size
  if (ctx->max_stack_size < stack_size) {
    ctx->max_stack_size = stack_size;
  }
  Assert(ctx->frame_level > 0);
  ctx->frame_level--;
  ctx->func_end_time[ctx->frame_level] = kwdbts::getCurrentTime();
  ctx->file_name[ctx->frame_level][0] = '\0';
  ctx->func_name[ctx->frame_level][0] = '\0';
  return;
}
#endif

// Get timestamp from KWDB routine thread.
KTimestamp getCurrentTime() {
  return 0;  // ctx->conn->timer;
}

// Get file name only from file path
const char* GetFileName(const char* path) {
  if (nullptr == path) {
    return nullptr;
  }
  for (const char* p = path + strlen(path) - 1; p > path; p--) {
    if (*p == '/') {
      p++;
      return p;
    }
  }
  return path;
}

void SetObjectColNotNull(char *bitmap, k_uint32 col_index) {
  const k_uint32 bit = col_index & static_cast<k_uint32>(0x07);
  const k_uint32 idx = col_index >> 3;
  bitmap[idx] &= ~(1 << bit);
}

void SetObjectColNull(char *bitmap, k_uint32 col_index) {
  const k_uint32 bit = col_index & static_cast<k_uint32>(0x07);
  const k_uint32 idx = col_index >> 3;
  bitmap[idx] |= (1 << bit);
}

}  //  namespace kwdbts
