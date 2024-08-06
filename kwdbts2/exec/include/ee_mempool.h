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

#pragma once

#include <mutex>
#include <vector>

#include "kwdb_type.h"

namespace kwdbts {

#define newpool_max_size (128)       // default number of memory pool
#define row_buffer_size (32 * 1024)  // default buffer size = 32k

typedef struct {
  k_uint32 iNumOfFreeBlock;         // amount of free block in the pool
  k_uint32 iNumOfSumBlock;          // amount of total block in the pool
  k_uint32 iBlockSize;              // block size
  std::vector<k_uint32> iFreeList;  // free block list
  k_char *data_;                    // pointer to pool data
  bool is_pool_init_{false};
  mutable std::mutex lock_;         // mutex to control buffer allocate and release
} EE_PoolInfoData;
typedef EE_PoolInfoData *EE_PoolInfoDataPtr;

kwdbts::EE_PoolInfoDataPtr EE_MemPoolInit(k_uint32 numOfBlock,
                                          k_uint32 blockSize);
k_char *EE_MemPoolMalloc(kwdbts::EE_PoolInfoDataPtr pstPoolMsg);
kwdbts::KStatus EE_MemPoolFree(kwdbts::EE_PoolInfoDataPtr pstPoolMsg,
                               k_char *data);
kwdbts::KStatus EE_MemPoolCleanUp(kwdbts::EE_PoolInfoDataPtr pstPoolMsg);

};  // namespace kwdbts
