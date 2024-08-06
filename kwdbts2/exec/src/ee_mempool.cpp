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

#include "ee_mempool.h"

#include "lg_api.h"

namespace kwdbts {
EE_PoolInfoDataPtr EE_MemPoolInit(k_uint32 numOfBlock, k_uint32 blockSize) {
  if (numOfBlock <= 0 || blockSize <= 0) {
    LOG_ERROR("invalid parameter numOfBlockL %d, blockSize: %d\r\n", numOfBlock,
              blockSize);
    return nullptr;
  }

  EE_PoolInfoDataPtr pstPoolMsg = nullptr;
  pstPoolMsg = KNEW(EE_PoolInfoData);
  if (pstPoolMsg == nullptr) {
    LOG_ERROR("ee pool new failed .\r\n");
    return nullptr;
  }

  pstPoolMsg->data_ = nullptr;
  k_uint64 iSumSize = blockSize * numOfBlock;
  pstPoolMsg->data_ = static_cast<k_char *>(malloc(iSumSize));
  if (pstPoolMsg->data_ == nullptr) {
    LOG_ERROR("failed to malloc memory, malloc sum size: %ld\r\n", iSumSize);
    delete pstPoolMsg;
    return nullptr;
  }

  pstPoolMsg->iBlockSize = blockSize;
  pstPoolMsg->iNumOfSumBlock = numOfBlock;
  pstPoolMsg->iFreeList.clear();

  memset(pstPoolMsg->data_, 0, iSumSize);
  for (k_uint32 i = 0; i < numOfBlock; ++i) {
    pstPoolMsg->iFreeList.push_back(i);
  }

  pstPoolMsg->iNumOfFreeBlock = numOfBlock;

  pstPoolMsg->is_pool_init_ = true;

  return pstPoolMsg;
}

k_char *EE_MemPoolMalloc(kwdbts::EE_PoolInfoDataPtr pstPoolMsg) {
  k_char *data = nullptr;
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return nullptr;
  }
  std::unique_lock unique_lock(pstTemPtr->lock_);
  if (pstTemPtr->iNumOfFreeBlock > 0) {
    k_uint32 iFreePos = pstTemPtr->iFreeList.front();
    pstTemPtr->iFreeList.erase(pstTemPtr->iFreeList.begin());

    k_uint64 iOffset = pstTemPtr->iBlockSize * iFreePos;
    data = pstTemPtr->data_ + iOffset;
  } else {
    LOG_ERROR("out of memory\r\n");
  }

  return data;
}

kwdbts::KStatus EE_MemPoolFree(kwdbts::EE_PoolInfoDataPtr pstPoolMsg,
                               k_char *data) {
  k_uint32 iOffset = 0;
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return kwdbts::FAIL;
  }

  std::unique_lock unique_lock(pstTemPtr->lock_);
  if (data == nullptr) {
    return kwdbts::FAIL;
  }
  // caclulate pool offset
  iOffset = ((data - pstTemPtr->data_) % pstTemPtr->iBlockSize);
  if (iOffset != 0) {
    LOG_ERROR("invalid free address:%p\r\n", data);
    return kwdbts::FAIL;
  }

  iOffset = ((data - pstTemPtr->data_) / pstTemPtr->iBlockSize);
  if (iOffset < 0 || iOffset >= pstTemPtr->iNumOfSumBlock) {
    LOG_ERROR("iOffset: error, iOffset: %d invalid address:%p\r\n", iOffset,
              data);
    return kwdbts::FAIL;
  }

  memset(data, 0, pstTemPtr->iBlockSize);
  pstTemPtr->iFreeList.push_back(iOffset);
  return kwdbts::SUCCESS;
}

kwdbts::KStatus EE_MemPoolCleanUp(kwdbts::EE_PoolInfoDataPtr pstPoolMsg) {
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return kwdbts::FAIL;
  }
  {
    std::unique_lock unique_lock(pstTemPtr->lock_);
    if (pstPoolMsg->is_pool_init_ == false) {
      return kwdbts::FAIL;
    }
    if (pstTemPtr->data_ != nullptr) {
      free(pstTemPtr->data_);
      pstTemPtr->data_ = nullptr;
    }
    pstTemPtr->iFreeList.clear();
  }
  delete pstTemPtr;
  pstTemPtr = nullptr;
  return kwdbts::SUCCESS;
}
}  // namespace kwdbts
