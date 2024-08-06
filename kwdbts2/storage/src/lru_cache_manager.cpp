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

#include "lru_cache_manager.h"

PartitionLRUCacheManager g_partition_caches_mgr;

PartitionLRUCacheManager::PartitionLRUCacheManager() {
  rwlock_ = new KRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_MANAGER_RWLOCK);
  capacity_thread_lock_ = new KLatch(LATCH_ID_PARTITION_LRU_CACHE_MANAGER_CAPACITY_THREAD_LOCK);
  capacity_queue_lock_ = new KLatch(LATCH_ID_PARTITION_LRU_CACHE_MANAGER_CAPACITY_QUEUE_LOCK);
}

PartitionLRUCacheManager::~PartitionLRUCacheManager() {
  if (capacity_thread_id_ > 0) {
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().JoinThread(capacity_thread_id_, 0);
  }
  caches_.clear();
  delete capacity_queue_lock_;
  capacity_queue_lock_ = nullptr;
  delete capacity_thread_lock_;
  capacity_thread_lock_ = nullptr;
  delete rwlock_;
  rwlock_ = nullptr;
}

void PartitionLRUCacheManager::Push(PartitionLRUCache* partition_cache) {
  wrLock();
  Defer defer {[&]() { unLock(); }};
  caches_.insert(partition_cache);
}

void PartitionLRUCacheManager::Erase(PartitionLRUCache* partition_cache) {
  wrLock();
  Defer defer {[&]() { unLock(); }};
  caches_.erase(partition_cache);
}

void PartitionLRUCacheManager::routine(void* args) {
  while (!kwdbts::KWDBDynamicThreadPool::GetThreadPool().IsCancel()) {
    if (kwdbts::KWDBDynamicThreadPool::GetThreadPool().IsCancel()) {
      capacity_thread_id_ = 0;
      break;
    }
    // Gets the capacity of the queue waiting for processing
    size_t capacity = 0;
    {
      MUTEX_LOCK(capacity_queue_lock_);
      Defer defer{[&]() {
        MUTEX_UNLOCK(capacity_queue_lock_);
      }};
      if (capacity_queue_.empty()) {
        capacity_thread_id_ = 0;
        break;
      }
      capacity = capacity_queue_.front();
      capacity_queue_.pop();
    }
    // Modify the capacity of all PartitionLRUCache
    {
      wrLock();
      Defer defer{[&]() { unLock(); }};
      for (auto cache : caches_) {
        if (cache) {
          cache->SetCapacity(capacity);
          cache->Clear(0);
        }
      }
    }
  }
  capacity_thread_id_ = 0;
}

void PartitionLRUCacheManager::SetCapacity(size_t new_capacity) {
  // Save new_capacity to the capacity queue
  {
    MUTEX_LOCK(capacity_queue_lock_);
    Defer defer{[&]() {
      MUTEX_UNLOCK(capacity_queue_lock_);
    }};
    capacity_queue_.push(new_capacity);
  }
  // Asynchronous thread processing capacity
  {
    MUTEX_LOCK(capacity_thread_lock_);
    Defer defer{[&]() {
      MUTEX_UNLOCK(capacity_thread_lock_);
    }};
    if (capacity_thread_id_ > 0) {
      return;
    }
    // Set the name and owner of the operation
    kwdbts::KWDBOperatorInfo kwdb_operator_info;
    kwdb_operator_info.SetOperatorName("PartitionLRUCacheManager");
    kwdb_operator_info.SetOperatorOwner("PartitionLRUCacheManager");
    time_t now;
    // Record the start time of the operation
    kwdb_operator_info.SetOperatorStartTime((uint64_t) time(&now));
    // Start asynchronous thread
    capacity_thread_id_ = kwdbts::KWDBDynamicThreadPool::GetThreadPool().ApplyThread(
      std::bind(&PartitionLRUCacheManager::routine, this, std::placeholders::_1), this,
      &kwdb_operator_info);
    if (capacity_thread_id_ < 1) {
      LOG_ERROR("PartitionLRUCacheManager capacity_thread create failed");
    }
  }
}
