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

#pragma once

#include <queue>
#include <set>
#include "lru_cache.h"
#include "th_kwdb_dynamic_thread_pool.h"

/**
 * @brief A partitioned LRU cache manager class for managing and scheduling multiple partitioned LRU caches.
 */
class PartitionLRUCacheManager {
 public:
  /**
   * @brief Constructor to initialize the partition LRU cache manager.
   */
  PartitionLRUCacheManager();

  /**
   * @brief Destructor to clean up partition LRU cache manager resources.
   */
  ~PartitionLRUCacheManager();

  /**
   * @brief Add a partitioned LRU cache to the manager.
   * @param[in] partition_cache A pointer to the partitioned LRU cache to join for management.
   */
  void Push(PartitionLRUCache* partition_cache);

  /**
   * @brief Erase a partitioned LRU cache from the manager.
   * @param[in] partition_cache A pointer to the partition LRU cache to remove.
   */
  void Erase(PartitionLRUCache* partition_cache);

  /**
   * @brief Set the maximum capacity of each partition LRU cache in the manager.
   * @param[in] new_capacity New capacity value.
   */
  void SetCapacity(size_t new_capacity);

 private:
  // Read-write lock
  KRWLatch* rwlock_;
  // LRU cache collection
  set<PartitionLRUCache*> caches_;

  // Locks for capacity queues
  KLatch* capacity_queue_lock_;
  // A queue of capacity modification requests
  queue<size_t> capacity_queue_;

  // Modify the lock of the capacity thread
  KLatch* capacity_thread_lock_;
  // Id of the capacity thread
  kwdbts::KThreadID capacity_thread_id_;
  // Current capacity, default is 20
  std::atomic<size_t> current_capacity_ = 20;

  /**
   * @brief Asynchronously modifies capacity thread function.
   * @param[in] args Parameters of the thread function.
   */
  void routine(void* args);

  // The following three functions encapsulate read-write locks to simplify lock operations.
  inline void rdLock() { RW_LATCH_S_LOCK(rwlock_); }
  inline void wrLock() { RW_LATCH_X_LOCK(rwlock_); }
  inline void unLock() { RW_LATCH_UNLOCK(rwlock_); }
};

extern PartitionLRUCacheManager g_partition_caches_mgr;
