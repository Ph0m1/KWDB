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

#include <unordered_map>
#include <vector>
#include <list>
#include <utility>
#include "mmap/MMapPartitionTable.h"

inline int ReleaseTable(MMapPartitionTable* rls_obj) {
  if (rls_obj == nullptr) {
    return 0;
  }
  int ref_cnt;
  MUTEX_LOCK(rls_obj->m_ref_cnt_mtx_);
  ref_cnt = --(rls_obj->refCount());

  LogObject(OBJ_RLS, rls_obj);
  MUTEX_UNLOCK(rls_obj->m_ref_cnt_mtx_);
  if (ref_cnt <= 1) {
    // notify the waiting drop thread if any
    KW_COND_SIGNAL(rls_obj->m_ref_cnt_cv_);
  }

  if (ref_cnt > 0) {
    return ref_cnt;
  }

  delete rls_obj;
  return 0;
}

/**
 * @brief Partition LRU cache class for caching and managing MMapPartitionTable objects.
 */
class PartitionLRUCache {
 public:
  // Define key/value pair type.
  // key=timestamp64 value, value=pointer to MMapPartitionTable.
  typedef typename std::pair<timestamp64, MMapPartitionTable*> key_value_pair_t;
  // Define the list iterator type
  typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

  /**
   * @brief Default constructor that initializes the cache.
   */
  PartitionLRUCache();

  /**
   * @brief Constructor to initialize the cache.
   * @param[in] capacity Maximum capacity of the cache.
   */
  explicit PartitionLRUCache(size_t capacity);

  /**
   * @brief Destructor to release resources
   */
  ~PartitionLRUCache();

  /**
   * @brief Insert a key-value pair into the cache.
   * @param[in] key timestamp64 value.
   * @param[in] value pointer to MMapPartitionTable.
   * @param[in] push_back Whether to insert key-value pairs at the end of the list.
   */
  void Put(const timestamp64& key, MMapPartitionTable*& value, bool push_back = false);

  /**
   * @brief Retrieve the key-value pairs in the cache.
   * @param[in] key timestamp64 value.
   * @return pointer to MMapPartitionTable.
   */
  MMapPartitionTable* Get(const timestamp64& key);

  /**
   * @brief Gets all key-value pairs in the cache.
   * @param[in] inc_ref Whether to increment the reference count of MMapPartitionTable.
   * @return List of Pointers to MMapPartitionTable
   */
  std::vector<MMapPartitionTable*> GetAll(bool inc_ref = false);

  /**
   * @brief Checks the cache for the presence of the specified key.
   * @param[in] key timestamp64 value.
   * @return Whether the key exists.
   */
  bool Exists(const timestamp64& key);

  /**
   * @brief Erase key-value pairs from the cache.
   * @param[in] key timestamp64 value.
   * @param[in] release_table Whether to release the MMapPartitionTable object.
   */
  void Erase(const timestamp64& key, bool release_table = true);

  /**
   * @brief Clear key-value pairs from the cache.
   * @param[in] num The number of key-value pairs to clear.
   */
  void Clear(int num);

  /**
   * @brief Clear all key-value pairs in the cache.
   */
  void Clear();

  /**
   * @brief Gets the number of key-value pairs in the cache.
   * @return The number of key-value pairs in the cache.
   */
  size_t Size();

  /**
   * @brief Gets the maximum capacity of the cache.
   * @return The maximum capacity of the cache.
   */
  size_t GetCapacity();

  /**
   * @brief Set the maximum cache capacity.
   * @param[in] size New maximum capacity.
   */
  void SetCapacity(size_t size);

 private:
  // A list of cached items, maintained in the order they are accessed.
  std::list<key_value_pair_t> cache_items_list_;
  // Cache item mappings for quickly locating items in a list.
  std::unordered_map<timestamp64, list_iterator_t> cache_items_map_;
  // The items corresponding to max_key_ will not be eliminated.
  // If the value is 0, it means all items can be eliminated
  timestamp64 max_key_{0};
  // Maximum cache capacity. 0 indicates no limit.
  size_t capacity_;

  // Read/write lock, used for concurrency control.
  using PartitionLRUCacheRWLatch = KRWLatch;
  PartitionLRUCacheRWLatch* lru_cache_rwlock_;

  // The following three functions encapsulate read-write locks to simplify lock operations.
  inline void rdLock() { RW_LATCH_S_LOCK(lru_cache_rwlock_); }
  inline void wrLock() { RW_LATCH_X_LOCK(lru_cache_rwlock_); }
  inline void unLock() { RW_LATCH_UNLOCK(lru_cache_rwlock_); }

  /**
   * @brief Deprecate cache entries according to LRU policy(lock-free).
   * @param check_used Whether to check whether the MMapPartition is in use. If used, it will not be eliminated.
   * @param num Number of eliminations, 0 indicates no limit.
   */
  inline void clear_nolock(bool check_used, int num);
};
