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

#include "lru_cache.h"
#include "lg_api.h"
#include "lru_cache_manager.h"

PartitionLRUCache::PartitionLRUCache() {
  lru_cache_rwlock_ = new PartitionLRUCacheRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_RWLOCK);
  g_partition_caches_mgr.Push(this);
}

PartitionLRUCache::PartitionLRUCache(size_t capacity) : capacity_(capacity) {
  lru_cache_rwlock_ = new PartitionLRUCacheRWLatch(RWLATCH_ID_PARTITION_LRU_CACHE_RWLOCK);
  g_partition_caches_mgr.Push(this);
}

PartitionLRUCache::~PartitionLRUCache() {
  g_partition_caches_mgr.Erase(this);
  delete lru_cache_rwlock_;
  lru_cache_rwlock_ = nullptr;
}

void PartitionLRUCache::Put(const timestamp64& key, MMapPartitionTable*& value, bool push_back) {
  wrLock();
  Defer defer{[&]() {
    unLock();
    // Elimination and cleaning
    Clear(0);
  }};
  try {
    auto it = cache_items_map_.find(key);
    // If the key already exists but the corresponding value is different,
    // the new value instance is released and updated to the old value.
    if (it != cache_items_map_.end()) {
      if ((intptr_t) (it->second->second) != (intptr_t) value) {
        LOG_WARN("Partition[%ld] already in cache.", key);
        ReleaseTable(value);
        value = it->second->second;
        value->incRefCount();
        return;
      }
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
    if (!push_back) {
      cache_items_list_.push_front(key_value_pair_t(key, value));
    } else {
      cache_items_list_.push_back(key_value_pair_t(key, value));
    }
    cache_items_map_.insert(std::make_pair(key, cache_items_list_.begin()));
    if (key > max_key_) {
      max_key_ = key;
    }
    value->incRefCount();
  } catch (...) {
    // Catch the exception to ensure that the lock can be released
  }
}

MMapPartitionTable* PartitionLRUCache::Get(const timestamp64& key) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  try {
    auto it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
      return nullptr;
    } else {
      // Moves the key-value pair to the head of the list, indicating that it was recently used.
      cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_, it->second);
      it->second->second->incRefCount();
      return it->second->second;
    }
  } catch (...) {
    return nullptr;
  }
}

std::vector<MMapPartitionTable*> PartitionLRUCache::GetAll(bool inc_ref) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  std::vector<MMapPartitionTable*> tables;
  try {
    auto it = cache_items_map_.begin();
    for (; it != cache_items_map_.end() ; it++) {
      tables.emplace_back(it->second->second);
      if (inc_ref) {
        it->second->second->incRefCount();
      }
    }
  } catch (...) {
  }
  return tables;
}

bool PartitionLRUCache::Exists(const timestamp64& key) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  return cache_items_map_.find(key) != cache_items_map_.end();
}

void PartitionLRUCache::Erase(const timestamp64& key, bool release_table) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto it = cache_items_map_.find(key);
  if (it != cache_items_map_.end()) {
    if (release_table) {
      ReleaseTable(it->second->second);
    }
    cache_items_list_.erase(it->second);
    cache_items_map_.erase(it);
    if (key == max_key_) {
      // If max_key is set to 0, both can be eliminated
      max_key_ = 0;
    }
  }
}

void PartitionLRUCache::Clear(int num) {
  {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    // If the number of elements is less than the maximum number of elements, no elimination is required
    if (cache_items_map_.size() <= capacity_) {
      return;
    }
  }
  wrLock();
  clear_nolock(true, num);
  unLock();
}

void PartitionLRUCache::Clear() {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  // If max_key is set to 0, both can be eliminated
  max_key_ = 0;
  clear_nolock(false, cache_items_map_.size());
}

size_t PartitionLRUCache::Size() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  return cache_items_map_.size();
}

size_t PartitionLRUCache::GetCapacity() {
  return capacity_;
}

void PartitionLRUCache::SetCapacity(size_t new_capacity) {
  capacity_ = new_capacity;
}

inline void PartitionLRUCache::clear_nolock(bool check_used, int num) {
  if (cache_items_list_.size() < 1) {
    return;
  }
  int count = 0;
  for (auto it = cache_items_list_.rbegin() ; it != cache_items_list_.rend() ;) {
    // If the maximum key is not 0, the key with the maximum value is not eliminated.
    if (max_key_ != 0 && it->first == max_key_) {
      ++it;
      continue;
    }
    // If the table is used, it is not eliminated.
    if (check_used && it->second->isUsed()) {
      ++it;
      continue;
    }

    ReleaseTable(it->second);
    cache_items_map_.erase(it->first);
    it = list<key_value_pair_t>::reverse_iterator(cache_items_list_.erase((++it).base()));
    count++;
    if (num == 0) {
      // If the number of eliminations is not limited, they are eliminated up to the limited size of the LRU cache.
      if (cache_items_map_.size() <= capacity_) {
        break;
      }
    } else if (count == num) {
      // Stops after a specified number of cache entries are eliminated.
      break;
    }
  }
}
