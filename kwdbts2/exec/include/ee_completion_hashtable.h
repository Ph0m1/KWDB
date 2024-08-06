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

#include <functional>
#include "ee_global.h"
#include "lg_api.h"
#include "kwdb_type.h"

namespace kwdbts {

#define H_ALIGN_SIZE 2

// value
template <typename T>
struct Entry {
  T node_{nullptr};                 // value
  struct Entry<T> *next_{nullptr};    // Entry list
};

// bucket
template <typename T>
struct Bucket {
  k_uint64 hash_value_{0};          // hash value
  k_uint32 count_{0};               // The number of entries in the bucket
  union {
    T node_;           // a linked list of values in the current bucket
    Entry<T> *enrty_;  // a linked list of values in the current bucket
  };
  struct Bucket<T> *next_{nullptr};    // bucket list
};

template <typename T>
struct HashTable {
  k_uint64 size_{0};                  // buckets_ num
  k_uint64 total_{0};                 // total number of data
  bool initialized_{false};           // init
  Bucket<T> **buckets_{nullptr};      // hash buckets
};

/**
 * @brief 
 *                  initialize hash table
 * @param ht        HashTable object
 * @param nSize     buckets count
 * 
 * @return KStatus 
 *    KStatus::SUCCESS    Success
 *    KStatus::FAIL       Error
 */
template <class T>
extern KStatus completion_hash_init(HashTable<T> *ht, k_uint32 nSize) {
  k_uint32 sz = (nSize + H_ALIGN_SIZE - 1) & ~(H_ALIGN_SIZE - 1);
  ht->buckets_ = static_cast<Bucket<T> **>(malloc(sz * sizeof(Bucket<T> *)));
  if (nullptr == ht->buckets_) {
    ht->initialized_ = false;
    return KStatus::FAIL;
  }
  memset(ht->buckets_, 0, sz * sizeof(Bucket<T> *));
  ht->size_ = sz;
  ht->initialized_ = true;

  return KStatus::SUCCESS;
}

// The following function uses the union in a bucket { Entry *enrty_ } interface

/**
 * @brief 
 *                      find key bucket
 * @param ht            HashTable object
 * @param key           key
 * @param key_length    key length
 * 
 * @return Bucket* 
 *      Bucket pointer if success
 *      nullptr is Error
 */
template <class T>
extern Bucket<T> *completion_hash_find_bucket(HashTable<T> *ht, k_uint64 hash_value) {
  k_uint64 index = hash_value % ht->size_;

  Bucket<T> *p = ht->buckets_[index];
  while (p) {
    if (p->hash_value_ == hash_value) {
      return p;
    }
    p = p->next_;
  }
  return nullptr;
}

/**
 * @brief 
 *                    add key-value in hash table
 * @param ht          HashTable object
 * @param key         key
 * @param key_length  key length
 * @param node        value
 * 
 * @return KStatus 
 *    KStatus::SUCCESS    Success
 *    KStatus::FAIL       Error
 */
template <class T>
extern KStatus completion_hash_add(HashTable<T> *ht, k_uint64 hash_value, T node) {
  k_uint64 index = hash_value % ht->size_;
  Bucket<T> *p = ht->buckets_[index];
  while (p) {
    if (p->hash_value_ == hash_value) {
      // The keys are equal
      Entry<T> *n = static_cast<Entry<T> *>(malloc(sizeof(Entry<T>)));   // malloc data object
      if (nullptr == n) {
        LOG_ERROR("malloc failed");
        return KStatus::FAIL;
      }
      // fill the entry
      n->node_ = node;
      n->next_ = p->enrty_;
      // modify bucket
      p->enrty_ = n;
      ++p->count_;

      ++ht->total_;
      return KStatus::SUCCESS;
    }
    p = p->next_;
  }

  // first insert
  p = static_cast<Bucket<T> *>(malloc(sizeof(Bucket<T>)));
  if (nullptr == p) {
    return KStatus::FAIL;
  }

  p->enrty_ = static_cast<Entry<T> *>(malloc(sizeof(Entry<T>)));
  if (nullptr == p->enrty_) {
    return SUCCESS;
  }

  // update bucket
  p->hash_value_ = hash_value;
  p->next_ = ht->buckets_[index];
  ht->buckets_[index] = p;
  // update entry
  p->enrty_->node_ = node;
  p->enrty_->next_ = nullptr;
  p->count_ = 1;
  ++ht->total_;

  return SUCCESS;
}

/**
 * @brief 
 *                    key exist
 * @param ht          HashTable object
 * @param key         key
 * @param key_length  key_length
 * 
 * @return true       exist
 * @return false      not exist
 */
template <class T>
extern bool completion_hash_exists(HashTable<T> *ht, k_uint64 hash_value) {
  // calculate the idex which in buckets
  k_uint64 index = hash_value % ht->size_;

  Bucket<T> *p = ht->buckets_[index];
  while (p) {
    if (p->hash_value_ == hash_value) {
      return 1;
    }
    p = p->next_;
  }

  return 0;
}

/**
 * @brief 
 *              Free hashtable
 * @param ht    HashTable object
 */
template <class T>
extern void completion_hash_free(HashTable<T> *ht) {
  for (k_uint32 i = 0; i < ht->size_; ++i) {
    completion_hash_free_bucket(ht->buckets_[i]);
  }

  SafeFreePointer(ht->buckets_);
}

template <class T>
void completion_hash_free_bucket(Bucket<T> *bucket) {
  while (nullptr != bucket) {
    Bucket<T> *tmp = bucket;
    completion_hash_free_entry(bucket->enrty_);
    bucket = bucket->next_;
    SafeFreePointer(tmp);
  }
}

template <class T>
void completion_hash_free_entry(Entry<T> *entry) {
  while (nullptr != entry) {
    Entry<T> *tmp = entry;
    entry = entry->next_;
    SafeFreePointer(tmp);
  }
}

// The following macro indicates that the union in the bucket is used  { void
// *node_ } interface
#define HASH_INSERT(TYPE, HASH_TABLE, HASH_VALUE, DATA_STRUCT, NEXT, KSTATUS_RETURN)        \
  do {                                                                                      \
    k_uint64 index = HASH_VALUE %  HASH_TABLE->size_;                                       \
    auto *bucket3333 = HASH_TABLE->buckets_[index];                                         \
                                                                                            \
    while (bucket3333) {                                                                    \
      if (bucket3333->hash_value_ == HASH_VALUE) {                                          \
        if (0 == NEXT) {                                                                    \
          KSTATUS_RETURN = KStatus::FAIL;                                                   \
        } else {                                                                            \
          DATA_STRUCT->next_ = bucket3333->node_;                                           \
          bucket3333->node_ = DATA_STRUCT;                                                  \
          ++bucket3333->count_;                                                             \
          ++HASH_TABLE->total_;                                                             \
          KSTATUS_RETURN = KStatus::SUCCESS;                                                \
        }                                                                                   \
        break;                                                                              \
      }                                                                                     \
      bucket3333 = bucket3333->next_;                                                       \
    }                                                                                       \
                                                                                            \
    if (nullptr == bucket3333) {                                                            \
      bucket3333 = static_cast<Bucket<TYPE> *>(malloc(sizeof(Bucket<TYPE>)));               \
      if (nullptr != bucket3333) {                                                          \
        bucket3333->hash_value_ = HASH_VALUE;                                               \
        bucket3333->node_ = DATA_STRUCT;                                                    \
        bucket3333->next_ = HASH_TABLE->buckets_[index];                                    \
        HASH_TABLE->buckets_[index] = bucket3333;                                           \
        ++HASH_TABLE->total_;                                                               \
        KSTATUS_RETURN = KStatus::SUCCESS;                                                  \
      }                                                                                     \
    }                                                                                       \
  } while (0)

#define HASH_FIND_KEY_EXISTS(TYPE, HASH_TABLE, HASH_VALUE, EXIST)                           \
  do {                                                                                      \
    k_uint64 index = HASH_VALUE %  HASH_TABLE->size_;                                       \
    Bucket<TYPE> *bucket3333 = HASH_TABLE->buckets_[index];                                 \
    while (bucket3333) {                                                                    \
      if (bucket3333->hash_value_ == HASH_VALUE) {                                          \
        EXIST = true;                                                                       \
        break;                                                                              \
      }                                                                                     \
      bucket3333 = bucket3333->next_;                                                       \
    }                                                                                       \
  } while (0)

#define HASH_FIND_BUCKET(TYPE, HASH_TABLE, HASH_VALUE, BUCKET)                              \
  do {                                                                                      \
    k_uint64 index = HASH_VALUE %  HASH_TABLE->size_;                                       \
    Bucket<TYPE> * bucket3333 = HASH_TABLE->buckets_[index];                                \
    while (bucket3333) {                                                                    \
      if (bucket3333->hash_value_ == HASH_VALUE) {                                          \
        BUCKET = bucket3333;                                                                \
        break;                                                                              \
      }                                                                                     \
      bucket3333 = bucket3333->next_;                                                       \
    }                                                                                       \
  } while (0)

#define HASH_TABLE_FREE(HASH_TABLE, TYPE, DELETE_TYPE)                                      \
  do {                                                                                      \
    for (k_uint32 i = 0; i < HASH_TABLE->size_; ++i) {                                      \
      Bucket<TYPE> *buck = HASH_TABLE->buckets_[i];                                         \
      while (nullptr != buck) {                                                             \
        Bucket<TYPE> *tmp = buck;                                                           \
        if (true == DELETE_TYPE) {                                                          \
          SafeDeletePointer(buck->node_);                                                   \
        }                                                                                   \
        buck = buck->next_;                                                                 \
        SafeFreePointer(tmp);                                                               \
      }                                                                                     \
    }                                                                                       \
                                                                                            \
    SafeFreePointer(HASH_TABLE->buckets_);                                                  \
  } while (0)

}  // namespace kwdbts
