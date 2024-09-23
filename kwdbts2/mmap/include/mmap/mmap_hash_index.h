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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>
#include "ts_object.h"
#include "data_type.h"
#include "mmap_file.h"
#include "ts_object_error.h"
#include "lg_api.h"
#include "lt_rw_latch.h"

typedef  uint32_t  HashIndexRowID;
typedef  uint32_t  TagTableRowID;
typedef  uint64_t  HashCode;

using TagHashIndexMutex = KLatch;

using TagHashIndexRWLock = KRWLatch;

using TagHashBucketRWLock = KRWLatch;

// data of index file
struct HashIndexData {
  HashCode         hash_val;
  TagTableRowID    bt_row;
  HashIndexRowID   next_row;
};


class  HashBucket {
 private:
  HashIndexRowID* m_mem_bucket_;
  TagHashBucketRWLock* m_bucket_rwlock_;
  // pthread_rwlock_t  rwlock_;
  size_t m_bucket_count_;

 public:
  explicit HashBucket(size_t  bucket_count = 8);
  virtual ~HashBucket();
  size_t get_bucket_index(const size_t& key);
  void resize(size_t new_bucket_count);
  HashIndexRowID& bucketValue(size_t n) {
    return *(m_mem_bucket_ + n);
  }
  inline int Rlock() {
    RW_LATCH_S_LOCK(m_bucket_rwlock_);
    return 0;
    //return pthread_rwlock_rdlock(&rwlock_);
  }
  inline int Wlock() {
    RW_LATCH_X_LOCK(m_bucket_rwlock_);
    return 0;
    // return pthread_rwlock_wrlock(&rwlock_);
  }
  inline int Unlock() {
    RW_LATCH_UNLOCK(m_bucket_rwlock_);
    return 0;
    // return pthread_rwlock_unlock(&rwlock_);
  }
};
struct IndexMetaData {
  size_t    m_file_size;  // Hash index file size
  size_t    m_record_size;  // Hash index record size
  uint32_t  m_row_count;  // Hash index row count
  size_t    m_bucket_count;
  uint64_t  m_lsn;
  bool      m_droped;
};

// please keep lsn and drop together and relative order
constexpr int lsnOffsetInHashIndex() {
  return offsetof(struct IndexMetaData, m_lsn);
}

typedef uint64_t (*HashFunc) (const char *data, int len);

class MMapHashIndex: public MMapFile {
  const off_t kHashMetaDataSize = 1024;

 protected:

  TagHashIndexMutex*  m_rehash_mutex_;

  TagHashIndexRWLock* m_file_rwlock_;
  // pthread_mutex_t rehash_mutex_;  // rehash
  // RWLock   m_file_rwlock_;
  HashIndexData *mem_hash_;
  HashFunc hash_func_;
  // MMapTagColumnTable*  m_bt_;

  std::vector<HashBucket* > buckets_;
  int    type_;
  size_t n_bkt_instances_;
  size_t m_element_count_;
  size_t m_bucket_count_;
  int    m_key_len_;

  void* addr(off_t offset) const
  { return reinterpret_cast<void*>((intptr_t) mem_ + offset); }

  void resizeBucket(size_t  new_bucket_count);

  inline HashIndexData* row(size_t n) const { 
    return reinterpret_cast<HashIndexData *>(offsetAddr(addr(kHashMetaDataSize), n * metaData().m_record_size));
  }

  inline char* keyvalue(size_t n) const {
    return reinterpret_cast<char *>((intptr_t) mem_ + kHashMetaDataSize + n * metaData().m_record_size + sizeof(HashIndexData));
  }
  std::pair<bool, size_t> is_need_rehash();

  void rehash(size_t new_size);

  int read_first(const char* key, int len);

  void mutexLock() { MUTEX_LOCK(m_rehash_mutex_); }

  void mutexUnlock() { MUTEX_UNLOCK(m_rehash_mutex_); }

  void bucketsRlock();

  void bucketsUnlock();

  void bucketsWlock();

  void dataRlock();

  void dataWlock();

  void dataUnlock();

  void loadRecord(size_t start, size_t end);

  void getHashValue(const char *s, int len, size_t& hashcode);

  inline HashIndexData* addrHash() const {
    return reinterpret_cast<HashIndexData *>((intptr_t) mem_ + kHashMetaDataSize);
  }

  inline bool compare(const char* key, size_t src_row) {
    return (memcmp(keyvalue(src_row), key, m_key_len_) == 0);
  }

 public:
  explicit MMapHashIndex(size_t bkt_instances = 1, size_t per_bkt_count = 8);
  virtual ~MMapHashIndex();

  IndexMetaData& metaData() const
  { return *(reinterpret_cast<IndexMetaData *>(mem_)); }

  virtual int open(const string &path, const std::string &db_path, const string &tbl_sub_path, int flag,
                   ErrorInfo &err_info);

  inline uint8_t type() const  { return type_; }

  int size() const;

  int reserve(size_t n);

  int keySize() const { return m_key_len_; }

  int put(const char *s, int len, TagTableRowID tag_table_rowid);

  int delete_data(const char *key, int len);

  void init(int key_len) {
    m_key_len_ = key_len;
    metaData().m_record_size = m_key_len_ + sizeof(HashIndexData);
  }

  /**
   * @brief	find the value stored in hash table for a given key.
   *
   * @param 	key			key to be found.
   * @param 	len			length of the key.
   * @return	the stored value in the hash table if key is found; 0 otherwise.
   */
  uint32_t get(const char *s, int len);

  int remove();

  void printHashTable();
  inline uint64_t getLSN() const {
    return metaData().m_lsn;
  }

  inline void setLSN(uint64_t lsn) {
    metaData().m_lsn = lsn;
    return;
  }

  inline void setDrop() {
    metaData().m_droped = true;
    return;
  }

  inline bool isDroped() const {
    return metaData().m_droped;
  }
};
