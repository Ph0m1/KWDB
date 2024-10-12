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
#include "ts_object_error.h"
#include "lg_api.h"
#include "lt_rw_latch.h"

typedef uint32_t RelHashIndexRowID;
typedef uint64_t RelHashCode;

struct RelRowIndice {
  uint16_t batch_no;
  uint16_t offset_in_batch;
};

// Data of index file for multiple model processing
struct RelHashIndexData {
  RelHashCode hash_val;
  RelHashIndexRowID next_row;
  RelRowIndice rel_row_indice;
};

struct RelIndexMetaData {
  size_t m_capacity;       // Capacity of mem_bash_
  size_t m_record_size;  // Hash index record size
  uint32_t m_row_count;  // Hash index row count
  size_t m_bucket_count;
  uint64_t m_lsn;
};

// HashBucket for multiple model processing
class RelHashBucket {
 private:
  RelHashIndexRowID* m_mem_bucket_;
  size_t m_bucket_count_;

 public:
  explicit RelHashBucket(size_t bucket_count = 8);
  ~RelHashBucket();

  size_t get_bucket_index(const size_t& key) const;
  void resize(size_t new_bucket_count);
  RelHashIndexRowID& bucketValue(size_t n);
};

typedef uint64_t (*RelHashFunc) (const char *data, int len);

// hash index for HashTagScan to build dynamic hash index for multiple model processing
class RelHashIndex {
 private:
  char* mem_hash_;
  RelHashFunc hash_func_;
  std::vector<RelHashBucket*> buckets_;
  size_t n_bkt_instances_;
  size_t m_element_count_;
  size_t m_bucket_count_;
  int m_key_len_;
  RelIndexMetaData meta_data{0};

  void resizeBucket(size_t new_bucket_count);

  inline RelHashIndexData* row(size_t n) const;
  inline char* keyvalue(size_t n) const;

  void loadRecord(size_t start, size_t end);
  std::pair<bool, size_t> is_need_rehash();
  void rehash(size_t new_size);

 public:
  explicit RelHashIndex(size_t bkt_instances = 1, size_t per_bkt_count = 8);
  ~RelHashIndex();

  int open(const std::string &path);

  size_t size() const;
  int reserve(size_t n);
  int put(const char *key, int len, const RelRowIndice& row_indice);
  int addOrUpdate(const char *key, int len, const RelRowIndice& row_indice, RelRowIndice& last_row_indice);
  int delete_data(const char *key, int len);

  int init(int key_len);

  inline bool compare(const char* key, size_t src_row) {
    return (memcmp(keyvalue(src_row), key, m_key_len_) == 0);
  }

  int get(const char *key, int len, RelRowIndice& last_row_indice);
  void printHashTable();

  uint64_t getLSN() const;
  void setLSN(uint64_t lsn);
  void setDrop();
  bool isDroped() const;

  // Public interfaces for testing
  void publicRehash(size_t new_bucket_count) {
    rehash(new_bucket_count);
  }

  RelHashIndexData* publicRow(size_t n) const {
    return row(n);
  }
};
