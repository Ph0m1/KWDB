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

#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <thread>
#include <stdexcept>
#include "utils/big_table_utils.h"
#include "ts_object_error.h"
#include "lib/t1ha.h"
#include "ee_rel_hash_index.h"

RelHashBucket::RelHashBucket(size_t bucket_count) {
  m_bucket_count_ = bucket_count < 8 ? 8 : bucket_count;
  m_mem_bucket_ = reinterpret_cast<RelHashIndexRowID*>(new char[m_bucket_count_ * sizeof(RelHashIndexRowID)]);
  memset(m_mem_bucket_, 0x00, sizeof(RelHashIndexRowID) * m_bucket_count_);
}

RelHashBucket::~RelHashBucket() {
  delete[] m_mem_bucket_;
}

size_t RelHashBucket::get_bucket_index(const size_t& key) const {
  return key % m_bucket_count_;
}

void RelHashBucket::resize(size_t new_bucket_count) {
  if (new_bucket_count <= m_bucket_count_) {
      return;
  }
  RelHashIndexRowID* tmp_bucket =
          reinterpret_cast<RelHashIndexRowID*>(new char[new_bucket_count * sizeof(RelHashIndexRowID)]);
  memset(tmp_bucket, 0x00, sizeof(RelHashIndexRowID) * new_bucket_count);
  m_bucket_count_ = new_bucket_count;
  delete[] m_mem_bucket_;
  m_mem_bucket_ = tmp_bucket;
}

RelHashIndex::RelHashIndex(size_t bkt_instances, size_t per_bkt_count) {
  mem_hash_ = nullptr;
  hash_func_ = t1ha1_le;
  n_bkt_instances_ = bkt_instances < 1 ? 1 : bkt_instances;
  m_bucket_count_ = n_bkt_instances_ * (per_bkt_count < 8 ? 8 : per_bkt_count);
  m_element_count_ = 0;

  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    buckets_.push_back(new RelHashBucket(per_bkt_count));
  }
}

RelHashIndex::~RelHashIndex() {
  for (size_t i = 0; i < n_bkt_instances_; ++i) {
    delete buckets_[i];
    buckets_[i] = nullptr;
  }
  if (mem_hash_) {
    delete[] mem_hash_;
    mem_hash_ = nullptr;
  }
}

void RelHashIndex::resizeBucket(size_t new_bucket_count) {
  if (new_bucket_count <= m_bucket_count_) {
    return;
  }
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
  }
}

int RelHashIndex::init(int key_len) {
  m_key_len_ = key_len;
  int record_size = m_key_len_ + sizeof(RelHashIndexData);

  size_t initial_size = m_bucket_count_ * record_size;
  mem_hash_ = new char[initial_size]();

  meta_data.m_bucket_count = m_bucket_count_;
  meta_data.m_record_size = record_size;
  meta_data.m_capacity = initial_size;

  return 0;
}

void RelHashIndex::rehash(size_t new_size) {
  size_t new_bucket_count = new_size;
  if (new_bucket_count <= m_bucket_count_) {
    // do nothing
    return;
  }
  auto start = std::chrono::high_resolution_clock::now();
  for (size_t idx = 0; idx < n_bkt_instances_; ++idx) {
    buckets_[idx]->resize(new_bucket_count / n_bkt_instances_);
  }
  reserve(new_bucket_count);
  for (uint32_t rownum = 1; rownum <= meta_data.m_row_count; ++rownum) {
    int bkt_ins_idx = (row(rownum)->hash_val >> 56) & (n_bkt_instances_ - 1);
    int bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(row(rownum)->hash_val);
    row(rownum)->next_row = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = rownum;
  }
  m_bucket_count_ = new_bucket_count;
  meta_data.m_bucket_count = m_bucket_count_;
  auto end = std::chrono::high_resolution_clock::now();
  auto ins_dur = std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
}

size_t RelHashIndex::size() const {
  if (mem_hash_) {
    return meta_data.m_capacity;
  }
  return 0;
}

int RelHashIndex::reserve(size_t n) {
  size_t new_capacity = n * meta_data.m_record_size;
  if (meta_data.m_capacity < new_capacity) {
    // Simulate memory allocation increase
    char* new_mem_hash = new char[new_capacity]();
    memcpy(new_mem_hash, mem_hash_, meta_data.m_capacity);
    delete[] mem_hash_;
    mem_hash_ = new_mem_hash;
    meta_data.m_capacity = new_capacity;
  }
  return 0;
}

RelHashIndexRowID& RelHashBucket::bucketValue(size_t n) {
  return m_mem_bucket_[n];
}

int RelHashIndex::put(const char *key, int len, const RelRowIndice& row_indice) {
  RelHashCode hash_val = (*hash_func_)(key, len);
  std::pair<bool, size_t> do_rehash = is_need_rehash();
  if (do_rehash.first) {
    rehash(do_rehash.second);
  }

  size_t rownum = meta_data.m_row_count + 1;
  meta_data.m_row_count++;
  ++m_element_count_;

  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  size_t bucket_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);

  RelHashIndexData* current_data = row(rownum);
  current_data->hash_val = hash_val;
  current_data->rel_row_indice = row_indice;
  memcpy(keyvalue(rownum), key, len);

  current_data->next_row = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);

  buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
  return 0;
}

int RelHashIndex::addOrUpdate(const char *key, int len, const RelRowIndice& row_indice, RelRowIndice& last_row_indice) {
  RelHashCode hash_val = (*hash_func_)(key, len);
  std::pair<bool, size_t> do_rehash = is_need_rehash();
  if (do_rehash.first) {
    rehash(do_rehash.second);
  }

  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  size_t bucket_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
  if (rownum == 0) {
    rownum = meta_data.m_row_count + 1;
    meta_data.m_row_count++;
    ++m_element_count_;
    RelHashIndexData* current_data = row(rownum);
    // This key doesn't exist, so need to add it
    current_data->hash_val = hash_val;
    current_data->next_row = 0;  // Assuming no chain initially
    current_data->rel_row_indice = row_indice;
    memcpy(keyvalue(rownum), key, len);
    buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
    last_row_indice = {0, 0};
  } else {
    // This bucket is already used, so need to check if it's a new key.
    while (rownum) {
      if (this->compare(key, rownum)) {
        // Found the key, so need to update it and return the last row indice to update linked list.
        last_row_indice = row(rownum)->rel_row_indice;
        row(rownum)->rel_row_indice = row_indice;
        return 0;
      }
      rownum = row(rownum)->next_row;
    }
    // Key not found, so need to create a new one.
    rownum = meta_data.m_row_count + 1;
    meta_data.m_row_count++;
    ++m_element_count_;
    RelHashIndexData* current_data = row(rownum);
    last_row_indice = current_data->rel_row_indice;
    current_data->hash_val = hash_val;
    current_data->rel_row_indice = row_indice;
    memcpy(keyvalue(rownum), key, len);
    current_data->next_row = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
    buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = rownum;
    last_row_indice = {0, 0};
  }

  return 0;
}

int RelHashIndex::delete_data(const char *key, int len) {
  RelHashCode hash_val = (*hash_func_)(key, len);
  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  size_t bucket_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);

  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bucket_idx);
  if (rownum > 0) {
    // Mark record as deleted (could use a flag or separate delete list)
    buckets_[bkt_ins_idx]->bucketValue(bucket_idx) = 0;
    return 0;
  } else {
    // This key not found.
    return -1;
  }
}

int RelHashIndex::get(const char *key, int len, RelRowIndice& last_row_indice) {
  RelHashCode hash_val = (*hash_func_)(key, len);
  size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
  size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
  size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);

  while (rownum) {
    // if (h_value->hash_val_ == rec[rownum].hash_val_) {
    if (this->compare(key, rownum)) {
      last_row_indice = row(rownum)->rel_row_indice;
      return 0;
    }
    rownum = row(rownum)->next_row;
  }
  return -1;
}

void RelHashIndex::printHashTable() {
  // Implementation for debugging
}

uint64_t RelHashIndex::getLSN() const {
  return meta_data.m_lsn;
}

void RelHashIndex::setLSN(uint64_t lsn) {
  meta_data.m_lsn = lsn;
}

RelHashIndexData* RelHashIndex::row(size_t n) const {
  return reinterpret_cast<RelHashIndexData*>(mem_hash_ + (n - 1) * meta_data.m_record_size);
}

char* RelHashIndex::keyvalue(size_t n) const {
  return mem_hash_ + (n - 1) * meta_data.m_record_size + sizeof(RelHashIndexData);
}

std::pair<bool, size_t> RelHashIndex::is_need_rehash() {
  if (m_element_count_ >= m_bucket_count_) {
    return std::make_pair(true, m_bucket_count_*2);
  }
  return std::make_pair(false, 0);
}
