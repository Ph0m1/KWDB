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

#include <vector>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "mmap/MMapPartitionTable.h"
#include "mmap/MMapTagColumnTable.h"
#include "ts_common.h"
#include "lg_api.h"

namespace kwdbts {
class SubEntityGroupManager;
class BaseEntityIterator {
 public:
  virtual KStatus Init() = 0;
  virtual KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) = 0;
  virtual KStatus Close() = 0;
  virtual ~BaseEntityIterator() = default;
};

class EntityGroupTagIterator {
 public:
  EntityGroupTagIterator() = delete;
  EntityGroupTagIterator(MMapTagColumnTable* tag_bt, const std::vector<k_uint32>& scan_tags) : scan_tags_(scan_tags) {
    cur_total_row_count_ = 0;
    cur_scan_rowid_ = 1;
    tag_bt_ = tag_bt;
    tag_bt_->mutexLock();
    cur_total_row_count_ = tag_bt_->actual_size();
    tag_bt_->mutexUnlock();
  }
  virtual ~EntityGroupTagIterator();

  KStatus Init(MMapTagColumnTable* tag_bt);
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count);
  KStatus Close();

 private:
  size_t cur_total_row_count_;
  size_t cur_scan_rowid_;
  std::vector<k_uint32> scan_tags_;
  MMapTagColumnTable* tag_bt_;
};

class TagIterator : public BaseEntityIterator {
 public:
  explicit TagIterator(std::vector<EntityGroupTagIterator*>& tag_grp_iters) : entitygrp_iterator_(tag_grp_iters) {}
  ~TagIterator() override;

  KStatus Init() override;
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
  KStatus Close() override;

 private:
  std::vector<EntityGroupTagIterator*> entitygrp_iterator_;
  EntityGroupTagIterator* cur_iterator_ = nullptr;
  uint32_t cur_idx_{};
};

class EntityGroupMetaIterator {
 public:
  EntityGroupMetaIterator() = delete;

  EntityGroupMetaIterator(uint64_t range_group_id, SubEntityGroupManager* ebt_manager) :
                          range_group_id_(range_group_id), ebt_manager_(ebt_manager) {}

  ~EntityGroupMetaIterator() = default;

  KStatus Init();

  KStatus Next(std::vector <EntityResultIndex>* entity_list, k_uint32* count);

 private:
  uint64_t range_group_id_;
  SubEntityGroupManager* ebt_manager_;
  std::vector<EntityResultIndex> entity_list_;
  // mark reading location
  uint32_t cur_index_ = 0;
};

class MetaIterator : public BaseEntityIterator {
 public:
  explicit MetaIterator(std::vector<EntityGroupMetaIterator*>& iters) : iters_(iters) {}
  ~MetaIterator() override;

  KStatus Init() override;
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
  KStatus Close() override;

 private:
  std::vector<EntityGroupMetaIterator*> iters_;
  uint32_t cur_index_ = 0;
};

}  //  namespace kwdbts
