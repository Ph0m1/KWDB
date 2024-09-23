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

#include <iomanip>
#include "kwdb_type.h"
#include "ts_time_partition.h"
#include "mmap/mmap_segment_table.h"
#include "mmap/ts_Iterator_4_partition.h"
#include "st_subgroup.h"


using namespace kwdbts;

/**
 * @brief iterator used for scan all data in sub group.
 *        we will scan partition by partition, and return generated batch object 
*/
class TsSubGroupIterator {
 public:
  TsSubGroupIterator(TsSubEntityGroup* sub_group, const TsPartitonIteratorParams& params) :
        sub_group_(sub_group), params_(params) {
    partition_obj_iter_ = sub_group_->GetPTIteartor(params_.ts_spans);
  }

  ~TsSubGroupIterator() {
    if (partition_data_iter_ != nullptr) {
      delete partition_data_iter_;
      partition_data_iter_ = nullptr;
    }
  }

  // if count =0 means all scan over. res.enityindex info mark which entity this res belong to.
  KStatus Next(ResultSet* res, uint32_t* count, bool full_block = false, TsBlockFullData* block_data = nullptr) {
    while (true) {
      if (partition_data_iter_ != nullptr) {
        KStatus s = partition_data_iter_->Next(res, count, full_block, block_data);
        if (s == KStatus::FAIL) {
          LOG_ERROR("get next partition object faild");
          return s;
        }
        if  (*count > 0) {
          return KStatus::SUCCESS;
        } else {
          // current partition is scan over.
          delete partition_data_iter_;
          partition_data_iter_ = nullptr;
        }
      } else {
        // current partition is scan over. or just begin scan.
        TsTimePartition* cur_partition_table{nullptr};
        KStatus s = partition_obj_iter_->Next(&cur_partition_table);
        if (s == KStatus::FAIL) {
          LOG_ERROR("get next partition object faild");
          return s;
        }
        if (cur_partition_table == nullptr) {
          // all scan over.
          *count = 0;
          return KStatus::SUCCESS;
        }
        partition_data_iter_ = new TsPartitonIterator(cur_partition_table, params_);
      }      
    }
    return KStatus::FAIL;
  }

 private:
  TsSubEntityGroup* sub_group_;
  TsPartitonIteratorParams params_;
  std::shared_ptr<TsSubGroupPTIterator> partition_obj_iter_{nullptr};
  TsPartitonIterator* partition_data_iter_{nullptr};
  int cur_entity_index_{-1};
  MMapSegmentTableIterator* segment_iter_{nullptr};
};

/**
 * @brief iterator used for scan all data in sub group.
 *        we will scan entity by entity, and return generated batch object 
*/
class TsSubGroupIteratorEntityBased {
 public:
  TsSubGroupIteratorEntityBased(TsSubEntityGroup* sub_group, const TsPartitonIteratorParams& params) :
   sub_group_(sub_group), params_(params) {}

  ~TsSubGroupIteratorEntityBased() {
    if (cur_entity_iter_ != nullptr) {
      delete cur_entity_iter_;
      cur_entity_iter_ = nullptr;
    }
  }
  KStatus Next(ResultSet* res, uint32_t* count) {
    return nextBatch(res, count, false, nullptr);
  }

  KStatus NextFullBlock(ResultSet* res, TsBlockFullData* block_data, bool* scan_over) {
    uint32_t count = 0;
    KStatus s = nextBatch(res, &count, true, block_data);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("next full block failed.");
      return s;
    }
    *scan_over = false;
    if (count == 0) {
      *scan_over = true;
    }
    return KStatus::SUCCESS;
  }

  KStatus nextBatch(ResultSet* res, uint32_t* count, bool full_block = false, TsBlockFullData* block_data = nullptr) {
    while (true) {
      if (cur_entity_iter_ != nullptr) {
        KStatus s = cur_entity_iter_->Next(res, count, full_block, block_data);
        if (s == KStatus::FAIL) {
          LOG_ERROR("get next entity data faild");
          return s;
        }
        if (*count > 0) {
          // get some data
          return KStatus::SUCCESS;
        } else {
          // current entity scan over.
          delete cur_entity_iter_;
          cur_entity_iter_ = nullptr;
        }
      } else {
        // current entity scan over. or just begin scan.
        cur_entity_idx_++;
        if (cur_entity_idx_ >= params_.entity_ids.size()) {
          // all entities scan over.
          *count = 0;
          return KStatus::SUCCESS;
        }
        TsPartitonIteratorParams entity_param = params_;
        entity_param.entity_ids = {params_.entity_ids[cur_entity_idx_]};
        cur_entity_iter_ = new TsSubGroupIterator(sub_group_, entity_param);
      }  
    }
    return KStatus::FAIL;
  }

 private:
  TsSubEntityGroup* sub_group_;
  TsPartitonIteratorParams params_;
  int cur_entity_idx_{-1};
  TsSubGroupIterator* cur_entity_iter_{nullptr};
};

