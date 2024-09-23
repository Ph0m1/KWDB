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

#include "tag_iterator.h"
#include "st_group_manager.h"
#include "lt_cond.h"

namespace kwdbts {
EntityGroupTagIterator::~EntityGroupTagIterator() {
  // TODO(zhuderun): tag table release
}

KStatus EntityGroupTagIterator::Init(MMapTagColumnTable* tag_bt) {
  // TODO(zhuderun): add table ref
  tag_bt_ = tag_bt;
  tag_bt_->mutexLock();
  cur_total_row_count_ = tag_bt_->actual_size();
  tag_bt_->mutexUnlock();
  return KStatus::SUCCESS;
}
bool in(kwdbts::k_uint32 hp, const std::vector<kwdbts::k_uint32>& hps) {
  for (int i=0; i < hps.size(); i++) {
    if (hps[i] == hp) {
      return true;
    }
  }
  return false;
}

KStatus EntityGroupTagIterator::Next(std::vector<EntityResultIndex>* entity_id_list,
                                     ResultSet* res, k_uint32* count) {
  if  (cur_scan_rowid_ > cur_total_row_count_) {
    LOG_INFO("fetch tag table %s%s, "
      "cur_scan_rowid[%lu] > cur_total_row_count_[%lu], count: %u",
      tag_bt_->sandbox().c_str(), tag_bt_->name().c_str(), cur_scan_rowid_,
      cur_total_row_count_, *count);
    return(KStatus::SUCCESS);
  }

  uint32_t fetch_count = *count;
  bool has_data = false;
  size_t start_row = 0;
  size_t row_num = 0;
  ErrorInfo err_info;
  tag_bt_->startRead();
  for (row_num = cur_scan_rowid_; row_num <= cur_total_row_count_; row_num++) {
    if (fetch_count >= ONE_FETCH_COUNT) {
      LOG_DEBUG("fetch_count[%u] >= ONECE_FETCH_COUNT[%u]", fetch_count,
        ONE_FETCH_COUNT);
      // m_cur_scan_rowid_=row_num;
      goto success_end;
      // *count = fetch_count;
      // return KStatus::SUCCESS;
    }
    bool needSkip = false;
    uint32_t hash_point;
    if (version_ == TagIteratorType::TAG_IT_HASHED) {
      tag_bt_->getHashpointByRowNum(row_num, &hash_point);
      needSkip = !in(hash_point, hps_);
      LOG_DEBUG("row %lu hashpoint is %u %s in search list", row_num, hash_point, needSkip?"not":"");
    }
    if (!tag_bt_->isValidRow(row_num) || needSkip) {
      if (has_data) {
        for (int idx = 0; idx < scan_tags_.size(); idx++) {
          // Batch* batch = new Batch(m_tag_bt_->GetColumnAddr(start_row, tagid),
          // (row_num - start_row), m_tag_bt_->getBitmapAddr(start_row, tagid));
          Batch* batch = GenTagBatchRecord(tag_bt_, start_row, row_num, scan_tags_[idx], err_info);
          if (!batch) {
            LOG_ERROR("failed to get batch from the tag table %s%s",
              tag_bt_->sandbox().c_str(), tag_bt_->name().c_str());
            tag_bt_->stopRead();
            return (KStatus::FAIL);
          }
          res->push_back(idx, batch);
        }
        has_data = false;
        start_row = row_num + 1;
      }
      continue;
    }
    if (!has_data) {
      start_row = row_num;
      has_data = true;
    }
    if (version_ == TagIteratorType::TAG_IT_HASHED) {
      tag_bt_->getHashedEntityIdByRownum(row_num, hash_point, entity_id_list);
    } else {
      tag_bt_->getEntityIdByRownum(row_num, entity_id_list);
    }
    fetch_count++;
  }  // end for
if (fetch_count == *count) {
  LOG_WARN("no valid record in the tag table %s%s, cur_total_row_count_[%lu], "
      "cur_scan_rowid_[%lu]",
      tag_bt_->sandbox().c_str(), tag_bt_->name().c_str(), cur_total_row_count_,
      cur_scan_rowid_);
  tag_bt_->stopRead();
  return (KStatus::SUCCESS);
}
success_end:
  if (start_row < row_num) {
    // need start_row < end_row
    for (int idx = 0; idx < scan_tags_.size(); idx++) {
      Batch* batch = GenTagBatchRecord(tag_bt_, start_row, row_num, scan_tags_[idx], err_info);
      if (!batch) {
        LOG_ERROR("failed to get batch from the tag table %s%s",
          tag_bt_->sandbox().c_str(), tag_bt_->name().c_str());
        tag_bt_->stopRead();
        return (KStatus::FAIL);
      }
      res->push_back(idx, batch);
    }
  } else {
    LOG_INFO("not required to call GenTagBatchRecord on the tag table %s%s, "
      "fetch_count: %u, start_row[%lu] >= row_num[%lu]",
      tag_bt_->sandbox().c_str(), tag_bt_->name().c_str(), fetch_count,
      start_row, row_num);
  }
  tag_bt_->stopRead();
  *count = fetch_count;
  LOG_INFO("fatch the tag table %s%s, fetch_count: %u",
    tag_bt_->sandbox().c_str(), tag_bt_->name().c_str(), *count);
  cur_scan_rowid_ = row_num;
  return (KStatus::SUCCESS);
}

KStatus EntityGroupTagIterator::Close() {
  if (tag_bt_ != nullptr) {
    int ref_cnt = tag_bt_->decRefCount();
    if (ref_cnt <= 1) {
      KW_COND_SIGNAL(tag_bt_->m_ref_cnt_cv_);
    }
  }
  return (KStatus::SUCCESS);
}

TagIterator::~TagIterator() {
  for (uint32_t idx = 0; idx < entitygrp_iterator_.size(); idx++) {
    entitygrp_iterator_[idx]->Close();
    delete entitygrp_iterator_[idx];
    entitygrp_iterator_[idx] = nullptr;
  }
  entitygrp_iterator_.clear();
}

KStatus TagIterator::Init() {
  if (entitygrp_iterator_.empty()) {
    LOG_ERROR("invalid EntityGroupTagIterator");
    return (KStatus::FAIL);
  }
  cur_idx_ = 0;
  cur_iterator_ = entitygrp_iterator_[cur_idx_];
  return KStatus::SUCCESS;
}

KStatus TagIterator::Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) {
  if (cur_idx_ >= entitygrp_iterator_.size()) {
    *count = 0;
    return KStatus::SUCCESS;
  }
  uint32_t fetch_count = 0;
  KStatus status = KStatus::SUCCESS;
  while (fetch_count < ONE_FETCH_COUNT && cur_idx_ < entitygrp_iterator_.size())  {
    cur_iterator_ = entitygrp_iterator_[cur_idx_];
    if (KStatus::SUCCESS != cur_iterator_->Next(entity_id_list, res, &fetch_count)) {
      LOG_ERROR("failed to get next batch");
      return KStatus::FAIL;
    }
    if (fetch_count >= ONE_FETCH_COUNT) {
      break;
    }
    cur_idx_++;
  }
  *count = fetch_count;
  return KStatus::SUCCESS;
}


KStatus TagIterator::Close() {
  for (uint32_t idx = 0; idx < entitygrp_iterator_.size(); idx++) {
    entitygrp_iterator_[idx]->Close();
    delete entitygrp_iterator_[idx];
    entitygrp_iterator_[idx] = nullptr;
  }
  entitygrp_iterator_.clear();
  return KStatus::SUCCESS;
}

KStatus EntityGroupMetaIterator::Init() {
  ErrorInfo error_info;
  for (uint32_t i = 1; i <= ebt_manager_->GetMaxSubGroupId(); ++i) {
    TsSubEntityGroup* subgroup = ebt_manager_->GetSubGroup(i, error_info);
    if (!subgroup) {
      continue;
    }
    std::vector<uint32_t> entities = subgroup->GetEntities();
    for (auto& entity : entities) {
      entity_list_.push_back(EntityResultIndex(range_group_id_, entity, i));
    }
    // ebt_manager_->ReleasePartitionTable(subgroup);
  }
  if (entity_list_.empty()) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus EntityGroupMetaIterator::Next(std::vector<EntityResultIndex>* entity_list, k_uint32* count) {
  if (cur_index_ >= entity_list_.size()) {
    return KStatus::SUCCESS;
  }
  size_t fetch_count = 0;
  while (cur_index_ < entity_list_.size()) {
    entity_list->push_back(entity_list_[cur_index_++]);
    if (++fetch_count >= (ONE_FETCH_COUNT - *count)) {
      break;
    }
  }
  *count += fetch_count;
  return KStatus::SUCCESS;
}

MetaIterator::~MetaIterator() {
  for (auto& iter : iters_) {
    delete iter;
    iter = nullptr;
  }
  iters_.clear();
}

KStatus MetaIterator::Init() {
  cur_index_ = 0;
  return SUCCESS;
}

KStatus MetaIterator::Next(std::vector<EntityResultIndex>* entity_list, ResultSet* res, k_uint32* count) {
  if (cur_index_ >= iters_.size()) {
    *count = 0;
    return KStatus::SUCCESS;
  }
  uint32_t fetch_count = 0;
  while (cur_index_ < iters_.size())  {
    EntityGroupMetaIterator* cur_iter = iters_[cur_index_];
    cur_iter->Next(entity_list, &fetch_count);
    if (fetch_count >= ONE_FETCH_COUNT) {
      break;
    }
    ++cur_index_;
  }
  *count = fetch_count;
  LOG_DEBUG("SUCCESS fetch_count: %u ", fetch_count);
  return KStatus::SUCCESS;
}

KStatus MetaIterator::Close() {
  for (auto& iter : iters_) {
    delete iter;
    iter = nullptr;
  }
  iters_.clear();
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
