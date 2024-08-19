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

#include <thread>
#include "ts_entity_idx_manager.h"
#include "utils/big_table_utils.h"
#include "lg_api.h"
#include "sys_utils.h"

TsEntityIdxManager::TsEntityIdxManager() {
  pthread_mutex_init(&obj_mutex_, NULL);
  max_rows_per_block_ = CLUSTER_SETTING_MAX_ROWS_PER_BLOCK;
  max_blocks_per_segment_ = CLUSTER_SETTING_MAX_BLOCK_PER_SEGMENT;
  block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
  entity_block_idxs_.resize(1000, nullptr);
}

TsEntityIdxManager::~TsEntityIdxManager() {
  for (auto& ptr : entity_block_idxs_) {
    if (ptr) delete ptr;
  }
}

int TsEntityIdxManager::Open(const string& file_path, const std::string& db_path, const string& tbl_sub_path,
                             bool alloc_block_item) {
  int i = 0;
  int err_code = 0;
  file_path_base_ = file_path;
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;

  while (true) {
    // open all meta files continous.
    std::string meta_file = file_path + "." + std::to_string(i);
    bool is_exist = IsExists(db_path_ + tbl_sub_path_ + meta_file);
    int flags = MMAP_OPEN_NORECURSIVE;
    if (i == 0 && !is_exist) {
      flags = MMAP_CREAT_EXCL;
    }
    if (i == 0 || is_exist) {
      MMapEntityIdx* entity_block_idx = new MMapEntityIdx(false, i == 0);
      err_code = entity_block_idx->init(meta_file, db_path, tbl_sub_path, flags, alloc_block_item, config_subgroup_entities);
      if (err_code < 0) {
        for (auto& ptr : entity_block_idxs_) {
          if (ptr) delete ptr;
        }
        return err_code;
      }

      if (i != 0) {
        entity_block_idx->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
        entity_block_idx->block_item_offset_num_ = i * META_BLOCK_ITEM_MAX;
      }

      entity_block_idxs_[i] = entity_block_idx;
      meta_num_ = ++i;
      resizeMeta();
    } else {
      break;
    }
  }

  // sync meta file paramter value. with mem struct.
  if (entity_block_idxs_[0]->entity_header_->max_blocks_per_segment == 0) {
    entity_block_idxs_[0]->entity_header_->max_blocks_per_segment = max_blocks_per_segment_;
  } else {
    max_blocks_per_segment_ = entity_block_idxs_[0]->entity_header_->max_blocks_per_segment;
  }

  if (entity_block_idxs_[0]->entity_header_->max_rows_per_block == 0) {
    entity_block_idxs_[0]->entity_header_->max_rows_per_block = max_rows_per_block_;
  } else {
    max_rows_per_block_ = entity_block_idxs_[0]->entity_header_->max_rows_per_block;
  }

  block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;

  if (entity_block_idxs_[0]->entity_header_->version != METRIC_VERSION) {
    LOG_ERROR("The [%s] versions don't match: Code Metric Version[%d], Data Metric Version[%d]",
              file_path.c_str(), METRIC_VERSION, entity_block_idxs_[0]->entity_header_->version);
    char* version_validate = getenv("KW_DATA_VERSION_VALIDATE");
    if (version_validate == nullptr || strcmp(version_validate, "true") == 0) {
      exit(1);
    }
  }
  return 0;
}

BlockItem* TsEntityIdxManager::GetBlockItem(BLOCK_ID item_id) {
  return entity_block_idxs_.at(GetMetaIndex(item_id))->GetBlockItem(item_id);
}

int TsEntityIdxManager::sync(int flags) {
  int meta_size = entity_block_idxs_.size();
  for (int i = 0; i < meta_size; i++) {
    int error_code = entity_block_idxs_[i]->sync(flags);
    if (error_code < 0) return error_code;
  }

  return 0;
}

int TsEntityIdxManager::GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  while (block_item_id != 0) {
    BlockItem* blockItem = GetBlockItem(block_item_id);
    if (blockItem == nullptr || blockItem->entity_id == 0) {
      return KWENOOBJ;
    }
    reverse ? block_items.push_back(blockItem) : block_items.push_front(blockItem);
    block_item_id = blockItem->prev_block_id;
  }
  return 0;
}

// delete meta files and structs in memory.
int TsEntityIdxManager::remove() {
  int error_code = 0;
  int meta_size = entity_block_idxs_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_idxs_[i]) {
      error_code = entity_block_idxs_[i]->remove();
      if (error_code < 0) return -1;
      delete entity_block_idxs_[i];
    }
  }
  entity_block_idxs_.clear();
  return error_code;
}

void TsEntityIdxManager::release() {
  int meta_size = entity_block_idxs_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_idxs_[i]) delete entity_block_idxs_[i];
  }
  entity_block_idxs_.clear();
}

int TsEntityIdxManager::MarkSpaceDeleted(uint32_t entity_id, BlockSpan* span) {
  std::lock_guard<std::shared_mutex> lk(entity_block_item_mutex_);
  BlockItem* block_item = span->block_item;
  assert(block_item != nullptr);
  setBatchDeleted(block_item->rows_delete_flags, span->start_row + 1, span->row_num);
  return 0;
}

void TsEntityIdxManager::resizeMeta() {
  if (meta_num_ > entity_block_idxs_.size() * 0.8) {
    entity_block_idxs_.resize(entity_block_idxs_.size() * 2);
  }
}

int TsEntityIdxManager::AddBlockItem(uint entity_id, BlockItem** blk_item) {
  EntityHeader* header = getEntityHeader();
  BLOCK_ID new_block_item_id = header->cur_block_id + 1;

  int err_code = 0;
  if (new_block_item_id > meta_num_ * META_BLOCK_ITEM_MAX) {
    // meta file has no space to store new blockitem, we need create new meta file.
    int i = meta_num_;
    std::string meta_file = file_path_base_ + "." + std::to_string(i);

    MMapEntityIdx* entity_block_idx = new MMapEntityIdx(false, false);
    err_code = entity_block_idx->init(meta_file, db_path_, tbl_sub_path_, MMAP_CREAT_EXCL, true, config_subgroup_entities);
    if (err_code < 0) {
      LOG_ERROR("Create meta file %s fail. error_code: %d", meta_file.c_str(), err_code);
      pthread_mutex_unlock(&obj_mutex_);
      return err_code;
    }

    entity_block_idx->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
    entity_block_idx->block_item_offset_num_ = i * META_BLOCK_ITEM_MAX;
    entity_block_idxs_[i] = entity_block_idx;
    meta_num_ = i + 1;
    resizeMeta();
  }

  header->cur_block_id++;
  auto entityItem = getEntityItem(entity_id);
  BLOCK_ID lastest_block_item_id = entityItem->cur_block_id;

  // get new blockitem from meta file,and initalize it.
  auto blockItem = GetBlockItem(new_block_item_id);
  memset(blockItem, 0, sizeof(BlockItem));
  blockItem->prev_block_id = lastest_block_item_id;
  blockItem->entity_id = entity_id;
  blockItem->block_id = new_block_item_id;

  *blk_item = blockItem;
  return err_code;
}

void TsEntityIdxManager::UpdateEnityItem(uint entity_id, BlockItem* blk_item) {
  auto entityItem = getEntityItem(entity_id);
  entityItem->block_count++;
  entityItem->cur_block_id = blk_item->block_id;
}
