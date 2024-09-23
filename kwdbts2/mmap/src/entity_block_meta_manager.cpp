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


#include <thread>
#include "dirent.h"
#include "sys/types.h"
#include "entity_block_meta_manager.h"
#include "utils/big_table_utils.h"
#include "st_config.h"
#include "sys_utils.h"

EntityBlockMetaManager::EntityBlockMetaManager() {
  pthread_mutex_init(&obj_mutex_, NULL);
  entity_block_metas_.resize(1000, nullptr);
}

EntityBlockMetaManager::~EntityBlockMetaManager() {
  for (auto& ptr : entity_block_metas_) {
    if (ptr) delete ptr;
  }
}

int EntityBlockMetaManager::Open(const string& file_path, const std::string& db_path, const string& tbl_sub_path,
                                 bool alloc_block_item) {
  table_id_ = std::atoi(getTsObjectName(file_path).c_str());
  file_path_base_ = file_path;
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  //  After finding all the metas first, then use map to ensure the order,
  //  at least meta.0 should be the first, and then calculate the offset will not be wrong.
  std::map<uint64_t, string> meta_map;
  int err_code = 0;
  DIR* dir_ptr = opendir((db_path_ + tbl_sub_path).c_str());
  if (!dir_ptr) {
    return KWENOOBJ;
  }

  struct dirent* entity;
  while ((entity = readdir(dir_ptr)) != nullptr) {
    if (entity->d_type != DT_REG) {
      continue;
    }
    string meta_prefix = file_path + ".";
    //  entity->d_name: 78.meta.XX
    //  meta_prefix: 78.meta
    if (strncmp(entity->d_name, meta_prefix.c_str(), meta_prefix.length()) == 0) {
      char* endptr;
      errno = 0;
      uint64_t num = std::strtoull(entity->d_name + meta_prefix.length(), &endptr, 10);
      if ((errno == ERANGE && num == ULLONG_MAX) || (errno != 0 && num == 0)) {
        continue;
        // There is a file name that starts with "78.meta.", but it is not followed by a number, ignore it
      }
      meta_map[num] = entity->d_name;
    }
  }
  closedir(dir_ptr);

  if (meta_map.empty()) {
    // There is no meta file in the path, initialize and create a meta.0
    auto entity_meta = new MMapEntityBlockMeta(false, true);
    int flags = MMAP_CREAT_EXCL;
    err_code = entity_meta->init(file_path + ".0", db_path, tbl_sub_path, flags, alloc_block_item,
                                 max_entities_per_subgroup);
    if (err_code < 0) {
      return err_code;
    }
    entity_block_metas_[0] = entity_meta;
    meta_num_ = 1;
  } else if (meta_map.find(0) == meta_map.end()) {
    // There is a meta file but not meta.0
    return KWENOOBJ;
  } else {
    bool success = true;
    for (auto& it: meta_map) {
      const uint64_t& num = it.first;
      string& file_name = it.second;
      if (meta_num_ < num + 1) {
        // Because the meta number starts from 0, +1 is the amount of metas that the system considers
        meta_num_ = num + 1;
      }
      int flags = MMAP_OPEN_NORECURSIVE;
      auto entity_meta = new MMapEntityBlockMeta(false, num == 0);
      err_code = entity_meta->init(file_name, db_path, tbl_sub_path, flags, alloc_block_item, max_entities_per_subgroup);
      if (err_code < 0) {
        success = false;
        break;
      }
      if (num != 0) {
        entity_meta->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
        // We will take entity_metas_[0] to get offset here, so be sure to do it in order
        entity_meta->block_item_offset_num_ = num * META_BLOCK_ITEM_MAX;
      }
      resizeMeta();
      // Since num is not necessarily continuous, we should check whether resize is required first,
      // otherwise, the num is too large and the read entity_metas_ will crash
      entity_block_metas_[num] = entity_meta;
    }
    if (!success) {
      for (auto& ptr: entity_block_metas_) {
        if (ptr) delete ptr;
      }
      return err_code;
    }
  }

  if (entity_block_metas_[0]->entity_header_->version != METRIC_VERSION) {
    LOG_ERROR("The [%s] versions don't match: Code Metric Version[%d], Data Metric Version[%d]",
              file_path.c_str(), METRIC_VERSION, entity_block_metas_[0]->entity_header_->version);
    char* version_validate = getenv("KW_DATA_VERSION_VALIDATE");
    if (version_validate == nullptr || strcmp(version_validate, "true") == 0) {
      exit(1);
    }
  }
  return 0;
}

BlockItem* EntityBlockMetaManager::GetBlockItem(BLOCK_ID item_id) {
  return entity_block_metas_.at(GetMetaIndex(item_id))->GetBlockItem(item_id);
}

int EntityBlockMetaManager::sync(int flags) {
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    int error_code = entity_block_metas_[i]->sync(flags);
    if (error_code < 0) return error_code;
  }

  return 0;
}

int EntityBlockMetaManager::GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  while (block_item_id != 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item == nullptr || block_item->entity_id == 0) {
      return KWENOOBJ;
    }
    reverse ? block_items.push_back(block_item) : block_items.push_front(block_item);
    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

// delete meta files and structs in memory.
int EntityBlockMetaManager::remove() {
  int error_code = 0;
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_metas_[i]) {
      error_code = entity_block_metas_[i]->remove();
      if (error_code < 0) return -1;
      delete entity_block_metas_[i];
    }
  }
  entity_block_metas_.clear();
  return error_code;
}

void EntityBlockMetaManager::release() {
  int meta_size = entity_block_metas_.size();
  for (int i = 0; i < meta_size; i++) {
    if (entity_block_metas_[i]) delete entity_block_metas_[i];
  }
  entity_block_metas_.clear();
}

int EntityBlockMetaManager::MarkSpaceDeleted(uint32_t entity_id, BlockSpan* span) {
  std::lock_guard<std::shared_mutex> lk(entity_block_item_mutex_);
  BlockItem* block_item = span->block_item;
  assert(block_item != nullptr);
  setBatchDeleted(block_item->rows_delete_flags, span->start_row + 1, span->row_num);
  return 0;
}

void EntityBlockMetaManager::resizeMeta() {
  if (meta_num_ > entity_block_metas_.size() * 0.8) {
    // Data reorganization may result in meta_num_ being much larger than the current size,
    // so it is safer to use meta_num_ to resize
    entity_block_metas_.resize(meta_num_ * 2);
    // entity_metas_.resize(entity_metas_.size()*2);
  }
}

int EntityBlockMetaManager::AddBlockItem(uint entity_id, BlockItem** blk_item) {
  EntityHeader* header = getEntityHeader();
  BLOCK_ID new_block_item_id = header->cur_block_id + 1;

  int err_code = 0;
  if (new_block_item_id > meta_num_ * META_BLOCK_ITEM_MAX) {
    // meta file has no space to store a new block item, we need create new meta file.
    int i = (new_block_item_id - 1) / META_BLOCK_ITEM_MAX;
    // The meta number may not be consecutive, and should correspond to the largest block_id
    std::string meta_file = file_path_base_ + "." + std::to_string(i);

    MMapEntityBlockMeta* entity_block_meta = new MMapEntityBlockMeta(false, false);
    err_code = entity_block_meta->init(meta_file, db_path_, tbl_sub_path_, MMAP_CREAT_EXCL, true, max_entities_per_subgroup);
    if (err_code < 0) {
      LOG_ERROR("Create meta file %s fail. error_code: %d", meta_file.c_str(), err_code);
      pthread_mutex_unlock(&obj_mutex_);
      return err_code;
    }

    entity_block_meta->SetEntityItemOffset(GetFirstMeta()->entity_item_offset_);
    entity_block_meta->block_item_offset_num_ = i * META_BLOCK_ITEM_MAX;
    entity_block_metas_[i] = entity_block_meta;
    meta_num_ = i + 1;
    resizeMeta();
  }

  header->cur_block_id++;
  auto entity_item = getEntityItem(entity_id);
  BLOCK_ID latest_block_item_id = entity_item->cur_block_id;

  // get new block item from meta file,and initialize it.
  auto block_item = GetBlockItem(new_block_item_id);
  memset(block_item, 0, sizeof(BlockItem));
  block_item->prev_block_id = latest_block_item_id;
  block_item->entity_id = entity_id;
  block_item->block_id = new_block_item_id;

  *blk_item = block_item;
  return err_code;
}

void EntityBlockMetaManager::UpdateEntityItem(uint entity_id, BlockItem* blk_item) {
  auto entity_item = getEntityItem(entity_id);
  entity_item->block_count++;
  entity_item->cur_block_id = blk_item->block_id;
}

int EntityBlockMetaManager::updateCompactMeta(std::map<uint32_t, BLOCK_ID>& obsolete_max_block,
                                              std::map<uint32_t, std::deque<BlockItem*>>& compacted_block_items) {

  //  obsolete_max_block: The maximum block_id per entity before the reorganization begins
  //  compacted_block_items: The blockItem for each entity from the snapshot after the reorganization
  std::map<uint32_t, BlockItem*> link_block; // map[entity_id]
  // The number of blocks before the reorganization,
  // that is, the number of blocks that will be replaced
  std::map<uint32_t, uint32_t> old_list_count;
  for (auto & it : obsolete_max_block) {
    uint32_t entity_id = it.first;
    BLOCK_ID obsolete_max_block_id = it.second;

    EntityItem* entity_item = getEntityItem(entity_id);
    if (obsolete_max_block_id == 0) {  // Represents this entity that has been found to be deleted in CompactingStatus.
      if (entity_item->need_compact) {
        entity_item->need_compact = false;  // Avoid calling for PrepareCompactData next time.
      }
      continue;
    }
    if (!entity_item || entity_item->cur_block_id <= 0) {
      LOG_ERROR("entity_id %d is not exist", entity_id);
      return KWENOOBJ;
    }

    link_block[entity_id] = nullptr; // The block that will be connected to the new block item
    BLOCK_ID blk_id = entity_item->cur_block_id;

    old_list_count[entity_id] = 0;
    uint32_t before_obsolete_max_block = 0;
    // if there is new data coming when reorganizing
    if (blk_id != obsolete_max_block_id) {
      // objective1: find out link_block.
      // objective2: calculate old_list_count
      while (blk_id > 0) {
        auto block_ptr = GetBlockItem(blk_id);
        before_obsolete_max_block++;
        if (block_ptr->prev_block_id == obsolete_max_block_id) {
          link_block[entity_id] = block_ptr;
          break;
        }
        blk_id = block_ptr->prev_block_id;
      }
    }
    if (blk_id == 0) {
      // Represents the disappearance of the block that existed before the reorganization
      LOG_ERROR("block %d is missing", obsolete_max_block_id);
      return KWENOOBJ;
    }
    // number before reorganization = current number - number of blocks before max block
    old_list_count[entity_id] =
        entity_item->block_count - before_obsolete_max_block;
  }

  for (auto & it : old_list_count) {
    uint32_t entity_id = it.first;
    BlockItem* link = link_block[entity_id];
    uint32_t new_list_count = 0;

    EntityItem* entity_item = getEntityItem(entity_id);
    while (!compacted_block_items[entity_id].empty()) {
      BlockItem* src = compacted_block_items[entity_id].back();
      BlockItem* dst = GetBlockItem(src->block_id); // Get the blockItem of the same block_id from meta
      memcpy(dst, src, sizeof(BlockItem)); // Copy the blockItem information to the meta of the source table
      dst->entity_id = entity_id;
      // Although the key of the compacted_block_items is the entity_id of the source table, the blockItem in
      // it is still the entity_id of the snapshot, so it needs to be changed
      if (new_list_count == 0 && !link) {
        // link_block is nullptr at first time, means that from the initiation of the reorganization to the present,
        // there is no new data insert
        entity_item->cur_block_id = dst->block_id;
      }
      if (link) { // means there is new data insert
        link->prev_block_id = dst->block_id;
      }

      link = dst;
      // free(src);
      compacted_block_items[entity_id].pop_back();
      new_list_count++;
    }

    entity_item->block_count = entity_item->block_count - it.second + new_list_count;
    if (entity_item->is_disordered) {
      // The reorganization has completed the sorting of entity
      entity_item->is_disordered = false;
    }
    if (entity_item->need_compact) {
      entity_item->need_compact = false;
    }
  }
  return 0;
}
