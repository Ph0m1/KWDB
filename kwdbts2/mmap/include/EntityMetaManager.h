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
#include "mmap/MMapEntityMeta.h"

/**
 * @brief EntityMetaManager used for managing meta data in partitionTable.
 *        meta data store in  xxxx.meta.n files. n starts from 0, increasing.
 *        each meta file has fixed size, and cannot remap.
 *        if meta space not enough, new meta file created.
*/
class EntityMetaManager {
 public:
  EntityMetaManager();
  ~EntityMetaManager();

  // opening meta files, and generating meta data struct in memory, using mmap mechanism.
  int Open(const string &file_path, const std::string &db_path, const string &tbl_sub_path, bool alloc_block_item);

  // get entityMeta object in current partition.
  MMapEntityMeta* GetEntityMeta(uint meta_block_id);

  inline size_t getBlockMaxRows() const { return max_rows_per_block_; }
  inline uint64_t getBlockBitmapSize() const { return block_null_bitmap_size_;}
  inline uint64_t getBlockMaxNum() { return max_blocks_per_segment_;}

  // mark entity deleted by entityMeta object.
  inline void deleteEntity(uint32_t entity_id) { return entity_metas_[0]->deleteEntity(entity_id);}

  // get entityItem object in current partition.
  inline EntityItem* getEntityItem(uint entity_id) const { return entity_metas_[0]->getEntityItem(entity_id);}

  inline int64_t& minTimestamp() { return entity_metas_[0]->minTimestamp(); }

  inline int64_t& maxTimestamp() { return entity_metas_[0]->maxTimestamp(); }

  inline EntityHeader* getEntityHeader() { return entity_metas_[0]->getEntityHeader(); }

  // segment can store data row num.
  uint64_t getReservedRows() { return max_rows_per_block_ * max_blocks_per_segment_; }

  BlockItem* GetBlockItem(BLOCK_ID item_id);

  void lock() { pthread_mutex_lock(&obj_mutex_); }
  void unlock() { pthread_mutex_unlock(&obj_mutex_);}

  // if current block item is full, new block item and add to entity.
  int AddBlockItem(uint entity_id, BlockItem** blk_item);

  // update  entity struct info with blockitem.
  void UpdateEnityItem(uint entity_id, BlockItem* blk_item);

  // sync data to file.
  int sync(int flags);

  std::vector<uint32_t> getEntities() { return entity_metas_[0]->getEntities(); }
  MMapEntityMeta* GetFirstMeta() { return entity_metas_[0]; }

  /**
   * @brief	get all blockitem objects of entity.
   * @return	0 succeed, otherwise -1.
   */
  int GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse = false);

  int remove();
  void release();

  /**
 * @brief	 caculate block span start location in  partition row.
 *
 * @param in span
 * @return	0 succeed, otherwise -1.
 */
  MetricRowID GetFileStartRow(const BlockSpan& span) {
    return MetricRowID{span.block_item->block_id, span.start_row + 1};
  }

  inline int GetMetaIndex(BLOCK_ID block_item_id){
    int idx = (block_item_id -1) / META_BLOCK_ITEM_MAX;
    assert(idx < entity_metas_.size());
    assert(idx < meta_num_);
    assert(entity_metas_[idx] != nullptr);
    return idx;
  }

  /**
 * @brief	 mark blockspan rows deleted.
 *
 * @param in span 	  block span
 * @return	0 succeed, otherwise -1.
 */
  int MarkSpaceDeleted(uint32_t entity_id, BlockSpan* span);


  /**
   * @brief	publish blockspan ,so data can be access.
   *
   * @param in span
   * @param in entity_id
   * @return	0 succeed, otherwise -1.
   */
  int PublishSpace(BlockSpan* span, uint32_t entity_id);

  // resize vector space that storing meta file names.
  void resizeMeta();

  uint16_t  config_subgroup_entities = 500;   // configure item: entity max num in subgroup

 protected:
  std::vector<MMapEntityMeta*> entity_metas_;
  std::string file_path_base_;
  std::string db_path_;
  std::string tbl_sub_path_;
  size_t block_null_bitmap_size_;
  uint32_t max_blocks_per_segment_ = 1000;  //configure item
  uint16_t max_rows_per_block_ = 1000;      // configure item
  pthread_mutex_t obj_mutex_;
  std::shared_mutex entity_block_item_mutex_;  // control entity item / block item
  uint32_t meta_num_ = 1;
};