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

#include <climits>
#include <string>
#include <vector>
#include <utility>
#include "lru_cache.h"
#include "mmap/mmap_root_table_manager.h"
#include "ts_time_partition.h"
#include "libkwdbts2.h"
#include "mmap/mmap_tag_column_table.h"
#include "map"
#include "deque"

namespace kwdbts {
typedef uint32_t SubGroupID;
typedef uint32_t EntityID;

class TsSubEntityGroup : public TSObject {
 public:
  static const int cache_capacity_ = 10;

  explicit TsSubEntityGroup(MMapRootTableManager*& root_tbl_manager);

  virtual ~TsSubEntityGroup();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  /**
   * @brief	Open and initialize SubEntityGroup. Mainly for initializing meta files
   *        If flags=CREATE, an already existing error will be returned when the meta file exists.
   *
   * @param 	tbl_name
   * @param 	schema		root table schema
   * @param 	subgroup_id		subgroup id
   * @param 	db_path
   * @param 	tbl_sub_path		subgroup sub path
   * @param 	flags option to open a file; O_CREAT to create new file.
   * @param 	max_entities_per_subgroup  max entities in subgroup
   * @return	0 succeed, otherwise < 0.
   */
  int OpenInit(SubGroupID subgroup_id, const std::string& db_path, const string& tbl_sub_path,
               int flags, uint16_t max_entities_per_subgroup, ErrorInfo& err_info);

  /**
  * @brief subgroup ID
  */
  SubGroupID GetID() { return subgroup_id_; }

  /**
 * @brief Assign an EntityID when adding a new entity.
 * @param[in] primary_tag   Entity primary tag
 *
 * @return The assigned EntityID, if there are no available IDs, returns 0
  */
  EntityID AllocateEntityID(const string& primary_tag, ErrorInfo& err_info);

  /**
  * @brief SubGroup has already been assigned an EntityID collection.
  *
  * @return Set of EntityID vectors assigned
   */
  std::vector<uint32_t> GetEntities();

  /**
   * @brief Get the number of entities that SubEntityGroup can store
   * @return uint16_t
   */
  uint16_t GetSubgroupEntities();

  /**
* @brief delete entity
 * @param[in] entity_id   Entity ID
*
* @return error code
 */
  int DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  int UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  /**
* @brief Modify the number of entities that SubEntityGroup can store.
*         When creating a new time partition MetricsTable, use the modified values without affecting the existing partition.
* @param[in] entity_num   entity num
*
* @return error code
 */
  int SetEntityNum(uint32_t entity_num);

/**
 * @brief Calculate the partition time based on the data timestamp
 * @param[in] ts   time in seconds
 * @param[out] max_ts   The maximum time of this partition
 *
 * @return Return the partition time in seconds
 */
  timestamp64 PartitionTime(timestamp64 ts, timestamp64& max_ts);

  /**
 * @brief Query minimum partition time
 *
 * @return
 */
  timestamp64 MinPartitionTime();

  /**
 * @brief Query maximum partition time
 *
 * @return
 */
  timestamp64 MaxPartitionTime();

/**
 * @brief Retrieve MMapPartitionTable from the time partition directory
 * @param[in] ts   The time in seconds is internally converted to the time of the partition it belongs to.
 * @param[in] create_if_not_exist  True: If the group table does not exist, call the internal createPartitionTable to create it.
 * @param[in] lru_push_back  False: Insert lru_cache put into the header; True: Insert to the end.
 *
 * @return
 */
  TsTimePartition* GetPartitionTable(timestamp64 ts, ErrorInfo& err_info,
                                     bool create_if_not_exist = false,
                                     bool lru_push_back = false);

  /**
 * @brief  Filter partition table instances based on ts span
 * @param[in] ts_span
 *
 * @return
 */
  vector<TsTimePartition*> GetPartitionTables(const KwTsSpan& ts_span, ErrorInfo& err_info);

  /**
 * @brief Create MMapPartitionTable in the time partition directory
 * @param[in] ts   The time in seconds is internally converted to the time of the partition it belongs to.
 *
 * @return MMapPartitionTable
 */
  TsTimePartition* CreatePartitionTable(timestamp64 ts, ErrorInfo& err_info);

/**
 * @brief Remove partition table within the ts.
 * @param[in] ts   The time in seconds is internally converted to the time of the partition to which it belongs.
 * @param[in] skip_busy if set true, the deletion will be delayed until the next
 *                      scheduling when partition is busy, default false
 * @return error code
 */
  int RemovePartitionTable(timestamp64 ts, ErrorInfo& err_info, bool skip_busy = false);

/**
 * @brief Delete expired partition data
 * @param[in] end_ts  end timestamps of expired data
 *
 * @return error code
 */
  int RemoveExpiredPartition(int64_t end_ts, ErrorInfo& err_info);

/**
 * @brief delete all partition in subgroup
 *
 * @return error code
 */
  int RemoveAll(bool is_force, ErrorInfo& err_info);

/**
 * @brief Partition table cache elimination
 *
 * @return error code
 */
  void PartitionCacheEvict();

  /**
 * @brief Add new column information
 * @param[in] attr_info  Newly added columns
 *
 * @return error code
 */

  virtual void sync(int flags);

  inline void MutexLockEntity(int32_t entity_id) { MUTEX_LOCK(entity_mutexes_[entity_id]); }

  inline void MutexUnLockEntity(int32_t entity_id) { MUTEX_UNLOCK(entity_mutexes_[entity_id]); }

  inline void SetUnavailable() { available_ = false; }

  inline void SetAvailable() { available_ = true; }

  inline bool IsAvailable() { return available_; }

  int ReOpenInit(ErrorInfo& err_info);

  std::vector <timestamp64> GetPartitionTsInSpan(KwTsSpan ts_span);
  int ClearPartitionCache();
  int ErasePartitionCache(timestamp64 pt_ts);

  /**
   * @brief Apply the reorganization result to the subgroup, and the ongoing subgroup cannot be read or written
   * @param[in] obsolete_max_block: map[partition_ts][entity_id]block_id, record the latest block_id of the entity
   *            at the time of reorganization initiation, which is used to concatenate metas
   * @param[in] obsolete_segment_ids: map[partition_ts]{segment_id...}, record the segment_id under the partition when the
   *            reorganization starts, and these segments will be deleted
   * @param[in] compacted_block_items: map[partition_ts][entity_id]{BlockItem...}, the reorganized BlockItems in snapshot
   *            will replace blockItems in the source meta
   * @param[in] compact_dir: Snapshot path for moving reorganized files
  */
  int ApplyCompactData(std::map<timestamp64, std::map<uint32_t, BLOCK_ID>> &obsolete_max_block,
                       std::map<timestamp64, std::vector<BLOCK_ID>> &obsolete_segment_ids,
                       std::map<timestamp64, std::map<uint32_t, std::deque<BlockItem*>>> &compacted_block_items,
                       string compact_dir);

 private:
  std::string db_path_;
  std::string tbl_sub_path_;
  // The absolute path to the subgroup directory
  std::string real_path_;
  // Referenced from the root table manager of SubEntityGroupManager
  MMapRootTableManager*& root_tbl_manager_;
  std::string table_name_;
  SubGroupID subgroup_id_;
  // The entity block meta of subgroup is mainly used to manage the allocation of EntityIDs
  MMapEntityBlockMeta* entity_block_meta_{nullptr};
  // The set of all partition times: key=minimum partition time, value=maximum partition time
  map<timestamp64, timestamp64> partitions_ts_;
  // deleted but skipped partitions
  map<timestamp64, timestamp64> deleted_partitions_;
  // Using LRU to cache commonly used partition tables, key=partition time
  PartitionLRUCache partition_cache_;

  using TsSubEntityGroupEntityLatch = KLatch;
  std::vector<TsSubEntityGroupEntityLatch* > entity_mutexes_;

  using TsSubEntityGroupLatch = KLatch;
  TsSubEntityGroupLatch* sub_entity_group_mutex_;

  using TsSubEntityGroupRWLatch = KRWLatch;
  TsSubEntityGroupRWLatch* sub_entity_group_rwlock_;

  bool available_ = true;

  inline void partitionTime(timestamp64 target_ts, timestamp64 begin_ts, timestamp64 interval,
                            timestamp64& min_ts, timestamp64& max_ts);

  /**
 * @brief Internal method to obtain MMapPartitionTable, unlocked
 *
 * @return
 */
  TsTimePartition* getPartitionTable(timestamp64 p_time, timestamp64 max_ts, ErrorInfo& err_info,
                                     bool create_if_not_exist = false, bool lru_push_back = false);
  /**
  * @brief Internal method for creating MMapPartitionTable
  *
  * @return
  */
  TsTimePartition* createPartitionTable(string& pt_tbl_sub_path, timestamp64 p_time, timestamp64 max_ts,
                                        ErrorInfo& err_info);

  /**
  * @brief internal functions of RemoveMMapPartitionTable
  * if the partition is in use and skip_busy is true, defer deletion until the next scheduling
  *
  * @return error code
  */
  int removePartitionTable(TsTimePartition* mt_table, bool is_force, ErrorInfo& err_info, bool skip_busy = false);

  inline string partitionTblSubPath(timestamp64 p_time) {
    if (p_time >= 0) {
      return std::move(tbl_sub_path_ + std::to_string(p_time) + "/");
    } else {
      return std::move(tbl_sub_path_ + "m"+ std::to_string(abs(p_time)) + "/");
    }
  }

  //
  void deleteEntityItem(uint entity_id) {
    entity_block_meta_->deleteEntity(entity_id);
  }

  // Internal method to obtain all partition information: Lock, copy partitions.ts_, and return
  map<int64_t, int64_t> allPartitions();

  void mutexLock() override {
    MUTEX_LOCK(sub_entity_group_mutex_);
  }
  void mutexUnlock() override {
    MUTEX_UNLOCK(sub_entity_group_mutex_);
  }
};

}  // namespace kwdbts
