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

#include <lg_api.h>
#include <dirent.h>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include "st_subgroup.h"
#include "sys_utils.h"
#include "mmap/mmap_metrics_table.h"
#include "lt_rw_latch.h"

namespace kwdbts {

TsSubEntityGroup::TsSubEntityGroup(MMapMetricsTable*& root_tbl)
    : TSObject(), root_tbl_(root_tbl) {
  table_name_ = root_tbl->name();
  // pthread_rwlock_init(&partition_lk_, NULL);
  partition_cache_.SetCapacity(cache_capacity_);  // Each subgroup can have up to 10 active partitions

  sub_entity_group_mutex_ = new TsSubEntityGroupLatch(LATCH_ID_TSSUBENTITY_GROUP_MUTEX);
  sub_entity_group_rwlock_ = new TsSubEntityGroupRWLatch(RWLATCH_ID_TS_SUB_ENTITY_GROUP_RWLOCK);
}

TsSubEntityGroup::~TsSubEntityGroup() {
  partition_cache_.Clear();
  // pthread_rwlock_destroy(&partition_lk_);
  if (entity_block_idx_ != nullptr) {
    delete entity_block_idx_;
    entity_block_idx_ = nullptr;
  }

  for (auto& mtx : entity_mutexes_) {
    delete mtx;
  }
  if (sub_entity_group_mutex_ != nullptr) {
    delete sub_entity_group_mutex_;
    sub_entity_group_mutex_ = nullptr;
  }
  if (sub_entity_group_rwlock_ != nullptr) {
    delete sub_entity_group_rwlock_;
    sub_entity_group_rwlock_ = nullptr;
  }
}

int TsSubEntityGroup::rdLock() {
  return RW_LATCH_S_LOCK(sub_entity_group_rwlock_);
}

int TsSubEntityGroup::wrLock() {
  return RW_LATCH_X_LOCK(sub_entity_group_rwlock_);
}

int TsSubEntityGroup::unLock() {
  return RW_LATCH_UNLOCK(sub_entity_group_rwlock_);
}

int TsSubEntityGroup::ReOpenInit(ErrorInfo& err_info) {
  if (entity_block_idx_ != nullptr) {
    delete entity_block_idx_;
    entity_block_idx_ = nullptr;
  }
  partitions_ts_.clear();
  return OpenInit(subgroup_id_, db_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, err_info);
}

int TsSubEntityGroup::OpenInit(SubGroupID subgroup_id, const std::string& db_path, const string& tbl_sub_path,
                               int flags, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (entity_block_idx_ != nullptr) {
    // err_info.setError(BOEPERM, tbl_sub_path + " already opened.");
    LOG_WARN("SubEntityGroup[%s] already opened.", tbl_sub_path.c_str());
    return 0;
  }
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  real_path_ = db_path_ + tbl_sub_path_;

  struct stat buffer;
  if (stat(real_path_.c_str(), &buffer) != 0) {
    if (flags & O_CREAT) {
      // If it is CREATE but the absolute path does not exist, create it
      if (!MakeDirectory(real_path_, err_info)) {
        return err_info.errcode;
      }
    } else {
      err_info.setError(KWENOOBJ, real_path_);
      return err_info.errcode;
    }
  }
  subgroup_id_ = subgroup_id;

  entity_block_idx_ = new MMapEntityIdx(true, true);
  string meta_path = table_name_ + ".et";
  int ret = entity_block_idx_->init(meta_path, db_path_, tbl_sub_path_, flags, false, 0);
  if (ret < 0) {
    err_info.setError(ret, tbl_sub_path);
    return ret;
  }
  // Load all partition directories
  DIR* dir_ptr = opendir((db_path_ + "/" + tbl_sub_path_).c_str());
  if (dir_ptr) {
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (entry->d_type != DT_DIR) {
        continue;
      }
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
      }
      // Check if there is a directory with partition time
      std::string partition_dir = entry->d_name;
      int64_t p_ts = convertToTimestamp(partition_dir);
      if (strcmp(partition_dir.c_str(), std::to_string(p_ts).c_str()) == 0) {
        // Load the partition table and obtain the minimum and maximum timestamps for the partition
        TsTimePartition* p_table = getPartitionTable(p_ts, p_ts, err_info, false);
        if (err_info.errcode < 0) {
          delete p_table;
          break;
        }
        if (p_table->DeleteFlag()) {
          deleted_partitions_[p_table->minTimestamp()] = p_table->maxTimestamp();
          if (RemovePartitionTable(p_ts, err_info, true) < 0) {
            LOG_WARN("Remove expired partition failed, path:%s", (db_path_ + tbl_sub_path_ + partition_dir).c_str());
          }
        }
        ReleaseTable(p_table);
      }
    }
    closedir(dir_ptr);
  }

  if (err_info.errcode < 0) {
    partitions_ts_.clear();
  }

  for (int i = 0; i <= entity_block_idx_->GetConfigSubgroupEntities(); i++) {
    entity_mutexes_.push_back(std::move(new TsSubEntityGroupEntityLatch(LATCH_ID_TSSUBENTITY_GROUP_ENTITYS_MUTEX)));
  }

  return err_info.errcode;
}

EntityID TsSubEntityGroup::AllocateEntityID(const string& primary_tag, ErrorInfo& err_info) {
  assert(entity_block_idx_ != nullptr);
  return entity_block_idx_->addEntity();
}

std::vector<uint32_t> TsSubEntityGroup::GetEntities() {
  assert(entity_block_idx_ != nullptr);
  return entity_block_idx_->getEntities();
}

int TsSubEntityGroup::SetEntityNum(uint32_t entity_num) {
  return -1;
}

timestamp64 TsSubEntityGroup::PartitionTime(timestamp64 ts, timestamp64& max_ts) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  int64_t interval = root_tbl_->partitionInterval();
  if (!partitions_ts_.empty()) {
    auto it = --partitions_ts_.end();
    if (ts >= it->first && ts <= it->second) {  // Latest partition
      max_ts = it->second;
      return it->first;
    } else if (ts > it->second) {  // Create a new partition
      timestamp64 min_ts;
      partitionTime(ts, it->second + 1, interval, min_ts, max_ts);
      return min_ts;
    }
  } else {  // Create a new partition
    timestamp64 min_ts;
    partitionTime(ts, 0, interval, min_ts, max_ts);
    return min_ts;
  }
  // Found the first partition larger than ts
  auto it = partitions_ts_.upper_bound(ts);
  int64_t begin_ts = it->first;
  int64_t min_start_ts = 0;
  if (it != partitions_ts_.begin()) {
    --it;
    if (it->second >= ts) {
      max_ts = it->second;
      return it->first;
    } else {
      min_start_ts = it->second + 1;
    }
  }
  // The previous partition does not contain ts,
  // create a new partition and ensure that the min_ts of the partition is not less than the max_ts+1
  // of the previous partition
  timestamp64 min_ts;
  partitionTime(ts, begin_ts, interval, min_ts, max_ts);
  if (min_start_ts != 0 && min_ts < min_start_ts) {
    min_ts = min_start_ts;
  }
  return min_ts;
}

timestamp64 TsSubEntityGroup::MinPartitionTime() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.size() > 0) {
    return partitions_ts_.begin()->first;
  }
  return 0;
}

timestamp64 TsSubEntityGroup::MaxPartitionTime() {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.size() > 0) {
    return (--partitions_ts_.end())->first;
  }
  return 0;
}

TsTimePartition* TsSubEntityGroup::GetPartitionTable(timestamp64 ts, ErrorInfo& err_info,
                                                     bool create_if_not_exist, bool lru_push_back) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  TsTimePartition* mt_table = partition_cache_.Get(p_time);
  if (mt_table != nullptr) {
    return mt_table;
  }

  if (deleted_partitions_.find(p_time) != deleted_partitions_.end()) {
    LOG_WARN("Partition is deleted.");
    err_info.setError(KWENOOBJ, "No such partition:" + db_path_ + partitionTblSubPath(ts));
    return nullptr;
  }

  wrLock();
  Defer defer{[&]() { unLock(); }};
  // After locking, call the internal method to obtain the table instance
  err_info.clear();
  mt_table = getPartitionTable(p_time, max_ts, err_info, create_if_not_exist, lru_push_back);
  return mt_table;
}

vector<TsTimePartition*> TsSubEntityGroup::GetPartitionTables(const KwTsSpan& ts_span, ErrorInfo& err_info) {
  std::vector<int64_t> p_times;
  {
    // First, traverse the partitions that meet the criteria: the start and end ranges of the partitions are within ts_span
    timestamp64 max_ts;
    timestamp64 pts_begin = PartitionTime(ts_span.begin, max_ts);
    timestamp64 pts_end = PartitionTime(ts_span.end, max_ts);
    rdLock();
    Defer defer{[&]() { unLock(); }};
    for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
      if (!(it->first > pts_end || it->second <= pts_begin)) {
        p_times.emplace_back(it->first);
      }
    }
  }
  vector<TsTimePartition*> results;
  for (int i = 0 ; i < p_times.size() ; i++) {
    wrLock();
    Defer defer{[&]() { unLock(); }};
    TsTimePartition* p_table = getPartitionTable(p_times[i], p_times[i], err_info, false, false);
    if (!err_info.isOK() && err_info.errcode != KWENOOBJ) {
      break;
    }
    results.emplace_back(p_table);
    err_info.clear();
  }

  if (!err_info.isOK()) {
    // If there is an error in obtaining the partition table, release the already successful partition table instance
    for (int i = 0 ; i < results.size() ; i++) {
      ReleaseTable(results[i]);
    }
    results.clear();
  }
  return std::move(results);
}

TsTimePartition* TsSubEntityGroup::getPartitionTable(timestamp64 p_time, timestamp64 max_ts, ErrorInfo& err_info,
                                                     bool create_if_not_exist, bool lru_push_back) {
  TsTimePartition* mt_table = partition_cache_.Get(p_time);
  if (mt_table != nullptr) {
    return mt_table;
  }
  string pt_tbl_sub_path = partitionTblSubPath(p_time);
  mt_table = new TsTimePartition(root_tbl_, entity_block_idx_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0 && create_if_not_exist) {
    err_info.clear();
    delete mt_table;
    mt_table = createPartitionTable(pt_tbl_sub_path, p_time, max_ts, err_info);
  }
  if (err_info.errcode < 0) {
    delete mt_table;
    return nullptr;
  }

  // Add cache after adding references to prevent being eliminated by cache
  mt_table->incRefCount();
  partitions_ts_[mt_table->minTimestamp()] = mt_table->maxTimestamp();
  partition_cache_.Put(p_time, mt_table, lru_push_back);

  return mt_table;
}

TsTimePartition* TsSubEntityGroup::CreatePartitionTable(timestamp64 ts, ErrorInfo& err_info) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  string pt_tbl_sub_path = partitionTblSubPath(p_time);

  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (partitions_ts_.find(p_time) != partitions_ts_.end()) {
    err_info.setError(KWEEXIST, pt_tbl_sub_path);
    return nullptr;
  }
  TsTimePartition* mt_table = createPartitionTable(pt_tbl_sub_path, p_time, max_ts, err_info);
  if (mt_table != nullptr) {
    mt_table->incRefCount();
    partitions_ts_[mt_table->minTimestamp()] = mt_table->maxTimestamp();
    partition_cache_.Put(p_time, mt_table);
  }
  return mt_table;
}

TsTimePartition* TsSubEntityGroup::createPartitionTable(string& pt_tbl_sub_path, timestamp64 p_time, timestamp64 max_ts,
                                                        ErrorInfo& err_info) {
  // Create partition directory
  if (!MakeDirectory(db_path_ + pt_tbl_sub_path, err_info)) {
    return nullptr;
  }
  vector<string> key{};

  TsTimePartition* mt_table = new TsTimePartition(root_tbl_, entity_block_idx_->GetConfigSubgroupEntities());
  mt_table->open(table_name_ + ".bt", db_path_, pt_tbl_sub_path, MMAP_CREAT_EXCL, err_info);
  if (!err_info.isOK()) {
    delete mt_table;
    return nullptr;
  }

  mt_table->minTimestamp() = p_time;
  mt_table->maxTimestamp() = max_ts;
  return mt_table;
}

int TsSubEntityGroup::RemovePartitionTable(timestamp64 ts, ErrorInfo& err_info, bool skip_busy) {
  timestamp64 max_ts;
  timestamp64 p_time = PartitionTime(ts, max_ts);
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (!skip_busy && partitions_ts_.find(p_time) == partitions_ts_.end()) {
    // If the skip_busy is set to true, when the p_time is deleted for the first time,
    // if it is skipped, the p_time will be deleted in the partitions_ts_,
    // and the next time step to here, if the skip_busy is not judged, will return directly.
    LOG_WARN("Partition[%ld] not exist.", p_time);
    return 0;
  }
  TsTimePartition* mt_table = getPartitionTable(p_time, max_ts, err_info, false);
  if (mt_table == nullptr) {
    return err_info.errcode;
  }
  // set partition table's deleted flag, if remove failed this time, the iterator will not query the table
  // and will try to delete it again when the next lifecycle is scheduled
  mt_table->DeleteFlag() = true;
  partitions_ts_.erase(p_time);
  if (removePartitionTable(mt_table, false, err_info, skip_busy) >= 0) {
    // remove refcount of cache
    partition_cache_.Erase(p_time, false);
  } else {
    // If removePartitionTable fails, we need to release mt_table obtained from getPartitionTable.
    ReleaseTable(mt_table);
  }
  return err_info.errcode;
}

int TsSubEntityGroup::RemoveExpiredPartition(int64_t end_ts, ErrorInfo& err_info) {
  {
    rdLock();
    Defer defer{[&]() { unLock(); }};
    for (auto& pts : partitions_ts_) {
      // If other threads are using the partition, the deletion operation will be delayed
      // until the next scheduling. In order to delete the delayed partitions, we need to
      // delete all the data up to the end_ts. So, put all the partitions
      // which end timestamp older than end_ts into p_times
      if (pts.second < end_ts) {
        deleted_partitions_[pts.first] = pts.second;
      }
    }
  }
  err_info.clear();
  // delete all partitions in p_times.
  for (auto p_time = deleted_partitions_.begin(); p_time != deleted_partitions_.end();) {
    string pt_tbl_sub_path = partitionTblSubPath(p_time->first);
    LOG_INFO("RemoveExpiredPartitionTable[%s] start", pt_tbl_sub_path.c_str());
    int ret = RemovePartitionTable(p_time->first, err_info, true);
    if (ret < 0) {
      if (ret != KWERSRCBUSY) {
        LOG_ERROR("RemoveExpiredPartitionTable[%s] failed", pt_tbl_sub_path.c_str());
        break;
      }
      // If error_code = KWERSRCBUSY, don't break, skip and remove the partition next time.
      // Clear err_info, avoid error reporting due to KWERSRCBUSY returned.
      err_info.clear();
      LOG_WARN("RemoveExpiredPartitionTable[%s] skipped", pt_tbl_sub_path.c_str());
      continue;
    }
    deleted_partitions_.erase(p_time++);
    LOG_INFO("RemoveExpiredPartitionTable[%s] succeeded", pt_tbl_sub_path.c_str());
  }
  return err_info.errcode;
}

int TsSubEntityGroup::removePartitionTable(TsTimePartition* mt_table, bool is_force, ErrorInfo& err_info,
                                           bool skip_busy) {
  mt_table->setObjectStatus(OBJ_READY_TO_CHANGE);
  string pt_path = db_path_ + partitionTblSubPath(mt_table->minTimestamp());
  if (!is_force) {
    MUTEX_LOCK(mt_table->m_ref_cnt_mtx_);
    // check if there are any other refcount
    while (mt_table->isUsedWithCache()) {
      if (skip_busy) {
        // skip_busy is true, we can defer deletion
        LOG_WARN("Partition is in use, defer deletion until the next scheduling. partition:%s", pt_path.c_str());
        // error_code < 0, partition_ts_ and partition_cache_ will not erase this partition,
        // but partition's delete flag is set to true, so will not query this partition by iterator later.
        err_info.setError(KWERSRCBUSY, "Partition is in use");
        MUTEX_UNLOCK(mt_table->m_ref_cnt_mtx_);
        return err_info.errcode;
      }
      // skip_busy is false, need to wait until partition not be used
      KW_COND_WAIT(mt_table->m_ref_cnt_cv_, mt_table->m_ref_cnt_mtx_);
    }
    MUTEX_UNLOCK(mt_table->m_ref_cnt_mtx_);
  }

  // unmap and remove segments
  err_info.errcode = mt_table->remove();
  if (err_info.errcode >= 0) {
    // remove partition directory
    Remove(pt_path, err_info);
  }
  delete mt_table;
  mt_table = nullptr;
  return err_info.errcode;
}

map<int64_t, int64_t> TsSubEntityGroup::allPartitions() {
  map<int64_t, int64_t> parts;
  rdLock();
  Defer defer{[&]() { unLock(); }};
  // Load all partition tables
  // std::vector<MMapMetricsTable*> tables;
  for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
    parts[it->first] = it->second;
  }
  return std::move(parts);
}

int TsSubEntityGroup::RemoveAll(bool is_force, ErrorInfo& err_info) {
  int error_code = 0;
  wrLock();
  Defer defer{[&]() { unLock(); }};
  // Load all partition tables
  std::vector<TsTimePartition*> tables;
  for (auto it = partitions_ts_.begin() ; it != partitions_ts_.end() ; it++) {
    err_info.clear();
    TsTimePartition* mt_table = getPartitionTable(it->first, it->second, err_info, false);
    if (err_info.errcode == KWENOOBJ) {  // Ignoring non-existent partition tables (deleted or file corrupted)
      err_info.clear();
      continue;
    }
    if (err_info.errcode < 0) {
      break;
    }
    tables.emplace_back(mt_table);
  }

  if (err_info.errcode >= 0) {
    // Release all partition tables in cache
    partition_cache_.Clear();
    for (int i = 0 ; i < tables.size() ; i++) {
      if (removePartitionTable(tables[i], is_force, err_info) < 0) {
        break;
      }
      tables[i] = nullptr;
    }
    // Delete metadata and subgroup directory
    err_info.errcode = entity_block_idx_->remove();
    delete entity_block_idx_;
    entity_block_idx_ = nullptr;
    partitions_ts_.clear();

    err_info.errcode = fs::remove_all(real_path_.c_str());
    if (err_info.errcode < 0) {
      LOG_ERROR("remove[%s] error : %s", real_path_.c_str(), strerror(errno));
    }
  }

  // Release the obtained table
  for (int i = 0 ; i < tables.size() ; i++) {
    if (tables[i] != nullptr) ReleaseTable(tables[i]);
  }

  return err_info.errcode;
}

void TsSubEntityGroup::PartitionCacheEvict() {
  partition_cache_.Clear(0);
}

int TsSubEntityGroup::DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  // Firstly, delete the datablock of Entity in each partition table
  map<int64_t, int64_t> partitions = allPartitions();
  for (auto it = partitions.begin() ; it != partitions.end() ; it++) {
    uint64_t num = 0;
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      p_table->DeleteEntity(entity_id, lsn, &num, err_info);
    }
    ReleaseTable(p_table);
    if (err_info.errcode < 0) {
      break;
    }
    *count = *count + num;
  }
  deleteEntityItem(entity_id);

  return err_info.errcode;
}

int TsSubEntityGroup::AlterTable(AttributeInfo& attr_info, ErrorInfo& err_info) {
  map<int64_t, int64_t> partitions = allPartitions();

  for (auto it = partitions.begin() ; it != partitions.end() ; it++) {
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (err_info.errcode < 0) {
      LOG_ERROR("alter table [%s] failed: %s", table_name_.c_str(), err_info.errmsg.c_str());
      break;
    }
    if (p_table != nullptr) {
      p_table->AlterTable(attr_info, err_info);
      ReleaseTable(p_table);
      if (err_info.errcode < 0) {
        LOG_ERROR("alter table [%s] failed: %s", table_name_.c_str(), err_info.errmsg.c_str());
        break;
      }
    }
  }
  return err_info.errcode;
}

int TsSubEntityGroup::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  // Firstly, undo the deleted Entity in each partition table
  map<int64_t, int64_t> partitions = allPartitions();
  for (auto it = partitions.begin(); it != partitions.end(); it++) {
    uint64_t num = 0;
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      p_table->UndoDeleteEntity(entity_id, lsn, &num, err_info);
    }
    ReleaseTable(p_table);
    if (err_info.errcode < 0) {
      break;
    }
    *count = *count + num;
  }
  EntityItem* entity_item = entity_block_idx_->getEntityItem(entity_id);
  entity_item->is_deleted = false;

  return err_info.errcode;
}

void TsSubEntityGroup::sync(int flags) {
  map<int64_t, int64_t> partitions = allPartitions();
  ErrorInfo err_info;
  for (auto it = partitions.begin() ; it != partitions.end() ; it++) {
    TsTimePartition* p_table = GetPartitionTable(it->first, err_info, false);
    if (p_table != nullptr) {
      p_table->sync(flags);
    }
    ReleaseTable(p_table);
  }
}

inline void TsSubEntityGroup::partitionTime(timestamp64 target_ts, timestamp64 begin_ts, timestamp64 interval,
                                            timestamp64& min_ts, timestamp64& max_ts) {
  // begin_ts = 0 or other partition min_ts
  if (target_ts >= begin_ts) {
    // Starting from begin_ts with a time interval of interval, retrieve the partition time of cur_ts backwards
    min_ts = (target_ts - begin_ts) / interval * interval + begin_ts;
    max_ts = min_ts + interval - 1;
  } else {
    // Starting from begin_ts with a time interval of interval, retrieve the partition time where cur_ts is located forward
    timestamp64 offset = (begin_ts - target_ts + (interval-1)) / interval * interval;
    if (begin_ts >= INT64_MIN + offset) {
      min_ts = begin_ts - offset;
      max_ts = min_ts + interval - 1;
    } else {
      min_ts = INT64_MIN;
      max_ts = begin_ts - (begin_ts - target_ts) / interval * interval - 1;
    }
  }
}

}  // namespace kwdbts
