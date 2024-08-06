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

#include <dirent.h>
#include <iostream>
#include "lg_api.h"
#include "mmap/MMapPartitionTable.h"
#include "st_group_manager.h"
#include "BigObjectUtils.h"
#include "utils/ObjectUtils.h"
#include "perf_stat.h"
#include "sys_utils.h"

namespace kwdbts {

SubEntityGroupManager::~SubEntityGroupManager() {
  wrLock();
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; it++) {
    delete it->second;
  }
  subgroups_.clear();
  unLock();
  delete subgroup_mgr_mutex_;
  subgroup_mgr_mutex_ = nullptr;
  delete subgroup_mgr_rwlock_;
  subgroup_mgr_rwlock_ = nullptr;
}

int SubEntityGroupManager::OpenInit(const std::string& db_path, const std::string& tbl_sub_path,
                                    uint64_t table_id, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (subgroups_.size() > 0) {
    err_info.setError(BOEPERM, tbl_sub_path + " already opened.");
    return err_info.errcode;
  }
  db_path_ = db_path;
  table_id_ = table_id;
  // root_table->incRefCount();

  if (db_path_.back() != '/') {
    db_path_ = db_path_ + "/";
  }
  tbl_sub_path_ = normalizePath(tbl_sub_path);

  // max_sub_group_id
  DIR* dir_ptr = opendir((db_path_ + tbl_sub_path_).c_str());
  if (dir_ptr) {
    struct dirent* entity;
    while ((entity = readdir(dir_ptr)) != nullptr) {
      if (entity->d_type == DT_DIR) {
        if (strcmp(entity->d_name, ".") == 0 || strcmp(entity->d_name, "..") == 0
            || entity->d_name[0] == '_' || strcmp(entity->d_name, "wal") == 0) {
          continue;
        }
        string dir_name = entity->d_name;
        size_t idx = dir_name.find_last_of('_');
        if (idx == string::npos) {
          continue;
        }
        string table_id_str = dir_name.substr(0, idx);
        string sub_group_str = dir_name.substr(idx + 1);
        if (table_id_str == std::to_string(table_id)) {
          char* endptr;
          int64_t sub_group_id = std::strtol(sub_group_str.c_str(), &endptr, 10);
          if (*endptr == '\0' && max_subgroup_id_ < sub_group_id) {
            max_subgroup_id_ = sub_group_id;
          }
          TsSubEntityGroup* subgroup = openSubGroup(sub_group_id, err_info, false);
          if (err_info.errcode < 0) {
            LOG_ERROR("GetSubGroup error : %s", err_info.errmsg.c_str());
            break;
          }
          subgroups_[sub_group_id] = subgroup;
        }
      }
    }
    closedir(dir_ptr);
  }

  if (err_info.errcode < 0) {
    // Clean up the loaded subgroups
    subgroups_.clear();
  }
  return err_info.errcode;
}


TsSubEntityGroup* SubEntityGroupManager::CreateSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info) {
  assert(root_table_ != nullptr);
  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto it = subgroups_.find(subgroup_id);
  if (it != subgroups_.end()) {
    err_info.setError(BOEEXIST, "SubEntityGroup : " + std::to_string(subgroup_id));
    return it->second;
  }

  root_table_->rdLock();
  TsSubEntityGroup* sub_group = new TsSubEntityGroup(root_table_);
  root_table_->unLock();
  string group_sanbox = GetSubGroupTblSubPath(subgroup_id);
  if (sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_CREAT_EXCL, err_info) < 0) {
    delete sub_group;
    return nullptr;
  }
  subgroups_[subgroup_id] = sub_group;
  return sub_group;
}

TsSubEntityGroup* SubEntityGroupManager::GetSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info,
                                                     bool create_not_exist) {
  {
    // Initially, attempt to acquire the subgroup using a read lock. If the subgroup is not found, subsequently,
    // acquire a write lock and perform another search to ensure no other threads have created it.
    // If not found yet, create a new subgroup
    rdLock();
    Defer defer{[&]() { unLock(); }};
    auto it = subgroups_.find(subgroup_id);
    if (it != subgroups_.end()) {
      return it->second;
    }
  }

  wrLock();
  Defer defer{[&]() { unLock(); }};
  auto it = subgroups_.find(subgroup_id);
  if (it != subgroups_.end()) {
    return it->second;
  }

  TsSubEntityGroup* sub_group = openSubGroup(subgroup_id, err_info, create_not_exist);
  if (err_info.errcode < 0) {
    return nullptr;
  }
  if (subgroup_id > max_subgroup_id_) {
    max_subgroup_id_ = subgroup_id;
  }
  subgroups_[subgroup_id] = sub_group;
  return sub_group;
}

TsSubEntityGroup* SubEntityGroupManager::openSubGroup(SubGroupID subgroup_id, ErrorInfo& err_info,
                                                      bool create_not_exist) {
  root_table_->rdLock();
  TsSubEntityGroup* sub_group = new TsSubEntityGroup(root_table_);
  root_table_->unLock();
  string group_sanbox = GetSubGroupTblSubPath(subgroup_id);
  if (sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    delete sub_group;
    sub_group = nullptr;
  }
  if (err_info.errcode < 0 && create_not_exist) {
    err_info.clear();
    root_table_->rdLock();
    sub_group = new TsSubEntityGroup(root_table_);
    sub_group->OpenInit(subgroup_id, db_path_, group_sanbox, MMAP_CREAT_EXCL, err_info);
    root_table_->unLock();
  }
  if (err_info.errcode < 0) {
    delete sub_group;
    sub_group = nullptr;
  }
  return sub_group;
}

MMapPartitionTable* SubEntityGroupManager::CreatePartitionTable(timestamp64 ts, SubGroupID subgroup_id,
                                                              ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return nullptr;
  }
  return sub_group->CreatePartitionTable(ts, err_info);
}

MMapPartitionTable* SubEntityGroupManager::GetPartitionTable(timestamp64 ts, SubGroupID sub_group_id,
                                                           ErrorInfo& err_info, bool for_new) {
  KWDB_DURATION(StStatistics::Get().get_partition);
  TsSubEntityGroup* sub_group = GetSubGroup(sub_group_id, err_info, false);
  if (sub_group == nullptr) {
    return nullptr;
  }
  return sub_group->GetPartitionTable(ts, err_info, for_new);
}

vector<MMapPartitionTable*> SubEntityGroupManager::GetPartitionTables(const KwTsSpan& ts_span, SubGroupID subgroup_id,
                                                                      ErrorInfo& err_info) {
  KWDB_DURATION(StStatistics::Get().get_partitions);
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return vector<MMapPartitionTable*> ();
  }
  return sub_group->GetPartitionTables(ts_span, err_info);
}

void SubEntityGroupManager::ReleasePartitionTable(MMapPartitionTable* e_bt, bool is_force) {
  ReleaseTable(e_bt);
}

int SubEntityGroupManager::DropSubGroup(SubGroupID subgroup_id, bool if_exist,
                                        ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    return BOENOOBJ;
  }
  err_info.clear();
  wrLock();
  Defer defer{[&]() { unLock(); }};
  if (sub_group->RemoveAll(false, err_info) >= 0) {
    subgroups_.erase(subgroup_id);
  }
  return err_info.errcode;
}

int SubEntityGroupManager::DropPartitionTable(timestamp64 p_time, SubGroupID subgroup_id, bool if_exist,
                                              ErrorInfo& err_info) {
  TsSubEntityGroup* sub_group = GetSubGroup(subgroup_id, err_info, false);
  if (sub_group == nullptr) {
    err_info.setError(BOENOOBJ, tbl_sub_path_ + std::to_string(p_time));
    return err_info.errcode;
  }
  return sub_group->RemovePartitionTable(p_time, err_info);
}

int SubEntityGroupManager::DeleteExpiredData(int64_t end_ts, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  // Traverse through all subgroups to delete expired data
  for (auto& subgroup : subgroups_) {
    if (subgroup.second->RemoveExpiredPartition(end_ts, err_info) < 0) {
      break;
    }
  }
  return err_info.errcode;
}

int SubEntityGroupManager::DropAll(bool is_force, ErrorInfo& err_info) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; ) {
    if (it->second->RemoveAll(is_force, err_info) < 0) {
      return err_info.errcode;
    }
    delete it->second;
    it = subgroups_.erase(it);
  }

  return 0;
}

void SubEntityGroupManager::Compress(const timestamp64& compress_ts, ErrorInfo& err_info) {
  std::vector<MMapPartitionTable*> compress_tables;
  // Gets all the compressible partitions in the [INT64_MIN, ts] time range under subgroup
  rdLock();
  for (auto & subgroup : subgroups_) {
    vector<MMapPartitionTable*> p_tables = subgroup.second->GetPartitionTables({INT64_MIN, compress_ts}, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("SubEntityGroupManager GetPartitionTable error : %s", err_info.errmsg.c_str());
      break;
    }
    compress_tables.insert(compress_tables.end(), p_tables.begin(), p_tables.end());
  }
  unLock();

  bool compress_error = false;
  for (auto p_table : compress_tables) {
    if (p_table != nullptr && !compress_error) {
      p_table->Compress(compress_ts, err_info);
      if (err_info.errcode < 0) {
        LOG_ERROR("MMapPartitionTable[%s] compress error : %s", p_table->URL().c_str(), err_info.errmsg.c_str());
        compress_error = true;
      }
    }
    ReleaseTable(p_table);
  }

  // Call partition table lru cache to eliminate after compression
  rdLock();
  for (auto & subgroup : subgroups_) {
    subgroup.second->PartitionCacheEvict();
  }
  unLock();
}

int SubEntityGroupManager::removeDir(string path) {
  int ret = -1;
  if (!path.empty()) {
    if (Remove(path)) {
      return 0;
    }
  }
  return ret;
}

int SubEntityGroupManager::AllocateEntity(std::string& primary_tags, uint64_t tag_hash, SubGroupID* group_id,
                                          EntityID* entity_id) {
  // Calculate the number of group and try to meet the hash id range of range
  SubGroupID find_group = max_active_subgroup_id_;
  EntityID find_entity = 0;

  ErrorInfo err_info;
  while (find_entity == 0) {
    TsSubEntityGroup* g_bt = GetSubGroup(find_group, err_info, true);
    if (err_info.errcode < 0) {
      return err_info.errcode;
    }

    if (!g_bt->IsAvailable()) {
      find_group++;
      continue;
    }
    // Call the MMapEntityBigTable method to query whether there are any free entity IDs in g_bt
    // If 0 is returned, it means that this group has no available entity
    find_entity = g_bt->AllocateEntityID(primary_tags, err_info);
    if (find_entity > 0) {
      break;
    }
    // Calculate the next group to assign entity id
    find_group++;
    max_active_subgroup_id_ = find_group;
  }

  *group_id = find_group;
  *entity_id = find_entity;
  return 0;
}

int SubEntityGroupManager::AllocateNewSubgroups(const int& count, vector<SubGroupID>* subgroups) {
  SubGroupID find_group = max_active_subgroup_id_ + 1;
  int size = count;
  wrLock();
  Defer defer{[&]() { unLock(); }};
  while (find_group && size > 0) {
    auto it = subgroups_.find(find_group);
    if (it == subgroups_.end()) {
      subgroups->push_back(find_group);
      // create new subgroup
      ErrorInfo err_info;
      TsSubEntityGroup* sub_group = openSubGroup(find_group, err_info, true);
      if (err_info.errcode < 0) {
        LOG_ERROR("AllocateNewID: Create subgroup failed, id: %u", find_group);
        return err_info.errcode;
      }
      if (find_group > max_subgroup_id_) {
        max_subgroup_id_ = find_group;
      }
      sub_group->SetUnavailable();
      subgroups_[find_group] = sub_group;
      size--;
    }
    find_group++;
  }
  return 0;
}

int SubEntityGroupManager::ReuseEntities(SubGroupID group_id, std::vector<EntityID>& entity_ids) {
  ErrorInfo err_info;
  TsSubEntityGroup* g_bt = GetSubGroup(group_id, err_info);
  if (!g_bt) {
    return err_info.errcode;
  }
  for (int i = 0 ; i < entity_ids.size() ; i++) {
    // Reuse the entity, which means marking the entity as deleted
    // g_bt->deleteEntityItem(entity_ids[i]);
  }
  return 0;
}

int SubEntityGroupManager::AlterSubGroupColumn(AttributeInfo& attr_info, ErrorInfo& err_info) {
  rdLock();
  Defer defer{[&]() { unLock(); }};
  // Traversal and alter table in every subgroup.
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; it++) {
    it->second->AlterTable(attr_info, err_info);
    if (err_info.errcode < 0) {
      break;
    }
  }

  return err_info.errcode;
}

void SubEntityGroupManager::sync(int flags) {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (auto it = subgroups_.begin() ; it != subgroups_.end() ; it++) {
    it->second->sync(flags);
  }
}

void SubEntityGroupManager::CopyMetaData(MMapMetricsTable* dst_bt, MMapMetricsTable* src_bt) {
  dst_bt->metaData()->has_data = src_bt->metaData()->has_data;
  dst_bt->metaData()->actul_size = src_bt->metaData()->actul_size;
  dst_bt->metaData()->life_time = src_bt->metaData()->life_time;
  // dst_bt->metaData()->active_time = src_bt->metaData()->active_time;
  dst_bt->metaData()->partition_interval = src_bt->metaData()->partition_interval;
  dst_bt->metaData()->num_node = src_bt->metaData()->num_node;
  dst_bt->metaData()->is_deleted = src_bt->metaData()->is_deleted;
//  dst_bt->metaData()->num_leaf_node = src_bt->metaData()->num_leaf_node;
  dst_bt->metaData()->life_cycle = src_bt->metaData()->life_cycle;
  dst_bt->metaData()->min_ts = src_bt->metaData()->min_ts;
  dst_bt->metaData()->max_ts = src_bt->metaData()->max_ts;
}

void SubEntityGroupManager::SetSubgroupAvailable() {
  wrLock();
  Defer defer{[&]() { unLock(); }};
  for (auto& subgroup : subgroups_) {
    if (!subgroup.second->IsAvailable()) {
      subgroup.second->SetAvailable();
      if (max_active_subgroup_id_ > subgroup.first) {
        max_active_subgroup_id_ = subgroup.first;  // Update after migrating the snapshot
      }
    }
  }
}

}  // namespace kwdbts
