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
//

#include "mmap/mmap_root_table_manager.h"
#include "mmap/mmap_file.h"
#include "dirent.h"
#include "sys_utils.h"

inline string IdToEntityBigTableUrl(const KTableKey& table_id, uint32_t table_version) {
  return nameToEntityBigTableURL(std::to_string(table_id), s_bt + "_" + std::to_string(table_version));
}

MMapMetricsTable* MMapRootTableManager::openRootTable(uint32_t table_version, ErrorInfo& err_info) {
  auto* tmp_bt = new MMapMetricsTable();
  string bt_url = IdToEntityBigTableUrl(table_id_, table_version);
  tmp_bt->open(bt_url, db_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    delete tmp_bt;
    tmp_bt = nullptr;
    LOG_ERROR("root table[%s] open failed: %s", bt_url.c_str(), err_info.errmsg.c_str())
  }
  return tmp_bt;
}

impl_latch_virtual_func(MMapRootTableManager, &rw_latch_)

MMapRootTableManager::~MMapRootTableManager(){
  wrLock();
  Defer defer([&]() { unLock(); });
  for (auto& root_table : root_tables_) {
    if (root_table.second) {
      delete root_table.second;
      root_table.second = nullptr;
    }
  }
  root_tables_.clear();
}

KStatus MMapRootTableManager::Init(ErrorInfo& err_info) {
  uint32_t max_table_version = 0;
  string real_path = db_path_ + tbl_sub_path_;
  // Load all versions of root table
  DIR* dir_ptr = opendir(real_path.c_str());
  if (dir_ptr) {
    string prefix = std::to_string(table_id_) + s_bt + '_';
    size_t prefix_len = prefix.length();
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
      }
      if (entry->d_type == DT_REG &&
          strncmp(entry->d_name, prefix.c_str(), prefix_len) == 0) {
        uint32_t table_version = std::stoi(entry->d_name + prefix_len);
        // By default, it is not enabled
        root_tables_.insert({table_version, nullptr});
        if (table_version > max_table_version) {
          max_table_version = table_version;
        }
      }
    }
    closedir(dir_ptr);
  }
  // Open only the latest version of table
  auto* tmp_bt = new MMapMetricsTable();
  string bt_url = IdToEntityBigTableUrl(table_id_, max_table_version);
  tmp_bt->open(bt_url, db_path_, tbl_sub_path_, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] open error : %s", bt_url.c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    tmp_bt = nullptr;
    return FAIL;
  }
  // Save to map cache
  PutTable(max_table_version, tmp_bt);
  // Load the latest time partition interval
  partition_interval_ = tmp_bt->partitionInterval();
  return SUCCESS;
}

KStatus MMapRootTableManager::CreateRootTable(vector<AttributeInfo>& schema, uint32_t table_version,
                                              ErrorInfo& err_info, uint32_t cur_version) {
  assert(table_version > 0);
  wrLock();
  Defer defer([&]() { unLock(); });
  if (cur_table_version_ >= table_version) {
    LOG_ERROR("cannot create low version table: current version[%d], create version[%d]",
              cur_table_version_, table_version)
    return FAIL;
  }
  for (auto& attr: schema) {
    attr.version = table_version;
  }
  // Create a new version of root table
  string bt_url = IdToEntityBigTableUrl(table_id_, table_version);
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  auto* tmp_bt = new MMapMetricsTable();
  if (tmp_bt->open(bt_url, db_path_, tbl_sub_path_, MMAP_CREAT_EXCL, err_info) >= 0 || err_info.errcode == KWECORR) {
    tmp_bt->create(schema, table_version, tbl_sub_path_, partition_interval_, encoding, err_info, false);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("root table[%s] create error : %s", bt_url.c_str(), err_info.errmsg.c_str());
    tmp_bt->remove();
    delete tmp_bt;
    tmp_bt = nullptr;
    return FAIL;
  }

  // Copy the metadata of the previous version
  if (cur_version) {
    auto src_bt = root_tables_[cur_version];
    tmp_bt->metaData()->has_data = src_bt->metaData()->has_data;
    tmp_bt->metaData()->actul_size = src_bt->metaData()->actul_size;
    // tmp_bt->metaData()->life_time = src_bt->metaData()->life_time;
    tmp_bt->metaData()->partition_interval = src_bt->metaData()->partition_interval;
    tmp_bt->metaData()->num_node = src_bt->metaData()->num_node;
    tmp_bt->metaData()->is_dropped = src_bt->metaData()->is_dropped;
    tmp_bt->metaData()->min_ts = src_bt->metaData()->min_ts;
    tmp_bt->metaData()->max_ts = src_bt->metaData()->max_ts;
    // Version compatibility
    if (src_bt->metaData()->schema_version_of_latest_data == 0) {
      tmp_bt->metaData()->schema_version_of_latest_data = table_version;
    } else {
      tmp_bt->metaData()->schema_version_of_latest_data = src_bt->metaData()->schema_version_of_latest_data;
    }
  } else {
    tmp_bt->metaData()->schema_version_of_latest_data = table_version;
  }
  tmp_bt->setObjectReady();
  // Save to map cache
  root_tables_.insert({table_version, tmp_bt});
  // Update the latest version of table and other information
  cur_root_table_ = tmp_bt;
  cur_table_version_ = table_version;
  partition_interval_ = tmp_bt->partitionInterval();
  return SUCCESS;
}

KStatus MMapRootTableManager::PutTable(uint32_t table_version, MMapMetricsTable *table) {
  wrLock();
  Defer defer([&]() { unLock(); });
  auto bt_it = root_tables_.find(table_version);
  if (bt_it != root_tables_.end()) {
    delete bt_it->second;
    root_tables_.erase(bt_it);
  }
  root_tables_.insert({table_version, table});
  if (cur_table_version_ < table_version) {
    cur_root_table_ = table;
    cur_table_version_ = table_version;
    partition_interval_ = table->partitionInterval();
  }
  return KStatus::SUCCESS;
}

MMapMetricsTable* MMapRootTableManager::GetRootTable(uint32_t table_version, bool lock) {
  bool need_open = false;
  // Try to get the root table using a read lock
  {
    if (lock) {
      rdLock();
    }
    Defer defer([&]() { if (lock) { unLock(); }});
    if (table_version == 0 || table_version == cur_table_version_) {
      return cur_root_table_;
    }
    auto bt_it = root_tables_.find(table_version);
    if (bt_it != root_tables_.end()) {
      if (!bt_it->second) {
        need_open = true;
      } else {
        return bt_it->second;
      }
    }
  }
  if (!need_open) {
    return nullptr;
  }
  // Open the root table using a write lock
  if (lock) {
    wrLock();
  }
  Defer defer([&]() { if (lock) { unLock(); }});
  auto bt_it = root_tables_.find(table_version);
  if (bt_it != root_tables_.end()) {
    if (!bt_it->second) {
      ErrorInfo err_info;
      bt_it->second = openRootTable(bt_it->first, err_info);
    }
    return bt_it->second;
  }
  return nullptr;
}

uint32_t MMapRootTableManager::GetCurrentTableVersion() const {
  return cur_table_version_;
}

string MMapRootTableManager::GetTableName() {
  return name_;
}

uint64_t MMapRootTableManager::GetPartitionInterval() {
  return partition_interval_;
}

KStatus MMapRootTableManager::SetPartitionInterval(const uint64_t& partition_interval) {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Record the old partition interval for rollback processing
  uint64_t old_partition_interval = cur_root_table_->partitionInterval();
  std::vector<MMapMetricsTable*> completed_tables;
  // Iterate through all versions of the root table, updating the partition interval
  for (auto& root_table : root_tables_) {
    if (!root_table.second) {
      ErrorInfo err_info;
      root_table.second = openRootTable(root_table.first, err_info);
      if (!root_table.second) {
        LOG_ERROR("root table[%s] set partition interval failed",
                  IdToEntityBigTableUrl(table_id_, root_table.first).c_str());
        // rollback
        for (auto completedTable : completed_tables) {
          completedTable->partitionInterval() = old_partition_interval;
        }
        return KStatus::FAIL;
      }
    }
    root_table.second->partitionInterval() = partition_interval;
    completed_tables.push_back(root_table.second);
  }
  // Update the partition interval
  partition_interval_ = partition_interval;
  return KStatus::SUCCESS;
}

KStatus MMapRootTableManager::GetStatisticInfo(uint64_t& entity_num, uint64_t& insert_rows_per_day) {
  rdLock();
  Defer defer([&]() { unLock(); });
  entity_num = cur_root_table_->metaData()->entity_num;
  insert_rows_per_day = cur_root_table_->metaData()->insert_rows_per_day;
  return KStatus::SUCCESS;
}

KStatus MMapRootTableManager::SetStatisticInfo(uint64_t entity_num, uint64_t insert_rows_per_day) {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Record the old value for rollback processing
  uint64_t old_entity_num = cur_root_table_->metaData()->entity_num;
  uint64_t old_insert_rows_per_day = cur_root_table_->metaData()->insert_rows_per_day;
  std::vector<MMapMetricsTable*> completed_tables;
  // Iterate through all versions of the root table, updating the storage info
  for (auto& root_table : root_tables_) {
    if (!root_table.second) {
      ErrorInfo err_info;
      root_table.second = openRootTable(root_table.first, err_info);
      if (!root_table.second) {
        LOG_ERROR("root table[%s] set storage info failed",
                  IdToEntityBigTableUrl(table_id_, root_table.first).c_str());
        // rollback
        for (auto completedTable : completed_tables) {
          completedTable->metaData()->entity_num = old_entity_num;
          completedTable->metaData()->insert_rows_per_day = old_insert_rows_per_day;
        }
        return KStatus::FAIL;
      }
    }
    root_table.second->metaData()->entity_num = entity_num;
    root_table.second->metaData()->insert_rows_per_day = insert_rows_per_day;
    completed_tables.push_back(root_table.second);
  }
  return KStatus::SUCCESS;
}

const vector<AttributeInfo>& MMapRootTableManager::GetSchemaInfoWithoutHidden(uint32_t table_version) {
  MMapMetricsTable* root_table = GetRootTable(table_version);
  assert(root_table != nullptr);
  return root_table->getSchemaInfoWithoutHidden();
}

const vector<AttributeInfo>& MMapRootTableManager::GetSchemaInfoWithHidden(uint32_t table_version) {
  MMapMetricsTable* root_table = GetRootTable(table_version);
  assert(root_table != nullptr);
  return root_table->getSchemaInfoWithHidden();
}

const vector<uint32_t>& MMapRootTableManager::GetColsIdx(uint32_t table_version) {
  MMapMetricsTable* root_table = GetRootTable(table_version);
  assert(root_table != nullptr);
  return root_table->getColsIdx();
}

int MMapRootTableManager::GetColumnIndex(const AttributeInfo& attr_info) {
  int col_no = -1;
  auto schema_info = GetSchemaInfoWithHidden();
  for (int i = 0; i < schema_info.size(); ++i) {
    if ((schema_info[i].id == attr_info.id) && (!schema_info[i].isFlag(AINFO_DROPPED))) {
      col_no = i;
      break;
    }
  }
  return col_no;
}

int MMapRootTableManager::Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info) {
  wrLock();
  Defer defer([&]() { unLock(); });
  for (auto& root_table : root_tables_) {
    if (root_table.second) {
      root_table.second->Sync(check_lsn, err_info);
    }
  }
  return 0;
}

KStatus MMapRootTableManager::SetDropped() {
  wrLock();
  Defer defer([&]() { unLock(); });
  std::vector<MMapMetricsTable*> completed_tables;
  // Iterate through all versions of the root table, updating the drop flag
  for (auto& root_table : root_tables_) {
    if (!root_table.second) {
      ErrorInfo err_info;
      root_table.second = openRootTable(root_table.first, err_info);
      if (!root_table.second) {
        LOG_ERROR("root table[%s] set drop failed", IdToEntityBigTableUrl(table_id_, root_table.first).c_str());
        // rollback
        for (auto completed_table : completed_tables) {
          completed_table->setNotDropped();
        }
        return KStatus::FAIL;
      }
    }
    root_table.second->setDropped();
    completed_tables.push_back(root_table.second);
  }
  return KStatus::SUCCESS;
}

bool MMapRootTableManager::IsDropped() {
  rdLock();
  Defer defer([&]() { unLock(); });
  return cur_root_table_->isDropped();
}

KStatus MMapRootTableManager::RemoveAll() {
  wrLock();
  Defer defer([&]() { unLock(); });
  // Remove all root tables
  for (auto& root_table : root_tables_) {
    if (!root_table.second) {
      Remove(db_path_ + IdToEntityBigTableUrl(table_id_, root_table.first));
    } else {
      root_table.second->remove();
      delete root_table.second;
      root_table.second = nullptr;
    }
  }
  root_tables_.clear();
  return SUCCESS;
}

KStatus MMapRootTableManager::RollBack(uint32_t old_version, uint32_t new_version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  if (cur_table_version_ == old_version) {
    return SUCCESS;
  } else if (cur_table_version_ == new_version) {
    // Get the previous version of root table
    auto bt = GetRootTable(old_version, false);
    // Clear the current version of the data
    root_tables_.erase(new_version);
    cur_root_table_->remove();
    delete cur_root_table_;
    // Update the latest version information
    cur_root_table_ = bt;
    cur_table_version_ = old_version;
    partition_interval_ = bt->partitionInterval();
  } else {
    LOG_ERROR("the rollback version number(%u) is smaller than current table version(%u)", new_version, cur_table_version_);
    return FAIL;
  }
  return SUCCESS;
}

KStatus MMapRootTableManager::UpdateVersion(uint32_t cur_version, uint32_t new_version) {
  auto schema = GetSchemaInfoWithHidden(cur_version);
  ErrorInfo err_info;
  // Create a new version of the root table based on the resulting schema
  auto s = CreateRootTable(schema, new_version, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("UpdateVersion failed: table id = %u, new_version = %u", table_id_, new_version);
    return s;
  }
  return SUCCESS;
}

KStatus MMapRootTableManager::UpdateTableVersionOfLastData(uint32_t version) {
  wrLock();
  Defer defer([&]() { unLock(); });
  GetRootTable(cur_table_version_, false)->tableVersionOfLatestData() = version;
  return SUCCESS;
}

uint32_t MMapRootTableManager::GetTableVersionOfLatestData() {
  rdLock();
  Defer defer([&]() { unLock(); });
  uint32_t version = GetRootTable(cur_table_version_, false)->tableVersionOfLatestData();
  // Version compatibility
  return version == 0 ? 1 : version;
}
