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

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include "sys_utils.h"
#include "dirent.h"
#include "mmap/mmap_tag_table.h"
#include "ts_table.h"

TagTable::TagTable(const std::string& db_path,
                   const std::string& sub_path,
                   uint64_t table_id,
                   int32_t entity_group_id): 
                  m_db_path_(db_path), m_tbl_sub_path_(sub_path),
                  m_table_id(table_id), m_entity_group_id_(entity_group_id) {
  m_mutex_ = new KLatch(LATCH_ID_TAG_TABLE_METADATA_MUTEX);
}

TagTable::~TagTable() {
  delete m_version_mgr_;
  delete m_partition_mgr_;
  delete m_index_;
  delete m_mutex_;
  m_version_mgr_ = nullptr;
  m_partition_mgr_ = nullptr;
  m_index_ = nullptr;
  m_mutex_ = nullptr;
}

int TagTable::create(const vector<TagInfo> &schema, uint32_t table_version, ErrorInfo &err_info) {
  // 1. create table version
  m_version_mgr_ = KNEW TagTableVersionManager(m_db_path_, m_tbl_sub_path_, m_table_id);
  if (m_version_mgr_ == nullptr) {
    LOG_ERROR("KNEW TagTableVersionManager failed.");
    return -1;
  }
  auto tag_ver_obj = m_version_mgr_->CreateTagVersionObject(schema, table_version, err_info);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("call CreateTagVersionObject failed, %s ", err_info.errmsg.c_str());
    return -1;
  }
  // 2. create tag partition table
  m_partition_mgr_ = KNEW TagPartitionTableManager(m_db_path_, m_tbl_sub_path_, m_table_id, m_entity_group_id_);
  if (m_partition_mgr_ == nullptr) {
    LOG_ERROR("KNEW TagPartitionTableManager failed.");
    return -1;
  }
  if (m_partition_mgr_->CreateTagPartitionTable(schema, table_version, err_info) < 0) {
    LOG_ERROR("CreateTagPartitionTable failed, %s", err_info.errmsg.c_str());
    return -1;
  }
  // 3. create hash index
  if (initHashIndex(MMAP_CREAT_EXCL, err_info) < 0) {
    LOG_ERROR("create HashIndex failed, %s", err_info.errmsg.c_str());
    return -1;
  }
  tag_ver_obj->setStatus(TAG_STATUS_READY);
  m_version_mgr_->UpdateNewestTableVersion(table_version);
  m_version_mgr_->SyncCurrentTableVersion();
  return 0;
}

int TagTable::open(ErrorInfo &err_info) {
  if (nullptr == m_version_mgr_) {
    m_version_mgr_ = KNEW TagTableVersionManager(m_db_path_, m_tbl_sub_path_, m_table_id);
    if (m_version_mgr_ == nullptr) {
      LOG_ERROR("KNEW TagTableVersionManager failed.");
      return -1;
    }
  }

  if (nullptr == m_partition_mgr_) {
    m_partition_mgr_ = KNEW TagPartitionTableManager(m_db_path_, m_tbl_sub_path_, m_table_id, m_entity_group_id_);
    if (m_partition_mgr_ == nullptr) {
      LOG_ERROR("KNEW TagPartitionTableManager failed.");
      return -1;
    }
  }
  // 1. load all versions id
  std::vector<TableVersion> all_versions;
  if (loadAllVersions(all_versions, err_info) < 0) {
    return -1;
  }
  // 2. open all object
  for (const auto it : all_versions) {
    // open tag version object
    auto tag_ver_obj = m_version_mgr_->OpenTagVersionObject(it, err_info);
    if (nullptr == tag_ver_obj || !tag_ver_obj->isValid()) {
      // tag version is invalid
      continue;
    }
    // open partition table
    if (m_partition_mgr_->OpenTagPartitionTable(it, err_info) < 0) {
      continue;
    }
    m_version_mgr_->UpdateNewestTableVersion(it);
  }

  // 3. open hash index
  if (initHashIndex(MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    LOG_ERROR("open HashIndex failed. error: %s ", err_info.errmsg.c_str());
    return -1;
  }
  m_version_mgr_->SyncCurrentTableVersion();
  return 0;
}

int TagTable::remove(ErrorInfo &err_info) {
  // TsEntityGroup->drop_mutex_ control concurrency,so we don't add ref_cnt mutex to control waiting
  if (m_version_mgr_->RemoveAll(err_info) < 0) {
    LOG_ERROR("call TagTableVersionManager::RemoveAll failed, %s ", err_info.errmsg.c_str());
    return err_info.errcode;
  }
  delete m_version_mgr_;
  m_version_mgr_ = nullptr;
  if (m_partition_mgr_->RemoveAll(err_info) < 0) {
    LOG_ERROR("call TagPartitionTableManager::RemoveAll failed, %s ", err_info.errmsg.c_str());
    return err_info.errcode;
  }
  delete m_partition_mgr_;
  m_partition_mgr_ = nullptr;
  if (m_index_ && (m_index_->remove() < 0)) {
    LOG_ERROR("TagHashIndex remove failed.");
    return -1;
  }
  delete m_index_;
  m_index_ = nullptr;
  LOG_DEBUG("Tag table remove successed.");
  fs::remove_all((m_db_path_ + m_tbl_sub_path_).c_str());
  return 0;
}

// check ptag exist
bool TagTable::hasPrimaryKey(const char* primary_tag_val, int len, uint32_t& entity_id, uint32_t& sub_group_id) {
  auto ret = m_index_->get(primary_tag_val, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    return false;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_WARN("primary key record's table_version[%u] does not exist.", ret.first);
    return false;
  }
  tag_partition_table->getEntityIdGroupId(ret.second, entity_id, sub_group_id);
  return true;
}

// check ptag exist
bool TagTable::hasPrimaryKey(const char* primary_tag_val, int len) {
  auto ret = m_index_->get(primary_tag_val, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    return false;
  }
  return true;
}

// insert tag record
int TagTable::InsertTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id) {
  // 1. check version
  auto tag_version_object = m_version_mgr_->GetVersionObject(payload.GetTsVersion());
  if (nullptr == tag_version_object) {
    LOG_ERROR("Tag table version[%u] doesnot exist.", payload.GetTsVersion());
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("Tag partition table version[%u] doesnot exist.", tag_partition_version);
    return -1;
  }

  // 2. insert partition data
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, sub_group_id, payload.getHashPoint(),
                                  payload.GetTagAddr(), &row_no) < 0) {
    LOG_ERROR("insert tag partition table[%s/%s] failed. ",
       tag_partition_table->m_tbl_sub_path_.c_str(), tag_partition_table->m_name_.c_str());
    return -1;
  }
  // 3. insert index data
  TSSlice tmp_slice = payload.GetPrimaryTag();
  if (m_index_->put(tmp_slice.data, tmp_slice.len, tag_partition_version, row_no) < 0) {
    LOG_ERROR("insert hash index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
    return -1;
  }

  // 4. set undelete mark
  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

// update tag record
int TagTable::UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info) {
  // 1. delete
  TSSlice tmp_primary_tag = payload.GetPrimaryTag();
  if (this->DeleteTagRecord(tmp_primary_tag.data, tmp_primary_tag.len, err_info) < 0) {
    err_info.errmsg = "delete tag data failed";
    LOG_ERROR("delete tag data failed, error: %s", err_info.errmsg.c_str());
    return err_info.errcode;
  }

  // 2. insert
  if ((err_info.errcode = this->InsertTagRecord(payload, sub_group_id, entity_id)) < 0 ) {
    err_info.errmsg = "insert tag data fail";
    return err_info.errcode;
  }
  return 0;
}

// delete tag record by ptag
int TagTable::DeleteTagRecord(const char *primary_tags, int len, ErrorInfo& err_info) {
  // 1. find
  auto ret = m_index_->get(primary_tags, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    err_info.errmsg = "delete data not found";
    err_info.errcode = -1;
    return -1;
  }
  auto tag_part_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (tag_part_table == nullptr) {
    LOG_ERROR("Tag Partition Table does not exist. table_version: %u", ret.first);
    err_info.errmsg = "delete data not found";
    err_info.errcode = -1;
    return -1;
  }
  // 2. delete mark
  tag_part_table->startRead();
  tag_part_table->setDeleteMark(ret.second);
  tag_part_table->stopRead();

  // 3. delete index record
  auto ret_del = m_index_->delete_data(primary_tags, len);
  return 0;
}

// query tag by ptag
int TagTable::GetEntityIdList(const std::vector<void*>& primary_tags, const std::vector<uint32_t> &scan_tags,
                              std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                              kwdbts::ResultSet* res, uint32_t* count, uint32_t table_version) {
  TagPartitionTableRowID row;
  uint32_t fetch_count = 0;
  TagVersionObject*   result_tag_version_obj = nullptr;
  TagPartitionTable*  src_tag_partition_tbl = nullptr;
  TableVersionID last_tbl_version = 0;
  // 1. get result table version scan tags
  result_tag_version_obj = m_version_mgr_->GetVersionObject(table_version);
  if (nullptr == result_tag_version_obj) {
    LOG_ERROR("GetVersionObject not found. table_version: %u ", table_version);
    return -1;
  }
  std::vector<uint32_t> result_scan_tags;
  for(const auto& tag_idx : scan_tags) {
    result_scan_tags.emplace_back(result_tag_version_obj->getValidSchemaIdxs()[tag_idx]);
  }
  std::vector<uint32_t> src_scan_tags;
  for (int idx = 0; idx < primary_tags.size(); idx++) {
    //2. get index data
    auto ret = m_index_->get(reinterpret_cast<char*>(primary_tags[idx]), m_index_->keySize());
    if (ret.second == 0) {
      // not found
      LOG_DEBUG("hash index not found.");
      continue;
    }
    TableVersionID tbl_version = ret.first;
    TagPartitionTableRowID row = ret.second;
    if (src_tag_partition_tbl == nullptr || tbl_version != last_tbl_version) {
      src_tag_partition_tbl = m_partition_mgr_->GetPartitionTable(tbl_version);
      if (nullptr == src_tag_partition_tbl) {
        LOG_ERROR("GetPartitionTable not found. table_version: %u ", tbl_version);
        return -1;
      }
      last_tbl_version = tbl_version;
      // get actual schemas
      src_scan_tags.clear();
      for(int idx = 0; idx < scan_tags.size(); ++idx) {
        if (result_scan_tags[idx] >= src_tag_partition_tbl->getIncludeDroppedSchemaInfos().size()) {
          src_scan_tags.push_back(INVALID_COL_IDX);
        } else {
          src_scan_tags.push_back(result_scan_tags[idx]);
        }
      }
    }
    //3. read data
    src_tag_partition_tbl->startRead();
    if (!EngineOptions::isSingleNode()) {
      uint32_t hps;
      src_tag_partition_tbl->getHashpointByRowNum(row, &hps);
      src_tag_partition_tbl->getHashedEntityIdByRownum(row, hps, entity_id_list);
    } else {
      src_tag_partition_tbl->getEntityIdByRownum(row, entity_id_list);
    }
    // tag column
    int err_code = 0;
    if ((err_code = src_tag_partition_tbl->getColumnsByRownum(
                                          row, src_scan_tags, 
                                          result_tag_version_obj->getIncludeDroppedSchemaInfos(),
                                          res)) < 0) {
      src_tag_partition_tbl->stopRead();
      return err_code;
    }
    src_tag_partition_tbl->stopRead();
    fetch_count++;
  }  // end for primary_tags
  *count = fetch_count;
  return 0;                              
}

// query tag by rownum
int TagTable::GetColumnsByRownumLocked(TableVersion src_table_version, uint32_t src_row_id,
                              const std::vector<uint32_t>& src_tag_idxes,
                              const std::vector<TagInfo>& result_tag_infos,
                              kwdbts::ResultSet* res) {
  auto src_tag_partition_tbl = m_partition_mgr_->GetPartitionTable(src_table_version);
  if (nullptr == src_tag_partition_tbl) {
      LOG_ERROR("GetPartitionTable not found. table_version: %u ", src_table_version);
      return -1;
  }
  //2. read data
  src_tag_partition_tbl->startRead();
  // tag column
  int err_code = 0;
  if ((err_code = src_tag_partition_tbl->getColumnsByRownum(
                                        src_row_id, src_tag_idxes, 
                                        result_tag_infos,
                                        res)) < 0) {
    src_tag_partition_tbl->stopRead();
    return err_code;
  }
  src_tag_partition_tbl->stopRead();
  return 0;
}

int TagTable::CalculateSchemaIdxs(TableVersion src_table_version, const std::vector<uint32_t>& result_scan_idxs,
                                   const std::vector<TagInfo>& result_tag_infos,
                                  std::vector<uint32_t>* src_scan_idxs) {
  auto src_tag_version_obj = m_version_mgr_->GetVersionObject(src_table_version);
  if (nullptr == src_tag_version_obj) {
    LOG_ERROR("GetVersionObject not found. table_version: %u ", src_table_version);
    return -1;
  }

  // get actual schemas
  src_scan_idxs->clear();
  for(int idx = 0; idx < result_scan_idxs.size(); ++idx) {
    if (result_scan_idxs[idx] >= src_tag_version_obj->getIncludeDroppedSchemaInfos().size()) {
      src_scan_idxs->push_back(INVALID_COL_IDX);
    } else {
      src_scan_idxs->push_back(result_scan_idxs[idx]);
    }
  }
  return 0;
}

int TagTable::AddNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info) {
  // 1. create tag version object
  auto tag_ver_obj = m_version_mgr_->CreateTagVersionObject(schema, new_version, err_info);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("call CreateTagVersionObject failed, %s ", err_info.errmsg.c_str());
    return -1;
  }
  // 2. create tag partition table
  if (m_partition_mgr_->CreateTagPartitionTable(schema, new_version, err_info) < 0) {
    LOG_ERROR("CreateTagPartitionTable failed, %s", err_info.errmsg.c_str());
    m_version_mgr_->RollbackTableVersion(new_version, err_info);
    return -1;
  }
  tag_ver_obj->setStatus(TAG_STATUS_READY);
  m_version_mgr_->UpdateNewestTableVersion(new_version);
  return 0;
}

int TagTable::AlterTableTag(AlterType alter_type, const AttributeInfo& attr_info,
                    uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  // get latest version
  TableVersion latest_version = m_version_mgr_->GetNewestTableVersion();
  TagVersionObject* tag_ver_obj = m_version_mgr_->GetVersionObject(cur_version);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("tag version [%u] does not exist.", cur_version);
    err_info.errmsg = "tag version " + std::to_string(cur_version) + " does not exist";
    return -1;
  }
  TagInfo tag_schema = {attr_info.id, attr_info.type,
                        static_cast<uint32_t>(attr_info.length), 0,
                        static_cast<uint32_t>(attr_info.size), GENERAL_TAG};
  std::vector<TagInfo> cur_schemas = tag_ver_obj->getIncludeDroppedSchemaInfos();
  int col_idx = tag_ver_obj->getTagColumnIndex(tag_schema); 
  switch (alter_type)
  {
    case ADD_COLUMN:
      if (col_idx >= 0 && latest_version >= new_version) {
        LOG_WARN("tag schema already exist")
        return 0;
      }
      cur_schemas.push_back(tag_schema);
      break;

    case DROP_COLUMN:
      if (col_idx < 0) {
        LOG_WARN("tag schema does not exist, column(id %u), table_id = %lu", tag_schema.m_id, m_table_id);
        err_info.errmsg = "tag schema does not exist";
        return -1;
      } else if (latest_version >= new_version) {
        LOG_WARN("new_version [%u] already exist, tag newest_version[%u]",
              new_version, latest_version);
        return 0;
      }
      cur_schemas[col_idx].setFlag(AINFO_DROPPED);
      break;

    case ALTER_COLUMN_TYPE:
      if (col_idx < 0) {
        LOG_ERROR("alter tag type failed: column (id %u) does not exists, table id = %lu",
                  tag_schema.m_id, m_table_id);
        err_info.errmsg = "tag schema does not exist";
        return -1;
      } else if (latest_version == new_version) {
        return 0;
      }
      cur_schemas[col_idx] = tag_schema;
      break;
    default:
      LOG_ERROR("alter type is invaild.");
      return -1;
  }
  // add new partition table
  return AddNewPartitionVersion(cur_schemas, new_version, err_info);
}

int TagTable::initHashIndex(int flags, ErrorInfo& err_info) {
  string index_name = std::to_string(m_table_id) + ".tag"+ ".ht";
  m_index_ = new MMapHashIndex(m_partition_mgr_->getPrimarySize());
  err_info.errcode = m_index_->open(index_name, m_db_path_, m_tbl_sub_path_, 
                                    flags, err_info);
  if (err_info.errcode < 0) {
    delete m_index_;
    m_index_ = nullptr;
    err_info.errmsg = "create Hash Index failed.";
    LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), index_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  return err_info.errcode;
}

int TagTable::loadAllVersions(std::vector<TableVersion>& all_versions, ErrorInfo& err_info) {
  string real_path = m_db_path_ + m_tbl_sub_path_;
  // Load all versions of root table
  DIR* dir_ptr = opendir(real_path.c_str());
  if (dir_ptr) {
    string prefix = std::to_string(m_table_id) + ".tag" + ".mt" + '_';
    size_t prefix_len = prefix.length();
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
      }
      if (entry->d_type == DT_REG &&
          strncmp(entry->d_name, prefix.c_str(), prefix_len) == 0) {
        all_versions.push_back(std::stoi(entry->d_name + prefix_len));
      }
    }
    closedir(dir_ptr);
  } else {
    LOG_ERROR("opendir [%s] failed.", real_path.c_str());
    return -1;
  }
  if (all_versions.empty()) {
    LOG_ERROR("read all tag table version is empty.");
    return -1;
  }
  return 0;
}

// wal
void TagTable::sync_with_lsn(kwdbts::TS_LSN lsn) {
  std::vector<TagPartitionTable*> all_parttables;
  TableVersion cur_tbl_version = this->GetTagTableVersionManager()->GetCurrentTableVersion();
  m_partition_mgr_->GetAllPartitionTablesLessVersion(all_parttables, cur_tbl_version);
  for (const auto& part_table : all_parttables) {
    // add read lock
    (void)part_table->startRead();
    part_table->sync_with_lsn(lsn);
    (void)part_table->stopRead();
  }
  m_index_->setLSN(lsn);
  return ;
}

TagTuplePack* TagTable::GenTagPack(const char* primarytag, int len) {
  // 1. search primary tag
  auto ret = m_index_->get(primarytag, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    LOG_ERROR("GenTagPack does not found primary tag. len: %d", len);
    return nullptr;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return nullptr;
  }

  // 2. gen tag pack
  return tag_partition_table->GenTagPack(ret.second);
}

int TagTable::undoAlterTagTable(uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  // 1. check version is valid
  auto cur_tbl_version = m_version_mgr_->GetCurrentTableVersion();
  if (cur_version < cur_tbl_version) {
    LOG_WARN("Tag rollback version: %u  < committed version: %u ",
              cur_version, cur_tbl_version);
    return 0;
  }
  auto newest_tbl_version = m_version_mgr_->GetNewestTableVersion();
  if (new_version > newest_tbl_version) {
    LOG_ERROR("Tag Table newest table version is %u < rollback version: %u ",
               newest_tbl_version, new_version);
    return -1;
  }
  LOG_INFO("Rollback tag version begin, new_version: %u to cur_version: %u ", new_version, cur_version);
  m_version_mgr_->UpdateNewestTableVersion(cur_version, false);
  m_version_mgr_->RollbackTableVersion(new_version, err_info);
  m_partition_mgr_->RollbackPartitionTableVersion(new_version, err_info);
  return 0;
}

int TagTable::InsertForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag) {
  // don't do undo when tag table insert successfully
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // no found
    return 0;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return -1;
  }
  ErrorInfo err_info;
  return DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
}

int TagTable::InsertForRedo(uint32_t group_id, uint32_t entity_id,
		    kwdbts::Payload &payload) {
  // 1. search ptag
  TSSlice tmp_slice = payload.GetPrimaryTag();
  auto ret = m_index_->get(tmp_slice.data, tmp_slice.len);
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }
    tag_partition_table->startRead();
    if (!tag_partition_table->isValidRow(ret.second)) {
      tag_partition_table->unsetDeleteMark(ret.second);
    }
    tag_partition_table->stopRead();
    return 0;
  }
  // 2. redo insert
  return InsertTagRecord(payload, group_id, entity_id);

}

int TagTable::DeleteForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag_pack) {
    // 1. search primary tag
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  TagPartitionTable* tag_partition_table = nullptr;
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }
    tag_partition_table->startRead();
    tag_partition_table->unsetDeleteMark(ret.second);
    tag_partition_table->stopRead();
    return 0;
  }
  // 2. undo for insert tags
  if (tag_pack.data == nullptr) {
    LOG_ERROR("DeleteForUndo tag pack data is nullptr.");
    return -1;
  }
  TableVersion tbl_version = *reinterpret_cast<uint32_t*>(tag_pack.data + TagTuplePack::versionOffset_);
  auto tag_version_object = m_version_mgr_->GetVersionObject(tbl_version);
  if (nullptr == tag_version_object) {
    LOG_ERROR("tag metadata version [%u] object does not exist. ", tbl_version);
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", tag_partition_version);
    return -1;
  }
  uint32_t tag_hash_point = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  std::vector<TagInfo> schemas;
  TagTuplePack tag_tuple(schemas, tag_pack.data, tag_pack.len);

  // 3. insert partition data
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, group_id, tag_hash_point,
                                  tag_tuple.getTags().data, &row_no) < 0) {
    LOG_ERROR("insert tag partition table[%s/%s] failed. ",
       tag_partition_table->m_tbl_sub_path_.c_str(), tag_partition_table->m_name_.c_str());
    return -1;
  }
  // 4. insert index data
  if (m_index_->put(primary_tag.data, primary_tag.len, tbl_version, row_no) < 0) {
    LOG_ERROR("insert hash index data failed. table_version: %u row_no: %lu ", tbl_version, row_no);
    return -1;
  }

  // 5. set undelete mark
  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

int TagTable::DeleteForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag) {
  // 1. search primary tag
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    return 0;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return -1;
  }
  // 2. delete data
  TagPartitionTableRowID row_no = ret.second;
  tag_partition_table->startRead();
  tag_partition_table->setDeleteMark(ret.second);
  tag_partition_table->stopRead();
  auto ret_del = m_index_->delete_data(primary_tag.data, primary_tag.len);

  return 0;
}

int TagTable::UpdateForRedo(uint32_t group_id, uint32_t entity_id,
                    const TSSlice& primary_tag, kwdbts::Payload &payload) {
  // 1. search primary tag
  TagPartitionTable* tag_partition_table = nullptr;
  TableVersion tbl_version = 0;
  TagPartitionTableRowID tag_row_no = 0;
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }
    // delete mark
    tag_partition_table->startRead();
    tag_partition_table->setDeleteMark(ret.second);
    tag_partition_table->stopRead();
    // delete index data
    auto ret_del = m_index_->delete_data(primary_tag.data, primary_tag.len);
  }
  return InsertTagRecord(payload, group_id, entity_id);
}

int TagTable::UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                    const TSSlice& old_tag) {
  int rc = 0;
  ErrorInfo err_info;
  // 1. check
  if (old_tag.data == nullptr) {
    LOG_ERROR("UpdateForUndo old tag data is nullptr.");
    return -1;
  }
  TableVersion tbl_version = *reinterpret_cast<uint32_t*>(old_tag.data + TagTuplePack::versionOffset_);
  auto tag_version_object = m_version_mgr_->GetVersionObject(tbl_version);
  if (nullptr == tag_version_object) {
    LOG_ERROR("tag metadata version [%u] object does not exist. ", tbl_version);
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", tag_partition_version);
    return -1;
  }
  // 1. delete
  rc = DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
  if (rc < 0) {
    return rc;
  }
  uint32_t tag_hash_point = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  std::vector<TagInfo> schemas;
  TagTuplePack tag_tuple(schemas, old_tag.data, old_tag.len);
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, group_id, tag_hash_point, tag_tuple.getTags().data, &row_no) < 0) {
    LOG_ERROR("UpdateForUndo insert tag failed.");
    return -1;
  }
  if (m_index_->put(primary_tag.data, primary_tag.len, tag_partition_version, row_no) < 0) {
    LOG_ERROR("UpdateForUndo insert hash index failed");
    return -1;
  }
  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

TagTable* OpenTagTable(const std::string& db_path, const std::string &dir_path,
                                uint64_t table_id, int32_t entity_group_id, ErrorInfo &err_info) {
  // check path
  std::string new_sub_path = dir_path + "tag/";
  std::string new_dir_path = db_path + new_sub_path;
  if (access(new_dir_path.c_str(), 0)) {
     LOG_ERROR("Directory path %s doesnot exist.", new_dir_path.c_str());
     return nullptr;
  }
  // open tag table
  TagTable* tmp_bt = KNEW TagTable(db_path, new_sub_path, table_id, entity_group_id);
  int err_code = tmp_bt->open(err_info);
  if (err_info.errcode == KWENOOBJ) {
    // table not exists.
    LOG_WARN("the tag table %s%s does not exist", new_sub_path.c_str(), std::to_string(table_id).c_str());
    err_info.clear();
    delete tmp_bt;
    return nullptr;
  }
  if (err_code < 0) {
    // other errors
    LOG_ERROR("failed to open the tag table %s%s, error: %s",
      new_sub_path.c_str(), std::to_string(table_id).c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    return nullptr;
  }
  return tmp_bt;
}

TagTable* CreateTagTable(const std::vector<TagInfo> &tag_schema,
                                   const std::string& db_path, const std::string &dir_path,
                                   uint64_t table_id, int32_t entity_group_id,
                                   uint32_t table_version, ErrorInfo &err_info) {
  // check path
  std::string sub_path = dir_path + "tag/";
  std::string new_dir_path = db_path + sub_path;
  if (access(new_dir_path.c_str(), 0)) {
    if (!MakeDirectory(new_dir_path)) {
      return nullptr;
    }
  } else {
    LOG_ERROR("path: %s already exist.", new_dir_path.c_str());
    return nullptr;
  }
  // create tag table
  TagTable* tmp_bt = KNEW TagTable(db_path, sub_path, table_id, entity_group_id);
  if (tmp_bt == nullptr) {
    LOG_ERROR("create tag table out off memory.");
    return nullptr;
  }
  if (tmp_bt->create(tag_schema, table_version, err_info)< 0) {
    LOG_ERROR("failed to create the tag table %s%lu, error: %s",
        dir_path.c_str(), table_id, err_info.errmsg.c_str());
    delete tmp_bt;
    tmp_bt = nullptr;
  }

  return tmp_bt;
}

int DropTagTable(TagTable* bt, ErrorInfo& err_info) {
  return bt->remove(err_info);
}

int TagPartitionTableManager::Init(ErrorInfo& err_info) {
  return 0;
}

TagPartitionTableManager::~TagPartitionTableManager() {
  wrLock();
  // remove all tables
  for (auto& it : m_partition_tables_) {
    if(it.second) {
      delete it.second;
      it.second = nullptr;
    }
  }
  m_partition_tables_.clear();
  unLock();
}

int TagPartitionTableManager::CreateTagPartitionTable(const std::vector<TagInfo>& schema, uint32_t ts_version,  ErrorInfo& err_info) {
  // 1. check path
  std::string partition_table_path = m_tbl_sub_path_ + "tag" + "_" + std::to_string(ts_version) + "/";
  std::string real_path = m_db_path_ + partition_table_path;
  wrLock();
  // check partition table 
  auto part_tbl = m_partition_tables_.find(ts_version);
  if (part_tbl != m_partition_tables_.end()) {
     unLock();
     LOG_WARN("tag partition table version [%u] already exist.", ts_version);
     return 0;
  }
  if (access(real_path.c_str(), 0)) {
    // path does not exist
    if (!MakeDirectory(real_path)) {
      err_info.errcode = -1;
      err_info.errmsg = "create directory failed";
      LOG_ERROR("Create path : %s failed.", real_path.c_str());
      unLock();
      fs::remove_all(real_path.c_str());
      return -1;
    }
  }

  // 2. create partition table
  TagPartitionTable* tmp_bt = new TagPartitionTable();
  if (tmp_bt->open(m_table_prefix_name_, m_db_path_, partition_table_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
      err_info.errcode == KWECORR) {
    tmp_bt->create(schema, m_entity_group_id_, ts_version, err_info);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("failed to create tag partition table %s%s, error: %s",
        partition_table_path.c_str(), m_table_prefix_name_.c_str(), err_info.errmsg.c_str());
    tmp_bt->setObjectReady();
    tmp_bt->remove();
    delete tmp_bt;
    tmp_bt = nullptr;
    unLock();
    fs::remove_all(real_path.c_str());
    return -1;
  }
  if (m_primary_tag_size_ == 0) {
    m_primary_tag_size_ = tmp_bt->primaryTagSize();
  }
  // 3. insert into cache
  m_partition_tables_.insert({ts_version, tmp_bt});
  unLock();
  return 0;
}

int TagPartitionTableManager::OpenTagPartitionTable(TableVersion table_version, ErrorInfo& err_info) {
  wrLock();
  auto part_table = m_partition_tables_.find(table_version);
  if (part_table != m_partition_tables_.end()) {
     unLock();
     LOG_DEBUG("TagPartitionTable of version %u was existed.", table_version);
     return 0;
  }
  // set partition table path
  std::string partition_table_path = m_tbl_sub_path_ + "tag" + "_" + std::to_string(table_version) + "/";
  std::string real_path = m_db_path_ + partition_table_path;
  if (access(real_path.c_str(), 0)) {
    // path does not exist
    LOG_DEBUG("The path does not exist. %s ", real_path.c_str());
    unLock();
    return 0;
  }
  TagPartitionTable* tmp_bt = new TagPartitionTable();
  int err_code = tmp_bt->open(m_table_prefix_name_, m_db_path_, partition_table_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_code < 0) {
    // other errors
    LOG_ERROR("failed to open the tag table %s%s, error: %s",
      partition_table_path.c_str(), m_table_prefix_name_.c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    unLock();
    return -1;
  }
  if (m_primary_tag_size_ == 0) {
    m_primary_tag_size_ = tmp_bt->primaryTagSize();
  }
  m_partition_tables_.insert({table_version, tmp_bt});
  unLock();
  return 0;
}
                          
TagPartitionTable* TagPartitionTableManager::GetPartitionTable(uint32_t table_version) {
  rdLock();
  auto part_table = m_partition_tables_.find(table_version);
  if (part_table != m_partition_tables_.end()) {
     unLock();
     return part_table->second;
  }
  unLock();
  LOG_ERROR("Get tag partition table failed. table_version: %u ", table_version);
  return nullptr;
}

void TagPartitionTableManager::GetAllPartitionTablesLessVersion(std::vector<TagPartitionTable*>& tag_part_tables, TableVersion max_table_version) {
  rdLock();
  for (const auto& it : m_partition_tables_) {
    if (it.first <= max_table_version) {
      tag_part_tables.push_back(it.second);
    }
  }
  unLock();
  return ;
}

int TagPartitionTableManager::RemoveAll(ErrorInfo& err_info) {
  wrLock();
  // remove all tables
  for (auto& it : m_partition_tables_) {
    if(it.second) {
      it.second->remove();
      delete it.second;
      it.second = nullptr;
    }
  }
  m_partition_tables_.clear();
  unLock();
  return 0;
}

int TagPartitionTableManager::RollbackPartitionTableVersion(TableVersion need_rollback_version, ErrorInfo& err_info) {
  wrLock();
  auto part_table = m_partition_tables_.find(need_rollback_version);
  if (part_table == m_partition_tables_.end()) {
     unLock();
     LOG_WARN("tag partition table need rollback version [%u] does not exist.", need_rollback_version);
     return 0;
  }
  part_table->second->remove();
  delete part_table->second;
  m_partition_tables_.erase(part_table);
  unLock();
  return 0;
}