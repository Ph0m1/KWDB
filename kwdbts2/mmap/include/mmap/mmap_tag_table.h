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
#include <string>
#include <vector>
#include "utils/date_time_util.h"
#include "big_table.h"
#include "mmap_object.h"
#include "mmap_hash_index.h"
#include "ts_common.h"
#include "lg_api.h"
#include "payload.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"
#include "cm_func.h"
#include "mmap_tag_column_table.h"
#include "mmap_tag_version_manager.h"
#include "mmap_tag_column_table_aux.h"

extern uint32_t k_entity_group_id_size;
extern uint32_t k_per_null_bitmap_size;

using TagHashIndex = MMapHashIndex;
using TableVersion = uint32_t;
using TagPartitionTable = MMapTagColumnTable;

#define INVALID_COL_IDX UINT32_MAX

class TagPartitionTableManager;

class TagTable {
 private:
  KLatch*   m_mutex_;

  int mutexLock() {return MUTEX_LOCK(m_mutex_);}

  int mutexUnlock() {return MUTEX_UNLOCK(m_mutex_);}

 protected:
  TagTableVersionManager*        m_version_mgr_{nullptr};
  TagHashIndex*                  m_index_{nullptr};
  TagPartitionTableManager*      m_partition_mgr_{nullptr};
  std::string m_db_path_;
  std::string m_tbl_sub_path_;
  uint64_t m_table_id{0};
  uint32_t m_entity_group_id_{0};
  size_t   m_primary_tag_size{0};
 public:
  explicit TagTable(const std::string& db_path, const std::string& sub_path, uint64_t table_id, int32_t entity_group_id);

  virtual ~TagTable();

  int create(const vector<TagInfo> &schema, uint32_t table_version, ErrorInfo &err_info);

  int open(ErrorInfo &err_info);

  int remove(ErrorInfo &err_info);
  // check ptag exist & get entity id
  bool hasPrimaryKey(const char* primary_tag_val, int len, uint32_t& entity_id, uint32_t& group_id);

  // check ptag exist
  bool hasPrimaryKey(const char* primary_tag_val, int len);

  // insert tag record
  int InsertTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id);

  // update tag record
  int UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info);

  // query tag by ptag
  int GetEntityIdList(const std::vector<void*>& primary_tags, const std::vector<uint32_t> &scan_tags,
                              std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                              kwdbts::ResultSet* res, uint32_t* count, uint32_t table_version = 0);
  // query tag by rownum
  int GetColumnsByRownumLocked(TableVersion src_table_version, uint32_t src_row_id,
                              const std::vector<uint32_t>& src_tag_idxes,
                              const std::vector<TagInfo>& result_tag_infos,
                              kwdbts::ResultSet* res);

  int CalculateSchemaIdxs(TableVersion src_table_version, const std::vector<uint32_t>& result_scan_idxs,
                                   const std::vector<TagInfo>& result_tag_infos,
                                  std::vector<uint32_t>* src_scan_idxs);

  // delete tag record by ptag
  int DeleteTagRecord(const char *primary_tags, int len, ErrorInfo& err_info);

  int AlterTableTag(AlterType alter_type, const AttributeInfo& attr_info,
                    uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

  inline TagTableVersionManager* GetTagTableVersionManager() {return m_version_mgr_;}

  inline TagPartitionTableManager* GetTagPartitionTableManager() {return m_partition_mgr_;}

  // wal
  void sync_with_lsn(kwdbts::TS_LSN lsn);

  void sync(int flag);

  TagTuplePack* GenTagPack(const char* primarytag, int len);

  int undoAlterTagTable(uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

  int InsertForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag);
  int InsertForRedo(uint32_t group_id, uint32_t entity_id,
		    kwdbts::Payload &payload);
  int DeleteForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag_pack);

  int DeleteForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag);
  int UpdateForRedo(uint32_t group_id, uint32_t entity_id,
                    const TSSlice& primary_tag, kwdbts::Payload &payload);
  int UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                    const TSSlice& old_tag);

  int AddNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info);
 private:

  int initHashIndex(int flags, ErrorInfo& err_info);

  int loadAllVersions(std::vector<TableVersion>& all_versions, ErrorInfo& err_info);

};

class TagPartitionTableManager {
private:
  // Read/write lock used to control concurrent access.
  KRWLatch rw_latch_;
  int rdLock() {return RW_LATCH_S_LOCK(&rw_latch_);}
  int wrLock() {return RW_LATCH_X_LOCK(&rw_latch_);}
  int unLock() {return RW_LATCH_UNLOCK(&rw_latch_);}
protected:
  // tag partition table name prefix
  std::string m_table_prefix_name_;
  // Storage path of the database.
  string m_db_path_;
  // Table of subpaths.
  string m_tbl_sub_path_;
  // ID of the table.
  uint32_t m_table_id_;
  // ID of entitygroup
  uint32_t m_entity_group_id_{0};
  // Primary tag size
  size_t   m_primary_tag_size_{0};
  // Stores ordered mappings of all versions of the version table.
  std::map<uint32_t, TagPartitionTable*> m_partition_tables_;

public:
  explicit TagPartitionTableManager (const std::string& db_path, const std::string& tbl_sub_path, uint32_t table_id, uint32_t entity_group_id) :
      m_db_path_(db_path), m_tbl_sub_path_(tbl_sub_path), m_table_id_(table_id),m_entity_group_id_(entity_group_id),
      rw_latch_(RWLATCH_ID_TAG_TABLE_PARTITION_MGR_RWLOCK) {
        m_table_prefix_name_ = std::to_string(table_id) + ".tag" + ".pt";
      }

  virtual ~TagPartitionTableManager();

  int Init(ErrorInfo& err_info);

  int CreateTagPartitionTable(const std::vector<TagInfo>& schema, uint32_t ts_version,
                          ErrorInfo& err_info);

  int OpenTagPartitionTable(TableVersion table_version, ErrorInfo& err_info);
                          
  TagPartitionTable* GetPartitionTable(uint32_t table_version);

  int RemoveAll(ErrorInfo& err_info);

  inline size_t getPrimarySize() {return m_primary_tag_size_;}

  void GetAllPartitionTablesLessVersion(std::vector<TagPartitionTable*>& tag_part_tables, TableVersion max_table_version);

  int RollbackPartitionTableVersion(TableVersion need_rollback_version, ErrorInfo& err_info);
};

TagTable* OpenTagTable(const std::string& db_path, const std::string &dir_path,
                                uint64_t table_id,  int32_t entity_group_id, ErrorInfo &err_info);

TagTable* CreateTagTable(const std::vector<TagInfo> &tag_schema,
                                   const std::string& db_path, const std::string &dir_path,
                                   uint64_t table_id, int32_t entity_group_id,
                                   uint32_t table_version, ErrorInfo &err_info);

int DropTagTable(TagTable* bt, ErrorInfo& err_info);

