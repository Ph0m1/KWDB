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

#include <dirent.h>
#include <iostream>
#include <fstream>
#include "ts_snapshot.h"
#include "ts_table.h"
#include "st_group_manager.h"
#include "payload_builder.h"
#include "sys_utils.h"

namespace kwdbts {

TsTableSnapshot::~TsTableSnapshot() {
  delete snapshot_group_;
}

KStatus TsTableSnapshot::Init(kwdbContext_p ctx) {
  RangeGroup hash_range = entity_group_->HashRange();
  // construct snapshot file name
  string snapshot_str = to_string(hash_range.range_group_id) + "_" + to_string(snapshot_info_.id);
  string range_tbl_sub_path = tbl_sub_path_ + snapshot_str +  + "/";
  snapshot_group_ = new TsEntityGroup(ctx, entity_bt_manager_, db_path_, table_id_, hash_range, range_tbl_sub_path);

  if (snapshot_info_.type == 0) {
    // type = 0, create new snapshot and its directory in source node
    makeDirectory(db_path_ + range_tbl_sub_path);
    vector<TagInfo> tag_schema;
    std::vector<TagColumn*> tag_schema_info = entity_group_->GetSubEntityGroupTagbt()->getSchemaInfo();
    // Enough space is pre-allocated to avoid multiple memory allocations.
    tag_schema.reserve(tag_schema_info.size());
    for (auto& schema_info : tag_schema_info) {
      tag_schema.push_back(schema_info->attributeInfo());
    }
    // Create TsEntityGroup of the snapshot
    KStatus s = snapshot_group_->Create(ctx, tag_schema);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Create Snapshot TsEntityGroup failed, range_group_id[%lu], snapshot id[%lu]",
                hash_range.range_group_id, snapshot_info_.id);
      return KStatus::FAIL;
    }
  } else {
    // Open and initialize an existing snapshot TsEntityGroup in destination node
    KStatus s = snapshot_group_->OpenInit(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR(" Snapshot TsEntityGroup initialize failed, snapshot id=%lu", snapshot_info_.id);
      return KStatus::FAIL;
    }
  }
  schema_ = entity_bt_manager_->GetSchemaInfoWithHidden();
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::BuildSnapshot(kwdbContext_p ctx, TS_LSN lsn) {
  snapshot_mutex_.lock();
  // get tag rows to be migrated from tag table
  KStatus s = getMigratedTagRows();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Get migrated tag rows failed, range_group_id=%lu", snapshot_group_->HashRange().range_group_id);
    return s;
  }

  KTimestamp start_ts = INT64_MIN;
  KTimestamp end_ts = INT64_MAX;
  KwTsSpan ts_span = {start_ts, end_ts };
  for (uint32_t row_num : tag_row_nums_) {
    // construct payload and write into snapshot
    s = genMigratePayloadByBuilder(ctx, row_num, ts_span);
    if (s == KStatus::FAIL) {
      LOG_ERROR("MigrateEntityData failed, range_group_id=%lu", snapshot_group_->HashRange().range_group_id);
      return s;
    }
  }
  snapshot_mutex_.unlock();
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::CompressSnapshot(kwdbContext_p ctx, size_t* total) {
  // sync data to disk.
  static_cast<MMapTagColumnTable*>(snapshot_group_->GetSubEntityGroupTagbt())->sync(O_MATERIALIZATION);
  snapshot_group_->GetSubEntityGroupManager()->sync(O_MATERIALIZATION);
  // construct snapshot directory path
  KString dir_path = db_path_ + tbl_sub_path_ + std::to_string(snapshot_group_->HashRange().range_group_id) + "_" +
                     std::to_string(snapshot_info_.id);;
  KString file_name = dir_path + ".sqfs";  // compressed snapshot name(with path)
  // construct and execute system command "mksquashfs ..." to compress snapshot
  KString cmd = "mksquashfs " + dir_path + " " + file_name + " > /dev/null 2>&1";
  LOG_INFO("%s", cmd.c_str());
  if (!System(cmd)) {
    LOG_ERROR("Compress Snapshot file failed, snapshot path[%s]", dir_path.c_str());
    return KStatus::FAIL;
  }

  std::ifstream get_data_handle;
  // Open the compressed file and get its total size
  get_data_handle.open(file_name.c_str(), std::ios::binary | std::ios::in);
  if (get_data_handle.fail()) {
    LOG_ERROR("Open compressed file failed during CompressSnapshot, compressed snapshot[%s]", dir_path.c_str());
    return KStatus::FAIL;
  }
  get_data_handle.seekg(0, std::ios::end);  // go to end of the file
  *total  = get_data_handle.tellg();        // get total size
  get_data_handle.seekg(0, std::ios::beg);  // back to file begin
  LOG_INFO("Compressed snapshot file total size[%lu]", *total);
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::GetSnapshotData(kwdbContext_p ctx, uint64_t range_group_id,  size_t offset, size_t limit,
                                         TSSlice* data, size_t* total) {
  size_t length = (offset + limit > *total) ? *total- offset : limit;
  // When getting snapshot data sometimes, the total passed down from the upper layer is 0,
  // and the length is too large to cause an error when allocating memory
  // the following judgment is added to intercept it
  if (length > limit) {
    LOG_ERROR("Get data length failed during GetSnapshotData, range_group_id=%lu, snapshot_id=%ld",
              range_group_id, snapshot_info_.id);
    return KStatus::FAIL;
  }
  char* get_data_area = new char[length];
  memset(get_data_area, 0, length);

  KString file_name = db_path_+ tbl_sub_path_ +
      std::to_string(range_group_id) + "_" + std::to_string(snapshot_info_.id) + ".sqfs";
  std::ifstream get_data_handle;
  // open compressed file and read 'length' bytes data from 'offset'
  get_data_handle.open(file_name.c_str(), std::ios::binary | std::ios::in);
  Defer defer{[&]() { get_data_handle.close(); }};
  get_data_handle.seekg(offset);
  get_data_handle.read(get_data_area, length);
  if (get_data_handle.fail()) {
    LOG_ERROR("Read compressed file failed during GetSnapshotData, file path[%s]", file_name.c_str());
    return KStatus::FAIL;
  }
  LOG_INFO("Read compressed file data range during GetSnapshotData,"
           "snapshot id[%ld], start address[%ld], end address[%ld],"
           "total size=%ld", snapshot_info_.id, offset, offset + length - 1, *total);
  data->len = length;
  data->data = get_data_area;
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::Apply() {
  snapshot_mutex_.lock();
  SubGroupID max_subgroup_id = snapshot_group_->GetSubEntityGroupManager()->GetMaxSubGroupId();
  vector<SubGroupID> subgroups;
  // Allocate all subgroups needed in the target TsEntityGroup
  int ret = entity_group_->GetSubEntityGroupManager()->AllocateNewSubgroups(max_subgroup_id, &subgroups);
  if (ret < 0) {
    LOG_ERROR("AllocateNewSubgroupID failed during apply snapshot table[%ld].",
              entity_group_->HashRange().range_group_id)
    return KStatus::FAIL;
  }

  // Get the number of entities in each subgroup in the snapshot.
  std::vector<size_t> subgroups_entity_num;
  for (int i = 1; i <= max_subgroup_id; ++i) {
    ErrorInfo err_info;
    TsSubEntityGroup* snapshot_subgroup = snapshot_group_->GetSubEntityGroupManager()->GetSubGroup(i, err_info);
    if (!snapshot_subgroup) {
      LOG_ERROR("GetSubGroup[%d] failed during apply snapshot table[%ld].",
                i, entity_group_->HashRange().range_group_id)
      return KStatus::FAIL;
    }
    subgroups_entity_num.push_back(snapshot_subgroup->GetEntities().size());
  }

  SubGroupID snapshot_sub_group_id = 1;
  auto cur_subgroup = subgroups.begin();
  EntityID entity_id = 1;
  // Traverse all tags in the snapshot and apply them to the target subgroup
  auto snapshot_tag_bt = snapshot_group_->GetSubEntityGroupTagbt();
  for (int row_id = 1; row_id <= snapshot_tag_bt->size(); row_id++) {
    // append tag to destination entity group
    KStatus s = applyTagData(row_id, entity_id, *cur_subgroup);
    if (s == KStatus::FAIL) {
      LOG_ERROR("applyTagData failed during apply snapshot table[%ld].",
                entity_group_->HashRange().range_group_id)
      return KStatus::FAIL;
    }
    entity_id++;
    if (entity_id > subgroups_entity_num[snapshot_sub_group_id-1] || row_id == snapshot_tag_bt->size()) {
      // all tags of this subgroup/snapshot have been written, write entity data of this subgroup
      s = applyEntityData(*cur_subgroup, snapshot_sub_group_id);
      if (s == KStatus::FAIL) {
        LOG_ERROR("applyEntityData failed during apply snapshot table[%ld].",
                  entity_group_->HashRange().range_group_id)
        return KStatus::FAIL;
      }
      // change to next subgroup
      cur_subgroup++;
      if (cur_subgroup == subgroups.end()) {
        // it's the last subgroup
        break;
      }
      // refresh entity id
      entity_id = 1;
      snapshot_sub_group_id++;
    }
  }
  snapshot_mutex_.unlock();
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::PrepareCompactData(kwdbContext_p ctx,
                          std::map<SubGroupID, std::map<timestamp64, std::map<uint32_t, BLOCK_ID>>> &obsolete_max_block,
                          std::map<SubGroupID, std::map<timestamp64, std::vector<BLOCK_ID>>> &obsolete_segment_ids,
                          std::map<SubGroupID, std::vector<uint32_t>> &subgroup_row,
                          std::map<SubGroupID, std::vector<KwTsSpan>> &subgroup_ts_span) {
  snapshot_mutex_.lock();
  Defer defer{[&]() {
    snapshot_mutex_.unlock();
  }};

  ErrorInfo err_info;
  KStatus s;
  auto entity_tag_bt = entity_group_->GetSubEntityGroupTagbt();
  if (entity_tag_bt == nullptr) {
    LOG_ERROR("openMMapTagColumnBigTable error : %s" , err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  SubEntityGroupManager* ebt_manager = entity_group_->GetSubEntityGroupManager();
  auto ts_span = KwTsSpan{snapshot_info_.partition_ts.begin/1000, snapshot_info_.partition_ts.end/1000};

  for (size_t row_num = 1; ; row_num++) {
    // Get all tag rows in the range to be reorganized, and find out them belong to which subgroup
    // Don't check row_num in for loop statement!!! Because entity_tag_bt->size() may be changed when processing for loop,
    // and the compiler optimizes the size to the value it was when the for loop statement was first executed.
    if (row_num > entity_tag_bt->size()) {
      break;
    }

    uint64_t tag_hash = TsTable::GetConsistentHashId(reinterpret_cast<char*>(entity_tag_bt->record(row_num)),
                                                     entity_tag_bt->primaryTagSize());
    if (tag_hash < snapshot_info_.begin_hash || tag_hash > snapshot_info_.end_hash) {
      continue;  // not in compaction range
    }
    std::vector<kwdbts::EntityResultIndex> entity_id_list;
    entity_tag_bt->getEntityIdByRownum(row_num, &entity_id_list);
    if (entity_id_list.empty()) {
      LOG_ERROR("getEntityIdByRownum failed during PrepareCompactData");
      return KStatus::FAIL;
    }
    SubGroupID subgroup_id = entity_id_list[0].subGroupId;
    EntityID entity_id = entity_id_list[0].entityId;
    subgroup_row[subgroup_id].push_back(row_num);  // for BuildCompactData

    std::vector<TsTimePartition*> p_bts = ebt_manager->GetPartitionTables(ts_span, subgroup_id, err_info);
    if (err_info.errcode < 0) {
      return KStatus::FAIL;
    }
    for (TsTimePartition *p_bt : p_bts) {
      timestamp64 ts = p_bt->minTimestamp();
      timestamp64 max_ts = p_bt->maxTimestamp();

      // when interval is altered, skip this partition
      if (p_bt->PartitionInterval() != entity_bt_manager_->GetPartitionInterval()) {
        EntityItem *entity = p_bt->getEntityItem(entity_id);
        entity->is_disordered = false;
        ebt_manager->ReleasePartitionTable(p_bt);
        continue;
      }
      // this subgroup + partition is not set status
      if (obsolete_segment_ids[subgroup_id].find(ts) == obsolete_segment_ids[subgroup_id].end()
          && (snapshot_info_.partition_ts.begin <= ts * 1000 && snapshot_info_.partition_ts.end >= max_ts * 1000)
          && p_bt->NeedCompaction(entity_id)) {
        p_bt->CompactingStatus(obsolete_max_block[subgroup_id][ts], obsolete_segment_ids[subgroup_id][ts]);
        if (obsolete_max_block[subgroup_id][ts].empty()) {
          obsolete_max_block[subgroup_id].erase(ts);
        }
        if (obsolete_segment_ids[subgroup_id][ts].empty()) {
          obsolete_segment_ids[subgroup_id].erase(ts);
        } else {
          subgroup_ts_span[subgroup_id].push_back(KwTsSpan {ts * 1000, max_ts * 1000});
        }
      }
      ebt_manager->ReleasePartitionTable(p_bt);
    }

    if (obsolete_segment_ids[subgroup_id].empty()) {
      obsolete_segment_ids.erase(subgroup_id);
    }
    if (obsolete_max_block[subgroup_id].empty()) {
      obsolete_max_block.erase(subgroup_id);
    }

    // tag row is invalid, continue
  }
  return KStatus::SUCCESS;
}

// refer to BuildSnapshot
KStatus TsTableSnapshot::BuildCompactData(kwdbContext_p ctx, std::vector<uint32_t> &tag_rows,
                                          std::vector<KwTsSpan> &partition_ts) {
  snapshot_mutex_.lock();
  for (unsigned int row_num : tag_rows) {
    // Split ts_span, data reorganization takes place only within one time partition
    for (KwTsSpan &ts_span : partition_ts) {
      KStatus s = genMigratePayloadByBuilder(ctx, row_num, ts_span);
      if (s == KStatus::FAIL) {
        LOG_ERROR("Compaction failed, range_group_id=%lu", snapshot_group_->HashRange().range_group_id);
        snapshot_mutex_.unlock();
        return s;
      }
    }
  }
  snapshot_mutex_.unlock();
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::ApplyCompactData(kwdbContext_p ctx, SubGroupID entity_group_subgroup_id,
                                          std::map<timestamp64, std::map<uint32_t, BLOCK_ID>> &obsolete_max_block,
                                          std::map<timestamp64, std::vector<BLOCK_ID>> &obsolete_segment_ids) {
  if (!snapshot_info_.reorder) {
    return KStatus::FAIL;
  }

  KStatus s;
  ErrorInfo err_info;
  snapshot_mutex_.lock();
  Defer defer{[&]() {
    snapshot_mutex_.unlock();
  }};

  // map[partition_ts][entity_id of source table] {blockItems}
  std::map<timestamp64, std::map<uint32_t, std::deque<BlockItem*>>> compacted_block_items;

  KwTsSpan ts_span = KwTsSpan{snapshot_info_.partition_ts.begin/1000, snapshot_info_.partition_ts.end/1000};
  SubEntityGroupManager *snapshot_manager = snapshot_group_->GetSubEntityGroupManager();

  auto snapshot_tag_bt = snapshot_group_->GetSubEntityGroupTagbt();
  SubGroupID snapshot_subgroup_id = 0;
  // Get block items from snapshot, and use entity_id from source table as key in compacted_block_items
  for (size_t row_num = 1; row_num <= snapshot_tag_bt->size(); row_num++) {
    TSSlice primary_tag {reinterpret_cast<char*>(snapshot_tag_bt->record(row_num)), snapshot_tag_bt->primaryTagSize()};
    EntityID snapshot_entity_id = 0;
    if (snapshot_tag_bt->getEntityIdGroupId(primary_tag.data, primary_tag.len,
                                            snapshot_entity_id, snapshot_subgroup_id) < 0) {
      return KStatus::FAIL;
    }

    EntityID entity_id = 0;
    SubGroupID subgroup_id = 0;  // retrieved subgroup_id should be equal to entity_group_subgroup_id
    if (entity_group_->GetSubEntityGroupTagbt()->getEntityIdGroupId(primary_tag.data, primary_tag.len,
                                                                    entity_id, subgroup_id) < 0) {
      continue;  // this tag could be deleted
    }

    std::vector<TsTimePartition*> p_bts = snapshot_manager->GetPartitionTables(ts_span, snapshot_subgroup_id, err_info);
    for (auto p_bt : p_bts) {
      std::deque<BlockItem*> block_item_queue;
      p_bt->GetAllBlockItems(snapshot_entity_id, block_item_queue);
      if (!block_item_queue.empty()) {
        compacted_block_items[p_bt->minTimestamp()][entity_id] = block_item_queue;
      }
      ReleaseTable(p_bt);
    }
  }

  string entity_group_dir = db_path_ + std::to_string(table_id_) + "/"
                            + std::to_string(entity_group_->HashRange().range_group_id);

  string snapshot_dir = entity_group_dir + "_" + std::to_string(snapshot_info_.id);

  string snapshot_part_dir = snapshot_dir + "/" + std::to_string(table_id_) + "_" + std::to_string(snapshot_subgroup_id);

  TsSubEntityGroup *subgroup = entity_group_->GetSubEntityGroupManager()->GetSubGroup(entity_group_subgroup_id, err_info);
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  if (subgroup->ApplyCompactData(obsolete_max_block, obsolete_segment_ids, compacted_block_items, snapshot_part_dir) < 0) {
    LOG_ERROR("ApplyCompactData fail, snapshot subgroup %u", snapshot_subgroup_id);
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::DropAll() {
  KString dir_path = db_path_ + tbl_sub_path_ + to_string(snapshot_group_->HashRange().range_group_id) + "_" +
                     to_string(snapshot_info_.id);


#ifndef WITH_TESTS
  dir_path += " " + dir_path + ".sqfs";
#endif
  KString cmd = "rm -rf " + dir_path;
  if (!System(cmd)) {
    LOG_ERROR("Delete snapshot table file failed, file path[%s]", dir_path.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::getMigratedTagRows() {
  ErrorInfo err_info;
  tag_row_nums_.clear();  // clear it before push_back
  // get tag table from original ts table
  auto entity_tag_bt = entity_group_->GetSubEntityGroupTagbt();
  if (entity_tag_bt == nullptr) {
    LOG_ERROR("Get tag table from TsSubEntityGroup failed, error: %s" , err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  size_t row_num = 1;
  // traverse the tag table
  entity_tag_bt->startRead();
  for (; row_num <= entity_tag_bt->size(); row_num++) {
    if (!entity_tag_bt->isValidRow(row_num)) {
      // invalid row
      continue;
    }
    // get consistent hash id of this tag row
    uint64_t tag_hash = TsTable::GetConsistentHashId(reinterpret_cast<char*>(entity_tag_bt->record(row_num)),
                                                     entity_tag_bt->primaryTagSize());
    if (tag_hash >= snapshot_info_.begin_hash && tag_hash <= snapshot_info_.end_hash) {
      // if hash id is within the hash range, record it
      tag_row_nums_.push_back(row_num);
    }
  }
  entity_tag_bt->stopRead();
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::applyTagData(int row_id, const EntityID& entity_id, const SubGroupID& subgroup_id) {
  ErrorInfo err_info;
  std::vector<TagColumn*> tag_attribute_info = entity_group_->GetSubEntityGroupTagbt()->getSchemaInfo();
  vector<AttributeInfo> metrics_attribute_info = entity_bt_manager_->GetSchemaInfoWithoutHidden();

  ResultSet tag_res{(k_uint32)tag_attribute_info.size()};
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_attribute_info.size(); i++) {
    scan_tags.push_back(i);
  }
  snapshot_group_->GetSubEntityGroupTagbt()->getColumnsByRownum(row_id, scan_tags, &tag_res);
  // Use PayloadBuilder to build a payload, iterate through the tag attribute information,
  // obtain the corresponding values according to different tag types, and set them to the payload.
  PayloadBuilder pl_builder(tag_attribute_info, metrics_attribute_info);
  for (size_t i = 0; i < tag_attribute_info.size(); i++) {
    const Batch* tag_col_batch = tag_res.data[i].at(0);
    TagColumn* tag_schema_ptr = tag_attribute_info[i];
    char* value_addr = nullptr;
    int value_len = 0;
    // Different tags are stored differently in payload
    if (tag_schema_ptr->isPrimaryTag()) {
      value_addr = reinterpret_cast<char*>(tag_col_batch->mem);
      value_len = tag_schema_ptr->attributeInfo().m_size;
    } else {
      if (tag_schema_ptr->isVarTag()) {
        value_addr = reinterpret_cast<char*>(tag_col_batch->getVarColData(0));
        value_len = tag_col_batch->getVarColDataLen(0);
      } else {
        value_addr = reinterpret_cast<char*>(tag_col_batch->mem) + 1;
        value_len = tag_schema_ptr->attributeInfo().m_size;
      }
    }
    if (*reinterpret_cast<char*>(tag_col_batch->mem) == 0x01 || tag_schema_ptr->isPrimaryTag()) {  // null bitmap.
      pl_builder.SetTagValue(i, value_addr, value_len);
    }
  }
  tag_res.clear();

  const char *tag_addr = pl_builder.GetTagAddr();
  // Call the insert function to insert the tag data
  err_info.errcode = entity_group_->GetSubEntityGroupTagbt()->insert(entity_id, subgroup_id, tag_addr);
  if (err_info.errcode < 0) {
    LOG_ERROR("Insert tag data failed during applyTagData, range_group_id=%ld, snapshot_id=%ld.",
              entity_group_->HashRange().range_group_id, snapshot_info_.id)
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::applyEntityData(uint32_t& subgroup_id, SubGroupID& snapshot_sub_group_id) {
  // construct destination subgroup path.
  string dir_path = db_path_ + tbl_sub_path_ + std::to_string(entity_group_->HashRange().range_group_id) + "/" +
                    std::to_string(table_id_) + "_" + std::to_string(subgroup_id);

  // construct snapshot subgroup path.
  string snapshot_dir_path = db_path_ + tbl_sub_path_ + std::to_string(entity_group_->HashRange().range_group_id) + "_" +
                             std::to_string(snapshot_info_.id) + "/" + std::to_string(table_id_) + "_" +
                             std::to_string(snapshot_sub_group_id);
  // execute system command to copy data.
  std::string cp_cmd = "cp -r " + snapshot_dir_path + "/* " + dir_path;
  if (!System(cp_cmd)) {
    LOG_ERROR("Copy data from snapshot to dest failed, command[%s]", cp_cmd.c_str());
    return KStatus::FAIL;
  }

  DIR* dir_ptr = opendir(snapshot_dir_path.c_str());
  if (dir_ptr == nullptr) {
    LOG_ERROR("Open snapshot path failed, path[%s]", snapshot_dir_path.c_str());
    return KStatus::FAIL;
  }

  // refresh the memory records of the destination group
  ErrorInfo err_info;
  auto subgroup = entity_group_->GetSubEntityGroupManager()->GetSubGroup(subgroup_id, err_info);
  int result = subgroup->ReOpenInit(err_info);
  if (result < 0) {
    LOG_ERROR("ReOpen subgroup failed.");
    return KStatus::FAIL;
  }
  auto snapshot_subgroup = snapshot_group_->GetSubEntityGroupManager()->GetSubGroup(snapshot_sub_group_id, err_info);
  struct dirent* entry;
  while ((entry = readdir(dir_ptr)) != nullptr) {
    if (entry->d_type == DT_DIR) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_' || strcmp(entry->d_name, "wal") == 0) {  // directory: _log, _tmp, wal
        continue;
      }
      // By calling the GetPartitionTable, the subgroup is recorded in the memory of the entity group
      string partition_dir = entry->d_name;
      timestamp64 ts = convertToTimestamp(partition_dir);
      TsTimePartition* p_bt = subgroup->GetPartitionTable(ts, err_info);
      ReleaseTable(p_bt);

      TsTimePartition* snapshot_partition_bt = snapshot_subgroup->GetPartitionTable(ts, err_info);
      ReleaseTable(snapshot_partition_bt);

      // update root table's partition interval
      entity_bt_manager_->SetPartitionInterval(p_bt->PartitionInterval());
    }
  }
  closedir(dir_ptr);
  return KStatus::SUCCESS;
}

KStatus TsTableSnapshot::genMigratePayloadByBuilder(kwdbContext_p ctx, uint32_t row_id, KwTsSpan ts_span) {
  ErrorInfo err_info;
  std::vector<kwdbts::EntityResultIndex> entity_id_list;
  // Get the entity information corresponding to the primary tag of the specified row_id
  auto entity_tag_bt = entity_group_->GetSubEntityGroupTagbt();
  entity_tag_bt->getEntityIdByRownum(row_id, &entity_id_list);
  if (entity_id_list.empty()) {
    LOG_ERROR("Get entity id failed during build snapshot. snapshot id[%lu]", snapshot_info_.id);
    return KStatus::FAIL;
  }

  std::vector<k_uint32> scan_cols;
  uint32_t table_version = entity_bt_manager_->GetCurrentTableVersion();
  vector<AttributeInfo> metrics_attribute_info = entity_bt_manager_->GetSchemaInfoWithoutHidden(table_version);
  k_uint32 num_col = metrics_attribute_info.size();
  auto actual_cols = entity_bt_manager_->GetColsIdx(table_version);
  std::vector<k_uint32> ts_scan_cols;
  for (int i = 0; i < num_col; i++) {
    scan_cols.push_back(i);
    ts_scan_cols.emplace_back(actual_cols[i]);
  }

  // scan_agg_types should be updated when a new aggregate function is supported
  std::vector<Sumfunctype> scan_agg_types;
  TsIterator* iter;
  // use iterator to read data from source entity group
  KStatus s = entity_group_->GetIterator(ctx, entity_id_list[0].subGroupId, {entity_id_list[0].entityId},
                                         {ts_span}, scan_cols, ts_scan_cols, scan_agg_types,  table_version, &iter,
                                         entity_group_, snapshot_info_.reorder, snapshot_info_.reorder);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetIterator failed during build snapshot, subgroup_id[%u], entity_id[%u], snapshot_id[%lu]",
              entity_id_list[0].subGroupId, entity_id_list[0].entityId, snapshot_info_.id);
    return s;
  }
  Defer defer{[&]() {
    if (iter) {
      delete iter;
      iter = nullptr;
    }
  }};

  k_uint32 count = 0;
  ResultSet res{num_col};
  bool is_finished = false;
  // read data to be migrated
  s = iter->Next(&res, &count, &is_finished);
  if (s == KStatus::FAIL || is_finished) {
    LOG_ERROR("GetTagIterator next failed during build snapshot, subgroup_id[%u], entity_id[%u]",
              entity_id_list[0].subGroupId, entity_id_list[0].entityId);
    return s;
  }

  std::vector<TagColumn*> tag_attribute_info = entity_tag_bt->getSchemaInfo();
  // tag values
  ResultSet tag_res{(k_uint32)tag_attribute_info.size()};
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tag_attribute_info.size(); i++) {
    scan_tags.push_back(i);
  }
  if (snapshot_info_.type == 0) {
    // type = 0, create snapshot, get from source entity group
    entity_tag_bt->getColumnsByRownum(row_id, scan_tags, &tag_res);
  } else {
    LOG_ERROR("The function is called incorrectly, couldn't call this function when applying snapshot.");
    return KStatus::FAIL;
  }

  while (!is_finished) {
    // get batch numbers from this result
    uint32_t batch_num = res.data[0].size();
    for (int batch_index = 0; batch_index < batch_num; batch_index++) {
      PayloadBuilder pl_builder(tag_attribute_info, metrics_attribute_info);
      // first set tag value into payload builder
      for (size_t i = 0; i < tag_attribute_info.size(); i++) {
        // Get the address and length of the tag value
        const Batch* tag_col_batch = tag_res.data[i].at(0);
        TagColumn* tag_schema_ptr = tag_attribute_info[i];
        char* value_addr = nullptr;
        int value_len = 0;
        if (tag_schema_ptr->isPrimaryTag()) {
          value_addr = reinterpret_cast<char*>(tag_col_batch->mem);
          value_len = tag_schema_ptr->attributeInfo().m_size;
        } else {
          if (tag_schema_ptr->isVarTag()) {
            value_addr = reinterpret_cast<char*>(tag_col_batch->getVarColData(0));
            value_len = tag_col_batch->getVarColDataLen(0);
          } else {
            value_addr = reinterpret_cast<char*>(tag_col_batch->mem) + 1;
            value_len = tag_schema_ptr->attributeInfo().m_size;
          }
        }
        if (*reinterpret_cast<char*>(tag_col_batch->mem) == 0x01 || tag_schema_ptr->isPrimaryTag()) {  // null bitmap.
          // Set value to the pl_builder
          pl_builder.SetTagValue(i, value_addr, value_len);
        }
      }

      uint32_t batch_data_count = res.data[0][batch_index]->count;
      pl_builder.SetDataRows(batch_data_count);

      // Set the metrics data to payload builder
      for (size_t column = 0; column < metrics_attribute_info.size(); ++column) {
        auto batch = res.data[column][batch_index];
        for (int batch_data_index = 0; batch_data_index < batch->count; batch_data_index++) {
          bool is_null = false;
          batch->isNull(batch_data_index, &is_null);
          if (is_null) {
            LOG_INFO("The data is null at column[%ld:%s] batch[%d]",
                     column, metrics_attribute_info[column].name, batch_data_index);
            pl_builder.SetColumnNull(batch_data_index, column);
            continue;
          }
          char* value_addr = nullptr;
          int value_len = 0;
          if (metrics_attribute_info[column].type == VARSTRING || metrics_attribute_info[column].type == VARBINARY) {
            // Variable-length type
            value_addr = reinterpret_cast<char*>(batch->getVarColData(batch_data_index));
            value_len = batch->getVarColDataLen(batch_data_index);
            if (metrics_attribute_info[column].type == VARSTRING && value_len > 0) {
              value_len -= 1;
            }
          } else {
            value_len = metrics_attribute_info[column].size;
            value_addr = reinterpret_cast<char*>(batch->mem) + value_len * batch_data_index;
          }
          pl_builder.SetColumnValue(batch_data_index, column, value_addr, value_len);
        }
      }
      // construct payload
      TSSlice payload_data;
      if (!pl_builder.Build(&payload_data, table_version)) {
        LOG_ERROR("Payload build failed when build snapshot, snapshot id[%lu].", snapshot_info_.id);
        return KStatus::FAIL;
      }
      s = snapshot_group_->PutData(ctx, payload_data);
      if (s == KStatus::FAIL) {
        LOG_ERROR("PutData failed during genMigratePayloadData, range_group_id=%lu",
                  snapshot_group_->HashRange().range_group_id);
        return KStatus::FAIL;
      }
      delete[] payload_data.data;
    }
    // this batch is finished
    res.clear();
    s = iter->Next(&res, &count, &is_finished);
    if (s == KStatus::FAIL) {
      LOG_ERROR("GetTsIterator next failed during build snapshot, subgroup_id=%u, entity_id=%u",
                entity_id_list[0].subGroupId, entity_id_list[0].entityId);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
