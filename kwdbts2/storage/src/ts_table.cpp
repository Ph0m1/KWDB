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
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include "engine.h"
#include "mmap/mmap_metrics_table.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_snapshot.h"
#include "ts_table.h"
#include "perf_stat.h"
#include "sys_utils.h"
#include "lt_cond.h"
#include "ee_global.h"

extern DedupRule g_dedup_rule;
namespace kwdbts {

TsEntityGroup::TsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_table_manager, const string& db_path,
                             const KTableKey& table_id, const RangeGroup& range, const string& tbl_sub_path) :
    root_bt_manager_(root_table_manager), db_path_(db_path), table_id_(table_id),
    range_(range), tbl_sub_path_(tbl_sub_path) {
  cur_subgroup_id_ = 1;
  mutex_ = new KLatch(LATCH_ID_TSENTITY_GROUP_MUTEX);
  drop_mutex_ = new KRWLatch(RWLATCH_ID_TS_ENTITY_GROUP_DROP_RWLOCK);
}

TsEntityGroup::~TsEntityGroup() {
  if (ebt_manager_ != nullptr) {
    delete ebt_manager_;
    ebt_manager_ = nullptr;
  }
  if (tag_bt_) {
    delete tag_bt_;
  }
  if (mutex_) {
    delete mutex_;
    mutex_ = nullptr;
  }
  if (drop_mutex_) {
    delete drop_mutex_;
    drop_mutex_ = nullptr;
  }
}

KStatus TsEntityGroup::Create(kwdbContext_p ctx, vector<TagInfo>& tag_schema) {
  if (ebt_manager_ != nullptr) {
    LOG_ERROR("TsTableRange already OpenInit.")
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  // TODO(jiadx): create individual Tag table under the range directory
  MUTEX_LOCK(mutex_);
  tag_bt_ = CreateTagTable(tag_schema, db_path_, tbl_sub_path_, table_id_, range_.range_group_id, TAG_TABLE, err_info);
  MUTEX_UNLOCK(mutex_);
  if (tag_bt_ == nullptr) {
    LOG_ERROR("TsTableRange create error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  ebt_manager_ = new SubEntityGroupManager(root_bt_manager_);
  ebt_manager_->OpenInit(db_path_, tbl_sub_path_, table_id_, err_info);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::OpenInit(kwdbContext_p ctx) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  if (ebt_manager_ != nullptr) {
    LOG_ERROR("TsTableRange already OpenInit.")
    return KStatus::FAIL;
  }

  ErrorInfo err_info;
  // Open Tag table under the range directory
  tag_bt_ = OpenTagTable(db_path_, tbl_sub_path_, table_id_, err_info);
  if (err_info.errcode < 0 || tag_bt_ == nullptr) {
    LOG_ERROR("TsTableRange OpenInit error :%s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  ebt_manager_ = new SubEntityGroupManager(root_bt_manager_);
  ebt_manager_->OpenInit(db_path_, tbl_sub_path_, table_id_, err_info);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::PutEntity(kwdbContext_p ctx, TSSlice& payload, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  KStatus status = SUCCESS;
  ErrorInfo err_info;
  Payload pd(root_bt_manager_, payload);
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  // 1. query tag table
  TSSlice tmp_slice = pd.GetPrimaryTag();
  uint32_t entity_id, subgroup_id;
  if (tag_bt_->getEntityIdGroupId(tmp_slice.data, tmp_slice.len, entity_id, subgroup_id) == 0) {
    // update
    if (tag_bt_->UpdateTagRecord(pd, subgroup_id, entity_id, err_info) < 0) {
      LOG_ERROR("Update tag record failed. error: %s ", err_info.errmsg.c_str());
      releaseTagTable();
      return KStatus::FAIL;
    }
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice* payloads, int length, uint64_t mtr_id,
                               DedupResult* dedup_result, DedupRule dedup_rule) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  for (int i = 0; i < length; i++) {
    // Based on the number of payloads, call PutData repeatedly to write data
    KStatus s = PutData(ctx, payloads[i], 0, dedup_result, dedup_rule);
    if (s == FAIL) return s;
  }
  return SUCCESS;
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice payload) {
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  return PutData(ctx, payload, 0, &dedup_result, g_dedup_rule);
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id,
                               DedupResult* dedup_result, DedupRule dedup_rule) {
  return PutDataWithoutWAL(ctx, payload, mini_trans_id, dedup_result, dedup_rule);
}

KStatus TsEntityGroup::PutDataWithoutWAL(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id,
                               DedupResult* dedup_result, DedupRule dedup_rule) {
  // If wal is not enabled, this function will be used.
  KStatus status = SUCCESS;
  uint32_t group_id, entity_id;
  ErrorInfo err_info;
  group_id = 1;
  entity_id = 1;
  // Create a Payload object pd to prepare for subsequent writing.
  // The payload object includes both the tag and the payload data.
  // The data to be written can be retrieved using getColumnAddr() and getVarColumnAddr() methods.
  Payload pd(root_bt_manager_, payload);
  pd.dedup_rule_ = dedup_rule;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  Defer defer{[&]() {
    releaseTagTable();
  }};
  // check if lsn is set, in wal-off mode, lsn will not set, we should set lsn to 1 for marking lsn exist.
  TS_LSN pl_lsn;
  if (pd.GetLsn(pl_lsn)) {
    if (pl_lsn == 0) {
      pd.SetLsn(1);
    }
  }
  LOG_DEBUG("PutDataWithoutWAL write rows: %d.", pd.GetRowCount());
  // Query or assign EntityGroupID and EntityID based on the provided payload.
  // Initially, attempt to retrieve the IDs directly from the tag table.
  // If it does not exist, allocate and insert it into the tag table.
  bool new_one;
  if (KStatus::SUCCESS != allocateEntityGroupId(ctx, pd, &entity_id, &group_id, &new_one)) {
    LOG_ERROR("allocateEntityGroupId failed, entity id: %d, group id: %d.", entity_id, group_id);
    return KStatus::FAIL;
  }
  if (pd.GetFlag() == Payload::TAG_ONLY) {
    // Only when a tag is present, do not write data
    return KStatus::SUCCESS;
  }
  if (pd.GetRowCount() > 0) {
    return putDataColumnar(ctx, group_id, entity_id, pd, dedup_result);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::putTagData(kwdbContext_p ctx, int32_t groupid, int32_t entity_id, Payload& payload) {
  KWDB_DURATION(StStatistics::Get().put_tag);
  ErrorInfo err_info;
  // 1. Write tag data
  uint8_t payload_data_flag = payload.GetFlag();
  if (payload_data_flag == Payload::DATA_AND_TAG || payload_data_flag == Payload::TAG_ONLY) {
    // tag
    LOG_DEBUG("tag bt insert hashPoint=%d", payload.getHashPoint());
    err_info.errcode = tag_bt_->insert(entity_id, groupid, payload.getHashPoint(), payload.GetTagAddr());
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::putTagDataHashed(kwdbContext_p ctx, int32_t groupid, int32_t entity_id,
                                        uint32_t hashpoint, Payload& payload) {
  KWDB_DURATION(StStatistics::Get().put_tag);
  ErrorInfo err_info;
  // 1.Write tag data
  uint8_t payload_data_flag = payload.GetFlag();
  if (payload_data_flag == Payload::DATA_AND_TAG || payload_data_flag == Payload::TAG_ONLY) {
    // tag
    err_info.errcode = tag_bt_->insert(entity_id, groupid, payload.getHashPoint(), payload.GetTagAddr());
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetAllPartitions(kwdbContext_p ctx, std::unordered_map<SubGroupID,
                                        std::vector<timestamp64>>* subgrp_partitions) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scantags;
  std::vector<EntityResultIndex> entity_list;
  ResultSet res;
  uint32_t count;
  KStatus s = GetEntityIdList(ctx, primary_tags, scantags, &entity_list , &res, &count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAllPartitionDirs failed at GetEntityIdList.");
    return s;
  }
  KwTsSpan span{INT64_MIN, INT64_MAX};
  ErrorInfo err_info;
  for (auto& entity_index : entity_list) {
    if (entity_index.entityGroupId != range_.range_group_id) {
      // just filter this entitygroup only
      continue;
    }
    if (subgrp_partitions->find(entity_index.subGroupId) != subgrp_partitions->end()) {
      // this subgroup and its parttions already inserted.
      continue;
    }
    const auto& p_bts = ebt_manager_->GetPartitions(span, entity_index.subGroupId, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTables failed.");
      return KStatus::FAIL;
    }
    (*subgrp_partitions)[entity_index.subGroupId] = p_bts;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::allocateEntityGroupId(kwdbContext_p ctx, Payload& payload, uint32_t* entity_id,
                                            uint32_t* group_id, bool* new_tag) {
  *new_tag = false;
  KWDB_DURATION(StStatistics::Get().alloc_entity);
  ErrorInfo err_info;
  // Attempting to retrieve the group ID and entity ID from the tag table
  // 1.If found, assign a value and return
  // 2.If not found, attempt to lock and query again to prevent concurrent scenarios where the first query fails and
  // the entity is created successfully.
  // 3.If the second search fails, it indicates that no other thread has created the entity. Therefore, use the
  // consistent hashID algorithm to allocate a new ID and write it to the tag table.
  TSSlice tmp_slice = payload.GetPrimaryTag();
  uint32_t entityid, groupid;
  if (tag_bt_->getEntityIdGroupId(tmp_slice.data, tmp_slice.len, entityid, groupid) == 0) {
    *entity_id = entityid;
    *group_id = groupid;
    return KStatus::SUCCESS;
  }
  {
    // Locking, concurrency control, and the putTagData operation must not be executed repeatedly
    MUTEX_LOCK(mutex_);
    Defer defer{[&]() { MUTEX_UNLOCK(mutex_); }};
    if (tag_bt_->getEntityIdGroupId(tmp_slice.data, tmp_slice.len, entityid, groupid) == 0) {
      *entity_id = entityid;
      *group_id = groupid;
      return KStatus::SUCCESS;
    }
    // not found
    std::string tmp_str = std::to_string(table_id_);
    uint64_t tag_hash = TsTable::GetConsistentHashId(tmp_str.data(), tmp_str.size());
    std::string primary_tags;
    err_info.errcode = ebt_manager_->AllocateEntity(primary_tags, tag_hash, &groupid, &entityid);
    if (err_info.errcode < 0) {
      return KStatus::FAIL;
    }
    // insert tag table
    if (KStatus::SUCCESS != putTagData(ctx, groupid, entityid, payload)) {
      return KStatus::FAIL;
    }
  }
  *new_tag = true;
  *entity_id = entityid;
  *group_id = groupid;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::putDataColumnar(kwdbContext_p ctx, int32_t group_id, int32_t entity_id,
                                       Payload& payload, DedupResult* dedup_result) {
  KWDB_DURATION(StStatistics::Get().put_data);
  KWDB_STAT_ADD(StStatistics::Get().payload_row, payload.GetRowCount());
  ErrorInfo err_info;

  // only used for import data, other scenario will not use discard mode.
  // When importing data, it is necessary to allocate space for bitmap to record discarded rows.
  if (payload.dedup_rule_ == DedupRule::DISCARD
      && dedup_result->payload_num == 1
      && dedup_result->discard_bitmap.data == nullptr) {
    dedup_result->discard_bitmap.len = (payload.GetRowCount() + 7) / 8;
    dedup_result->discard_bitmap.data = reinterpret_cast<char*>(malloc(dedup_result->discard_bitmap.len));
    memset(dedup_result->discard_bitmap.data, 0, dedup_result->discard_bitmap.len);
  }

  // Write data column by column, employing a columnar storage format. Each data column is accompanied by an
  // independent mmap file,
  // necessitating the updating of multiple column files during data writes.
  // After parsing the INSERT statement, the execution module constructs a payload that is stored in a columnar
  // format in memory, which is identical to the physical storage structure.
  // To enhance data writing performance, batch data writing can utilize memory copying methods directly,
  // based on the data column.
  timestamp64 last_p_time = INVALID_TS;
  TsTimePartition* p_bt = nullptr;
  int batch_start = payload.GetStartRowId();
  timestamp64 p_time = INVALID_TS;

  bool all_success = true;
  std::vector<BlockSpan> alloc_spans;
  TsSubEntityGroup* sub_group = ebt_manager_->GetSubGroup(group_id, err_info, false);
  if (err_info.errcode < 0) {
    LOG_ERROR("push_back_payload error: there is no subgroup: %d", group_id);
    return KStatus::FAIL;
  }

  unordered_map<TsTimePartition*, PutAfterProcessInfo*> after_process_info;
  bool payload_dup = false;
  std::unordered_set<timestamp64> dup_set;

  // Examine the timestamp of the first piece of data within the payload to verify the partition being written to
  KTimestamp first_ts_ms = payload.GetTimestamp(payload.GetStartRowId());
  KTimestamp first_ts = first_ts_ms / 1000;
  timestamp64 first_max_ts;
  uint32_t hash_point = payload.getHashPoint();
  last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);
  k_int32 row_id = 0;
  LOG_DEBUG("Insert primaryTag is %d ", *(payload.GetPrimaryTag().data));
  // Based on the obtained partition timestamp, determine whether the current payload needs to switch partitions.
  // If necessary, first call push_back_payload to write the current partition data.
  // Then continue to call payloadNextSlice until the traversal of the Payload data is complete.
  while (payloadNextSlice(sub_group, payload, last_p_time, batch_start, &row_id, &p_time)) {
    p_bt = ebt_manager_->GetPartitionTable(last_p_time, group_id, err_info, true);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTable error: %s", err_info.errmsg.c_str());
      all_success = false;
      ebt_manager_->ReleasePartitionTable(p_bt);
      break;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition is invalid.");
      err_info.setError(KWEDUPREJECT, "Partition is invalid.");
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    last_p_time = p_time;

    std::vector<BlockSpan> cur_alloc_spans;
    std::vector<MetricRowID> to_deleted_real_rows;

    // push_back_payload is used to allocate space, write data to the partition table, and perform data deduplication
    // and aggregation during this process.
    // The returned cur_alloc_spans contains records of all allocated blocks
    // and the number of rows written to each block.
    // If there is a failure, putAfterProcess will remove the spatial marker for cur_alloc_spans.
    // All rows that need to be deleted (deduplicated) are recorded in the returned to_deleted_real_rows,
    // and recordPutAfterProInfo will process them.
    err_info.errcode = p_bt->push_back_payload(ctx, entity_id, &payload, batch_start, row_id - batch_start,
                                               &cur_alloc_spans, &to_deleted_real_rows, err_info, dedup_result);

    // After handling a partition, record the information to be processed subsequently
    recordPutAfterProInfo(after_process_info, p_bt, cur_alloc_spans, to_deleted_real_rows);

    if (err_info.errcode < 0) {
      LOG_ERROR("push_back_payload error: %s", err_info.errmsg.c_str());
      all_success = false;
      break;
    }
    batch_start = row_id;
  }

  // Writing completed, processing after_process_info recorded earlier
  putAfterProcess(after_process_info, entity_id, all_success);

  if (!all_success) {
    if (err_info.errcode == KWEDUPREJECT) {
      return KStatus::SUCCESS;
    }
    return KStatus::FAIL;
  }
  LOG_DEBUG("table[%s] input data num[%d].", this->tbl_sub_path_.c_str(), payload.GetRowCount());
  return KStatus::SUCCESS;
}

void TsEntityGroup::recordPutAfterProInfo(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                                          TsTimePartition* p_bt, std::vector<BlockSpan>& cur_alloc_spans,
                                          std::vector<MetricRowID>& to_deleted_real_rows) {
  if (after_process_info.find(p_bt) != after_process_info.end()) {
    ebt_manager_->ReleasePartitionTable(p_bt);
  } else {
    after_process_info[p_bt] = new PutAfterProcessInfo();
  }

  // Update the partition data
  after_process_info[p_bt]->spans.insert(after_process_info[p_bt]->spans.end(),
                                         cur_alloc_spans.begin(), cur_alloc_spans.end());
  after_process_info[p_bt]->del_real_rows.insert(after_process_info[p_bt]->del_real_rows.end(),
                                                 to_deleted_real_rows.begin(), to_deleted_real_rows.end());
}

void TsEntityGroup::putAfterProcess(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                                    uint32_t entity_id, bool all_success) {
  for (auto iter : after_process_info) {
    iter.first->publish_payload_space((iter.second)->spans, (iter.second)->del_real_rows, entity_id, all_success);
    ebt_manager_->ReleasePartitionTable(iter.first);
    delete iter.second;
  }

  after_process_info.clear();
}

KStatus TsEntityGroup::DeleteRangeData(kwdbContext_p ctx, const HashIdSpan& hash_span, TS_LSN lsn,
                                       const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpans>* del_rows,
                                       uint64_t* count, uint64_t mtr_id, bool evaluate_del) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  // Based on the hash ID range, find all the rows of tag data that need to be deleted.
  vector<string> primary_tags;
  tag_bt_->startRead();
  for (size_t row = 1; row <= tag_bt_->size(); row++) {
    if (!tag_bt_->isValidRow(row)) {
      continue;
    }
    string primary_tag(reinterpret_cast<char*>(tag_bt_->record(row)), tag_bt_->primaryTagSize());
    uint64_t hash_id = TsTable::GetConsistentHashId(primary_tag.data(), primary_tag.size());
    if (hash_id >= hash_span.begin && hash_id <= hash_span.end) {
      primary_tags.emplace_back(primary_tag);
    }
  }
  tag_bt_->stopRead();
  *count = 0;
  for (const auto& p_tags : primary_tags) {
    // Delete the data corresponding to the tag within the time range
    uint64_t entity_del_count = 0;
    DelRowSpans del_row_spans{p_tags};
    KStatus status = DeleteData(ctx, p_tags, lsn, ts_spans, &del_row_spans.spans, &entity_del_count, mtr_id, evaluate_del);
    if (status == KStatus::FAIL) {
      LOG_ERROR("DeleteRangeData failed, delete entity by primary key %s failed", p_tags.c_str());
      return KStatus::FAIL;
    }
    // Update count
    *count += entity_del_count;
    if (!del_row_spans.spans.empty() && evaluate_del) {
      del_rows->push_back(del_row_spans);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteData(kwdbContext_p ctx, const string& primary_tag, TS_LSN lsn,
                                  const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>* rows,
                                  uint64_t* count, uint64_t mtr_id, bool evaluate_del) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  *count = 0;
  uint32_t entity_id = 0;
  uint32_t subgroup_id = 0;
  // Get subgroup id and entity id based on the primary tag.
  int ret = tag_bt_->getEntityIdGroupId(const_cast<char*>(primary_tag.c_str()),
                                        primary_tag.size(), entity_id, subgroup_id);
  if (ret < 0) {
    LOG_WARN("entity not exists, primary_tag: %s", primary_tag.c_str());
    return KStatus::SUCCESS;
  }
  timestamp64 min_ts = INT64_MAX, max_ts = INT64_MIN;
  for (auto span : ts_spans) {
    min_ts = (min_ts > span.begin) ? span.begin : min_ts;
    max_ts = (max_ts < span.end) ? span.end : max_ts;
  }
  ErrorInfo err_info;
  vector<TsTimePartition*> p_tables =
      ebt_manager_->GetPartitionTables({min_ts / 1000, max_ts / 1000}, subgroup_id, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("GetPartitionTable error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  bool del_failed = false;
  for (TsTimePartition* p_bt : p_tables) {
    if (del_failed) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    vector<DelRowSpan> delete_rows;
    uint64_t cnt = 0;
    int res = 0;
    if (evaluate_del) {
      res = p_bt->DeleteData(entity_id, 0, ts_spans, &delete_rows, &cnt, err_info, evaluate_del);
    } else {
      res = p_bt->DeleteData(entity_id, 0, ts_spans, nullptr, &cnt, err_info, evaluate_del);
    }
    if (res < 0) {
      del_failed = true;
    }
    if (rows && evaluate_del) {
      for (auto& row_span : delete_rows) {
        row_span.partition_ts = p_bt->minTimestamp();
        rows->emplace_back(row_span);
      }
    }
    (*count) += cnt;
    ebt_manager_->ReleasePartitionTable(p_bt);
  }
  if (del_failed) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  int ret = ebt_manager_->DeleteExpiredData(end_ts, err_info);
  if (ret < 0) {
    LOG_ERROR("SubGroup delete expired data failed, err: %s.", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteEntity(kwdbContext_p ctx, const string& primary_tag, uint64_t* del_cnt, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  uint32_t sub_group_id, entity_id;
  int ret = tag_bt_->getEntityIdGroupId(primary_tag.data(), primary_tag.size(), entity_id, sub_group_id);
  if (ret < 0) {
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  // Delete tag and its index
  tag_bt_->DeleteTagRecord(primary_tag.data(), primary_tag.size(), err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
    return KStatus::FAIL;
  }
  // Delete entities
  TsSubEntityGroup* p_bt = ebt_manager_->GetSubGroup(sub_group_id, err_info, false);
  if (p_bt) {
    p_bt->DeleteEntity(entity_id, 0, del_cnt, err_info);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteEntities(kwdbContext_p ctx, const std::vector<std::string>& primary_tags,
                                      uint64_t* count, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  *count = 0;
  for (const auto& p_tags : primary_tags) {
    uint64_t num = 0;
    KStatus status = DeleteEntity(ctx, p_tags, &num, mtr_id);
    if (status == KStatus::FAIL) {
      LOG_ERROR("Failed to delete entity by primary key %s", p_tags.c_str())
      return KStatus::FAIL;
    }
    *count += num;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteRangeEntities(kwdbContext_p ctx, const HashIdSpan& hash_span,
                                           uint64_t* count, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  // 1. First, query the primary tags of each record in the tag_bt.
  // 2. Compute the hash ID of the primary tags and check if it falls within the range HashIdSpan.
  //    If it does, proceed with the following steps.
  // 3. Invoke the `DeleteEntities` based on the primary tags to delete the entities.
  // 4. Remove the corresponding data and indexes from tag_bt.
  vector<string> primary_tags;
  tag_bt_->startRead();
  for (size_t row = 1; row <= tag_bt_->size(); row++) {
    if (!tag_bt_->isValidRow(row)) {
      continue;
    }
    string primary_tag(reinterpret_cast<char*>(tag_bt_->record(row)), tag_bt_->primaryTagSize());
    uint64_t hash_id = TsTable::GetConsistentHashId(primary_tag.data(), primary_tag.size());
    if (hash_id >= hash_span.begin && hash_id <= hash_span.end) {
      primary_tags.emplace_back(primary_tag);
    }
  }
  tag_bt_->stopRead();
  if (DeleteEntities(ctx, primary_tags, count, mtr_id) == KStatus::FAIL) {
    LOG_ERROR("delete entities error")
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                                       const std::vector<uint32_t>& scan_tags,
                                       std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (tag_bt_->GetEntityIdList(primary_tags, scan_tags, entity_id_list, res, count) < 0) {
    LOG_ERROR("GetEntityIdList error ");
    status = KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::Drop(kwdbContext_p ctx, bool is_force) {
  RW_LATCH_X_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  ebt_manager_->DropAll(is_force, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsTable::Drop error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  // tag table
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable fail. error: %s ", err_info.errmsg.c_str());
    return KStatus::SUCCESS;
  }
  DropTagTable(tag_bt_, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("dropTagTable : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  delete tag_bt_;
  tag_bt_ = nullptr;
  // delete directory of entity_group
  string group_path = db_path_ + tbl_sub_path_;
  LOG_INFO("remove group %s", group_path.c_str());
  fs::remove_all(group_path.c_str());
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::Compress(kwdbContext_p ctx, const KTimestamp& ts) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  ebt_manager_->Compress(ts, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsEntityGroup::Compress error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetIterator(kwdbContext_p ctx, SubGroupID sub_group_id, vector<uint32_t> entity_ids,
                                   std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                                   std::vector<k_uint32> ts_scan_cols, std::vector<Sumfunctype> scan_agg_types,
                                   uint32_t table_version, TsIterator** iter,
                                   std::shared_ptr<TsEntityGroup> entity_group,
                                   std::vector<timestamp64> ts_points, bool reverse, bool sorted, bool compaction) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support case).
  // TS_LSN read_lsn = GetOptimisticReadLsn();
  TsIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    if (sorted) {
      ts_iter = new TsSortedRowDataIterator(entity_group, range_.range_group_id, sub_group_id, entity_ids,
                                            ts_spans, scan_cols, ts_scan_cols, table_version, ASC, compaction);
    } else {
      ts_iter = new TsRawDataIterator(entity_group, range_.range_group_id, sub_group_id,
                                      entity_ids, ts_spans, scan_cols, ts_scan_cols, table_version);
    }
  } else {
    ts_iter = new TsAggIterator(entity_group, range_.range_group_id, sub_group_id, entity_ids,
                                ts_spans, scan_cols, ts_scan_cols, scan_agg_types, ts_points, table_version);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetTagIterator(kwdbContext_p ctx, std::vector<k_uint32>& scan_tags, EntityGroupTagIterator** iter) {
  ErrorInfo err_info;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  EntityGroupTagIterator* entity_group_iter = new EntityGroupTagIterator(tag_bt_, scan_tags);
  *iter = entity_group_iter;
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetTagIterator(kwdbContext_p ctx, std::vector<k_uint32>& scan_tags, EntityGroupTagIterator** iter,
                          const std::vector<uint32_t>& hps) {
  ErrorInfo err_info;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  EntityGroupTagIterator* entitygroupIter = new EntityGroupTagIterator(tag_bt_, scan_tags, hps);
  *iter = entitygroupIter;
  return KStatus::SUCCESS;
}


KStatus
TsEntityGroup::GetMetaIterator(kwdbContext_p ctx, EntityGroupMetaIterator** iter) {
  EntityGroupMetaIterator* meta_iter = new EntityGroupMetaIterator(range_.range_group_id, ebt_manager_);
  if (KStatus::SUCCESS != meta_iter->Init()) {
    delete meta_iter;
    meta_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = meta_iter;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::TSxClean(kwdbContext_p ctx) {
  CleanTagFiles(db_path_ + tbl_sub_path_, table_id_, -1);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::AlterTagInfo(kwdbContext_p ctx, TagInfo& new_tag_schema,
                                    ErrorInfo& err_info, uint32_t new_table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  // 1.get bigtable
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  // 2. construct old_tag_schema
  TagInfo old_tag_schema;
  bool is_found = false;
  for (const auto it : tag_bt_->getSchemaInfo()) {
    if (it->attributeInfo().m_id == new_tag_schema.m_id) {
      old_tag_schema = it->attributeInfo();
      is_found = true;
      break;
    }
  }
  if (!is_found) {
    LOG_ERROR("tag id: %u was no found", new_tag_schema.m_id);
    releaseTagTable();
    return KStatus::FAIL;
  }
  // 3. alter type
  if (tag_bt_->AlterTagType(old_tag_schema, new_tag_schema, err_info) < 0) {
    LOG_ERROR("AlterTagType failed. error: %s ", err_info.errmsg.c_str());
    releaseTagTable();
    return KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::AddTagInfo(kwdbContext_p ctx, TagInfo& new_tag_schema,
                                  ErrorInfo& err_info, uint32_t new_table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    releaseTagTable();
    return KStatus::FAIL;
  }
  if (tag_bt_->AddTagColumn(new_tag_schema, err_info) < 0) {
    LOG_ERROR("AddTagColumn failed. error: %s ", err_info.errmsg.c_str());
    releaseTagTable();
    return KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::DropTagInfo(kwdbContext_p ctx, TagInfo& tag_schema, ErrorInfo& err_info, uint32_t new_table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (tag_bt_->DropTagColumn(tag_schema, err_info) < 0) {
    LOG_ERROR("DropTagColumn failed. error: %s ", err_info.errmsg.c_str());
    releaseTagTable();
    return KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::UndoAddTagInfo(kwdbContext_p ctx, TagInfo& tag_schema, uint32_t new_table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  TagInfo old{};
  if (tag_bt_->AlterTableForUndo(0, 0, old, tag_schema, 3) < 0) {
    LOG_ERROR("AlterTableForUndo failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  tag_bt_->mutexLock();
  tag_bt_->UpdateTagVersionForUndo(new_table_version);
  tag_bt_->mutexUnlock();
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::UndoDropTagInfo(kwdbContext_p ctx, TagInfo& tag_schema, uint32_t new_table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  TagInfo info{};
  if (tag_bt_->AlterTableForUndo(0, 0, tag_schema, info, 4) < 0) {
    LOG_ERROR("AlterTableForUndo failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  tag_bt_->mutexLock();
  tag_bt_->UpdateTagVersionForUndo(new_table_version);
  tag_bt_->mutexUnlock();
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::UndoAlterTagInfo(kwdbContext_p ctx, TagInfo& origin_tag_schema, uint32_t new_table_version) {
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  TagInfo info{};
  info.m_id = origin_tag_schema.m_id;
  if (tag_bt_->AlterTableForUndo(0, 0, origin_tag_schema, info, 2) < 0) {
    LOG_ERROR("AlterTableForUndo failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  tag_bt_->mutexLock();
  tag_bt_->UpdateTagVersionForUndo(new_table_version);
  tag_bt_->mutexUnlock();
  releaseTagTable();
  return status;
}

KStatus
TsEntityGroup::GetColAttributeInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                   AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64;
      }
      attr_info.max_len = 3;
      break;
    case roachpb::SMALLINT:
      attr_info.type = DATATYPE::INT16;
      break;
    case roachpb::INT:
      attr_info.type = DATATYPE::INT32;
      break;
    case roachpb::BIGINT:
      attr_info.type = DATATYPE::INT64;
      break;
    case roachpb::FLOAT:
      attr_info.type = DATATYPE::FLOAT;
      break;
    case roachpb::DOUBLE:
      attr_info.type = DATATYPE::DOUBLE;
      break;
    case roachpb::BOOL:
      attr_info.type = DATATYPE::BYTE;
      break;
    case roachpb::CHAR:
      attr_info.type = DATATYPE::CHAR;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::BINARY:
    case roachpb::NCHAR:
      attr_info.type = DATATYPE::BINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::VARCHAR:
      attr_info.type = DATATYPE::VARSTRING;
      attr_info.max_len = col.storage_len() - 1;  // because varchar len will +1 when store
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::SDECHAR:
    case roachpb::SDEVARCHAR:
      attr_info.type = DATATYPE::STRING;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return KStatus::FAIL;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN);
  attr_info.length = col.storage_len();
  if (!col.nullable()) {
    attr_info.setFlag(AINFO_NOT_NULL);
  }
  if (col.dropped()) {
    attr_info.setFlag(AINFO_DROPPED);
  }
  attr_info.col_flag = (ColumnFlag) col.col_type();
  attr_info.version = 1;

  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetMetricColumnInfo(kwdbContext_p ctx, struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  switch (attr_info.type) {
    case DATATYPE::TIMESTAMP64_LSN:
      col.set_storage_type(roachpb::TIMESTAMPTZ);
      break;
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      col.set_storage_len(attr_info.max_len + 1);  // varchar(len) + 1
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::STRING:
      col.set_storage_type(roachpb::SDECHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::INVALID:
    default:
    return KStatus::FAIL;
  }

  col.set_column_id(attr_info.id);
  col.set_name(attr_info.name);
  col.set_nullable(true);
  col.set_dropped(false);
  if (!col.has_storage_len()) {
    col.set_storage_len(attr_info.length);
  }
  if (attr_info.isFlag(AINFO_NOT_NULL)) {
    col.set_nullable(false);
  }
  if (attr_info.isFlag(AINFO_DROPPED)) {
    col.set_dropped(true);
  }
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)(attr_info.col_flag));
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetTagColumnInfo(kwdbContext_p ctx, struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  col.set_storage_len(tag_info.m_length);
  col.set_column_id(tag_info.m_id);
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)((ColumnFlag)tag_info.m_tag_type));
  switch (tag_info.m_data_type) {
    case DATATYPE::TIMESTAMP64_LSN:
      col.set_storage_type(roachpb::TIMESTAMPTZ);
      break;
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      break;
    case DATATYPE::STRING:
      col.set_storage_type(roachpb::SDECHAR);
      break;
    case DATATYPE::INVALID:
    default:
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}
/// @brief RangeGroupID compute function. new used in hashPoint. RangeGroupID % 65535. hashPoint % 10
/// @param data primaryKey to compute which HashPoint it belongs to
/// @param length how long the primaryKey to compute
/// @return hashPoint ID
uint32_t TsTable::GetConsistentHashId(const char* data, size_t length) {
  // TODO(jiadx): 使用与GO层相同的一致性hashID算法，可能还需要一些方法参数
//  uint64_t hash_id = std::hash<string>()(primary_tags);
  const uint32_t offset_basis = 2166136261;  // 32位offset basis
  const uint32_t prime = 16777619;
  uint32_t hash_val = offset_basis;
  for (int i = 0; i < length; i++) {
    unsigned char b = data[i];
    hash_val *= prime;
    hash_val ^= b;
  }
  uint32_t range_num = 20;
  return hash_val % range_num;
}

MMapRootTableManager* TsTable::CreateMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                          vector<AttributeInfo>& schema, uint32_t table_version,
                                                          uint64_t partition_interval, ErrorInfo& err_info) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id, partition_interval);
  KStatus s = tmp_bt_manager->CreateRootTable(schema, table_version, err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

MMapRootTableManager* TsTable::OpenMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                        ErrorInfo& err_info) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id);
  KStatus s = tmp_bt_manager->Init(err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

TsTable::TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id)
    : db_path_(db_path), table_id_(table_id) {
  entity_bt_manager_ = nullptr;
  tbl_sub_path_ = std::to_string(table_id_) + "/";
  db_path_ = db_path_ + "/";
  is_dropped_.store(false);
  entity_groups_mtx_ = new TsTableEntityGrpsRwLatch(RWLATCH_ID_TS_TABLE_ENTITYGRPS_RWLOCK);
  snapshot_manage_mtx_ = new TsTableSnapshotLatch(LATCH_ID_TSTABLE_SNAPSHOT_MUTEX);
}

TsTable::~TsTable() {
  if (is_dropped_) {
    kwdbContext_t context;
    kwdbContext_p ctx = &context;
    KStatus s = InitServerKWDBContext(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("InitServerKWDBContext Error!");
    }
    s = DropAll(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DropAll Error!");
    }
  }
  if (entity_bt_manager_) {
    delete entity_bt_manager_;
  }
  if (entity_groups_mtx_) {
    delete entity_groups_mtx_;
    entity_groups_mtx_ = nullptr;
  }
  if (snapshot_manage_mtx_) {
    delete snapshot_manage_mtx_;
    snapshot_manage_mtx_ = nullptr;
  }
}

// Check that the directory name is a numeric
bool IsNumber(struct dirent* dir) {
  for (int i = 0; i < strlen(dir->d_name); ++i) {
    if (!isdigit(dir->d_name[i])) {
      // Iterate over each character and determine if it's a number
      return false;
    }
  }
  return true;
}

KStatus TsTable::Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups, ErrorInfo& err_info) {
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    err_info.setError(KWENOOBJ, "invalid path: " + db_path_ + tbl_sub_path_);
    LOG_WARN("can not access table path [%s].", ((db_path_ + tbl_sub_path_)).c_str())
    return KStatus::FAIL;
  }

  // Load entity bt
  entity_bt_manager_ = OpenMMapRootTableManager(db_path_, tbl_sub_path_, table_id_, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsTable Init error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  DIR* dir_ptr = opendir((db_path_ + "/" + tbl_sub_path_).c_str());
  if (dir_ptr == nullptr) {
    LOG_ERROR("invalid path : %s", ((db_path_ + tbl_sub_path_)).c_str());
    return KStatus::FAIL;
  }
  // Traverse the range directory
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    struct dirent* group_dir;
    while ((group_dir = readdir(dir_ptr)) != nullptr) {
      if (!IsNumber(group_dir)) {  // Check if it is an entity group directory
        continue;
      }
      int64_t range_group_id = std::atol(group_dir->d_name);
      if (range_group_id < 1) {
        LOG_ERROR("invalid range group : %ld", range_group_id);
      }
      string range_tbl_sub_path = tbl_sub_path_ + group_dir->d_name + "/";
      // hash_range's default type is FOLLOWER when loaded locally
      RangeGroup hash_range{(uint64_t) range_group_id, EntityGroupType::UNINITIALIZED};
      auto it = range_groups.find(range_group_id);
      if (it != range_groups.end()) {
        hash_range.typ = it->second;
      }
      std::shared_ptr<TsEntityGroup> t_range;
      KStatus s = newEntityGroup(ctx, hash_range, range_tbl_sub_path, &t_range);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsTableRange OpenInit error : %lu %lu", table_id_, hash_range.range_group_id);
        continue;
      }
      entity_groups_[hash_range.range_group_id] = std::move(t_range);
    }
    closedir(dir_ptr);
  }
  is_dropped_.store(entity_bt_manager_->IsDropped());
  return KStatus::SUCCESS;
}

KStatus TsTable::newEntityGroup(kwdbContext_p ctx, RangeGroup hash_range, const string& range_tbl_sub_path,
                                std::shared_ptr<TsEntityGroup>* ent_group) {
  constructEntityGroup(ctx, hash_range, range_tbl_sub_path, ent_group);
  return (*ent_group)->OpenInit(ctx);
}

KStatus TsTable::Create(kwdbContext_p ctx, vector<AttributeInfo>& metric_schema,
                        uint32_t ts_version, uint64_t partition_interval) {
  if (entity_bt_manager_ != nullptr) {
    LOG_ERROR("Entity Bigtable already exist.");
    return KStatus::FAIL;
  }
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path)) {
      return KStatus::FAIL;
    }
  }

  ErrorInfo err_info;
  // Create entity table
  entity_bt_manager_ = CreateMMapRootTableManager(db_path_, tbl_sub_path_, table_id_, metric_schema, ts_version,
                                                  partition_interval, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("createTable fail, table_id[%lu], msg[%s]", table_id_, err_info.errmsg.c_str());
  }

  if (entity_bt_manager_ == nullptr) {
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoIncludeDropped(data_schema);
}

KStatus TsTable::GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoExcludeDropped(data_schema);
}

KStatus TsTable::GetTagSchema(kwdbContext_p ctx, RangeGroup range, std::vector<TagColumn*>* tag_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  if (entity_groups_.find(range.range_group_id) == entity_groups_.end()) {
    return KStatus::FAIL;
  }
  *tag_schema = entity_groups_[range.range_group_id]->GetSchema();
  return KStatus::SUCCESS;
}

KStatus TsTable::GenerateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                         std::vector<AttributeInfo>& metric_schema,
                                         std::vector<TagInfo>& tag_schema) {
  EnterFunc()
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  for (auto col_var : metric_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    KStatus s = TsEntityGroup::GetMetricColumnInfo(ctx, col_var, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate metric Schema", col_var.type);
      Return(s);
    }
  }

  // Traverse tag schema and use tag info to construct metric column info of meta
  for (auto tag_info : tag_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    // XXX Notice: tag_info don't has tag column name,
    KStatus s = TsEntityGroup::GetTagColumnInfo(ctx, tag_info, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate tag Schema", tag_info.m_data_type);
      Return(s);
    }
    // Set storage length.
    if (col->has_storage_len() && col->storage_len() == 0) {
      col->set_storage_len(tag_info.m_size);
    }
  }

  Return(KStatus::SUCCESS);
}

KStatus TsTable::CreateEntityGroup(kwdbContext_p ctx, RangeGroup range, vector<TagInfo>& tag_schema,
                                   std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    if (entity_groups_.find(range.range_group_id) != entity_groups_.end()) {
      LOG_ERROR("TableRange already exist : %s", (tbl_sub_path_ + "." + std::to_string(range.range_group_id)).c_str());
      return KStatus::FAIL;
    }
  }

  string range_tbl_sub_path = tbl_sub_path_ + std::to_string(range.range_group_id) + "/";
  if (!MakeDirectory(db_path_ + range_tbl_sub_path)) {
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> t_group;
  constructEntityGroup(ctx, range, range_tbl_sub_path, &t_group);

  KStatus s = t_group->Create(ctx, tag_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsTableRange OpenInit error : %ld,%ld", table_id_, range.range_group_id);
    return KStatus::FAIL;
  }
  // init tag version
  t_group->UpdateTagVersion(entity_bt_manager_->GetCurrentTableVersion());
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  entity_groups_[range.range_group_id] = std::move(t_group);
  *entity_group = entity_groups_[range.range_group_id];
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityGroups(kwdbContext_p ctx, RangeGroups *groups) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  groups->len = entity_groups_.size();
  groups->ranges = static_cast<RangeGroup*>(malloc(sizeof(RangeGroup) * groups->len));
  if (groups->ranges == nullptr) {
    LOG_ERROR("failed malloc RangeGroups");
    return KStatus::FAIL;
  }
  int i = 0;
  for (auto &p : entity_groups_) {
    groups->ranges[i].range_group_id = p.first;
    groups->ranges[i].typ = p.second->HashRange().typ;
    LOG_INFO("range_group_id: %lu, type: %d", groups->ranges[i].range_group_id, groups->ranges[i].typ);
    ++i;
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::UpdateEntityGroup(kwdbContext_p ctx, const RangeGroup& range) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(range.range_group_id);
  if (it == entity_groups_.end()) {
    LOG_ERROR("no hash range : %ld", range.range_group_id);
    return KStatus::FAIL;
  }
  it->second->HashRange().typ = range.typ;
  return KStatus::SUCCESS;
}

KStatus
TsTable::GetEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(range_group_id);
  if (it != entity_groups_.end()) {
    *entity_group = it->second;
  } else {
    // s = CreateTableRange(ctx, range, table_range);
    LOG_ERROR("no hash range: %ld", range_group_id);
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::GetEntityGroupByPrimaryKey(kwdbContext_p ctx, const TSSlice& primary_key,
                                            std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  // scan all tags in entitygroup to check if entity in existed entitygroup.
  for (auto& entity_grp : entity_groups_) {
    if (entity_grp.second->GetSubEntityGroupTagbt()->hasPrimaryKey(primary_key.data, primary_key.len)) {
      *entity_group = entity_grp.second;
      return KStatus::SUCCESS;
    }
  }
  // if entity no exists in storage. store it to default entitygroup.
  auto iter = entity_groups_.find(default_entitygroup_id_in_dist_v2);
  if (iter != entity_groups_.end()) {
    *entity_group = iter->second;
    return KStatus::SUCCESS;
  }
  LOG_ERROR("can not find entitygroup for primary key %s", primary_key.data);
  return KStatus::FAIL;
}

KStatus
TsTable::GetEntityGroupByHash(kwdbContext_p ctx, uint16_t hashpoint, uint64_t *range_group_id,
                       std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  // *range_group_id = getRangeIdByHash(hashpoint);
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(*range_group_id);
  if (it != entity_groups_.end()) {
    *entity_group = it->second;
  } else {
    // s = CreateTableRange(ctx, range, table_range);
    LOG_ERROR("no hash range: %ln", range_group_id);
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                         uint64_t mtr_id, DedupResult* dedup_result, const DedupRule& dedup_rule) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> entity_grp;
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // entity_groups_: Data from entities in an EntityRangeGroup is stored in an EntityGroup.
    // EntityGroup is a logical unit to persist entities' data, including tag data and measurement data.
    auto it = entity_groups_.find(range_group_id);
    if (it == entity_groups_.end()) {
      LOG_ERROR("no entity group with id: %lu", range_group_id);
      return KStatus::FAIL;
    }
    entity_grp = it->second;
  }
  KStatus s = entity_grp->PutData(ctx, payload, payload_num, mtr_id, dedup_result, dedup_rule);
  return s;
}

KStatus TsTable::PutDataWithoutWAL(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                         uint64_t mtr_id, DedupResult* dedup_result, const DedupRule& dedup_rule) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> entity_grp;
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // entity_groups_: Data from entities in an EntityRangeGroup is stored in an EntityGroup.
    // EntityGroup is a logical unit to persist entities' data, including tag data and measurement data.
    auto it = entity_groups_.find(range_group_id);
    if (it == entity_groups_.end()) {
      LOG_ERROR("no entity group with id: %lu", range_group_id);
      return KStatus::FAIL;
    }
    entity_grp = it->second;
  }
  KStatus s = entity_grp->PutDataWithoutWAL(ctx, *payload, mtr_id, dedup_result, dedup_rule);
  return s;
}

KStatus TsTable::FlushBuffer(kwdbContext_p ctx) {
  // No processing required
  return KStatus::SUCCESS;
}


KStatus TsTable::CreateCheckpoint(kwdbContext_p ctx) {
  // No processing required
  return KStatus::SUCCESS;
}

KStatus TsTable::Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes) {
  // No processing required
  return KStatus::SUCCESS;
}

KStatus TsTable::GetAllLeaderEntityGroup(kwdbts::kwdbContext_p ctx,
                                         std::vector<std::shared_ptr<TsEntityGroup>>* leader_entity_groups) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  leader_entity_groups->clear();
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    // if (entity_group.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(entity_group.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (entity_group.second->hashRange().typ == EntityGroupType::LEADER) {
      leader_entity_groups->push_back(entity_group.second);
    // }
  }
  return s;
}

KStatus TsTable::DropEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, bool sync) {
  LOG_INFO("DropEntityGroup: range_group_id %lu", range_group_id);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  auto it = entity_groups_.find(range_group_id);
  if (it == entity_groups_.end()) {
    LOG_ERROR("no hash range");
    RW_LATCH_UNLOCK(entity_groups_mtx_);
    return KStatus::FAIL;
  }
  auto entity_group = it->second;
  entity_groups_.erase(it);
  RW_LATCH_UNLOCK(entity_groups_mtx_);

  KStatus s = entity_group->Drop(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("entity group drop failed, range_group_id=%lu", range_group_id);
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer{[&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); }};
    entity_groups_[range_group_id] = entity_group;
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::DropAll(kwdbContext_p ctx, bool is_force) {
  LOG_INFO("TsTable::drop table %ld", table_id_);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  std::unordered_map<uint64_t, int8_t> range_groups;
  for (auto & entity_group : entity_groups_) {
    range_groups.insert({entity_group.second->HashRange().range_group_id, entity_group.second->HashRange().typ});
    s = entity_group.second->Drop(ctx, is_force);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange drop failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }
  entity_groups_.clear();
  // clear tbl_ranges, and re-initialize
  if (s != KStatus::SUCCESS) {
    delete entity_bt_manager_;
    entity_bt_manager_ = nullptr;
    Init(ctx, range_groups);
    return s;
  }

  entity_bt_manager_->RemoveAll();
  delete entity_bt_manager_;
  entity_bt_manager_ = nullptr;
  // 删除TsTable目录
  LOG_INFO("TsTable::remove table %ld files", table_id_);
  ::remove((db_path_ + tbl_sub_path_).c_str());
  LOG_INFO("TsTable::drop table %ld over", table_id_);
  return KStatus::SUCCESS;
}

KStatus TsTable::Compress(kwdbContext_p ctx, const KTimestamp& ts) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  if (!entity_bt_manager_->SetCompressStatus(true)) {
    // Other threads are currently being compressed, skipping this compression
    LOG_INFO("table[%lu] is compressing, skip this compression", table_id_);
    return KStatus::SUCCESS;
  }
  KStatus s = KStatus::SUCCESS;
  std::vector<std::shared_ptr<TsEntityGroup>> compress_entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // get all entity groups
    for (auto& entity_group : entity_groups_) {
      compress_entity_groups.push_back(entity_group.second);
    }
  }
  for (auto& entity_group : compress_entity_groups) {
    if (!entity_group) {
      continue;
    }
    s = entity_group->Compress(ctx, ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange compress failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }

  entity_bt_manager_->SetCompressStatus(false);
  return s;
}

KStatus TsTable::GetEntityIndex(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                std::vector<EntityResultIndex> &entity_store) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    MMapTagColumnTable* entity_tag_bt = p.second->GetSubEntityGroupTagbt();
    entity_tag_bt->startRead();
    for (int rownum = 1; rownum <= entity_tag_bt->size(); rownum++) {
      if (!entity_tag_bt->isValidRow(rownum)) {
        continue;
      }
      uint32_t tag_hash = TsTable::GetConsistentHashId(
                          reinterpret_cast<char*>(entity_tag_bt->record(rownum)), entity_tag_bt->primaryTagSize());
      if (begin_hash <= tag_hash && tag_hash <= end_hash) {
        entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
      }
    }
    entity_tag_bt->stopRead();
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityGrpByPriKey(kwdbContext_p ctx, const TSSlice& primary_key, uint64_t* entity_grp_idp) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    MMapTagColumnTable* entity_tag_bt = p.second->GetSubEntityGroupTagbt();
    int entity_id;
    SubGroupID sub_grp_id;
    if (entity_tag_bt->hasPrimaryKey(primary_key.data, primary_key.len)) {
      *entity_grp_idp = p.first;
      return KStatus::SUCCESS;
    }
  }
  *entity_grp_idp = default_entitygroup_id_in_dist_v2;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityIndexWithRowNum(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                          std::vector<std::pair<int, EntityResultIndex>> &entity_tag) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    MMapTagColumnTable* entity_tag_bt = p.second->GetSubEntityGroupTagbt();
    entity_tag_bt->startRead();
    for (int rownum = 1; rownum <= entity_tag_bt->actual_size(); rownum++) {
      if (!entity_tag_bt->isValidRow(rownum)) {
        continue;
      }
      std::vector<EntityResultIndex> entity_store;
      uint32_t tag_hash = TsTable::GetConsistentHashId(
                          reinterpret_cast<char*>(entity_tag_bt->record(rownum)), entity_tag_bt->primaryTagSize());
      if (begin_hash <= tag_hash && tag_hash <= end_hash) {
        entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
        entity_tag.push_back(std::pair<int, EntityResultIndex>(rownum, entity_store[0]));
      }
    }
    entity_tag_bt->stopRead();
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetAvgTableRowSize(kwdbContext_p ctx, uint64_t* row_size) {
  // fixed tuple length of one row.
  size_t row_length = 0;
  std::vector<AttributeInfo> schemas;
  auto s = entity_bt_manager_->GetSchemaInfoExcludeDropped(&schemas);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  for (auto& col : schemas) {
    if (col.type == DATATYPE::VARSTRING || col.type == DATATYPE::VARBINARY) {
      row_length += col.max_len;
    } else {
      row_length += col.length;
    }
  }
  // todo(liangbo01): make precise estimate if needed.
  *row_size = row_length;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataVolume(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, uint64_t* volume) {
  LOG_DEBUG("GetDataVolume begin. table[%lu] hash[%lu - %lu], time [%ld - %ld]",
            table_id_, begin_hash, end_hash, ts_span.begin, ts_span.end);
  uint64_t row_length = 0;
  KStatus s = GetAvgTableRowSize(ctx, &row_length);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAvgTableRowSize failed. table [%lu] ", table_id_);
    return s;
  }
  // total row num that  satisfied ts_span and hash span.
  std::vector<EntityResultIndex> entity_store;
  s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }
  size_t total_rows = 0;
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      partitions = cur_sub_group->GetPartitionTables({ts_span.begin / 1000, ts_span.end / 1000}, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%lu - %u]", entity_grp.first, sub_grp.first);
          return KStatus::FAIL;
      }
      std::deque<BlockItem*> block_item_queue;
      timestamp64 min_ts, max_ts;
      for (auto& partition : partitions) {
        for (auto cur_entity_id : sub_grp.second) {
          block_item_queue.clear();
          partition->GetAllBlockItems(cur_entity_id, block_item_queue);
          for (auto& block_item : block_item_queue) {
            if (partition->GetBlockMinMaxTS(block_item, &min_ts, &max_ts) &&
                isTimestampWithinSpans({ts_span}, min_ts, max_ts)) {
              total_rows += block_item->publish_row_count - block_item->getDeletedCount();
            }
          }
        }
        ReleaseTable(partition);
      }
    }
  }
  LOG_DEBUG("GetDataVolume end. table[%lu] average row size[%lu], row num [%lu], entities num [%lu]",
            table_id_, row_length, total_rows, entities.size());
  *volume = row_length * total_rows;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataVolumeHalfTS(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, timestamp64* half_ts) {
  LOG_DEBUG("GetDataVolumeHalfTS begin. table[%lu] hash[%lu - %lu], time [%ld - %ld]",
              table_id_, begin_hash, end_hash, ts_span.begin, ts_span.end);
  *half_ts = INVALID_TS;
  if (begin_hash != end_hash) {
    LOG_ERROR("can not calculate half timestmap with different hash [%lu - %lu]", begin_hash, end_hash);
    return KStatus::FAIL;
  }
  // total rows of matched entities.
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }

  std::vector<AttributeInfo> data_schema;
  s = GetDataSchemaIncludeDropped(ctx, &data_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get GetDataSchemaIncludeDropped.");
    return s;
  }
  // caclulate all partitions matched entities rows. partitions based on news configure.
  std::map<int64_t, size_t> partitions_rows;
  std::map<int64_t, timestamp64> partitions_mid_ts;
  size_t total_rows = 0;
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      partitions = cur_sub_group->GetPartitionTables({ts_span.begin / 1000, ts_span.end / 1000}, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%lu - %u]", entity_grp.first, sub_grp.first);
          return KStatus::FAIL;
      }
      std::deque<BlockItem*> block_item_queue;
      timestamp64 min_ts, max_ts, max_pt_time;
      for (auto& partition : partitions) {
        uint64_t partition_time = cur_sub_group->PartitionTime(partition->minTimestamp(), max_pt_time);
        TsPartitonIteratorParams param{entity_grp.first, sub_grp.first, sub_grp.second, {ts_span}, {0}, {0}, data_schema};
        TsPartitonIterator pt_iter(partition, param);
        uint32_t partition_row_count = 0;
        while (true) {
          ResultSet res(1);
          uint32_t count = 0;
          if (KStatus::SUCCESS != pt_iter.Next(&res, &count, false, nullptr)) {
            LOG_ERROR("TsPartitonIterator next failed.");
            return KStatus::FAIL;
          }
          if (count == 0) {
            break;
          }
          partition_row_count += count;
        }
        if (partition_row_count > 0) {
          partitions_rows[partition_time] += partition_row_count;
          total_rows += partition_row_count;
          timestamp64 p_min_ts{INT64_MAX}, p_max_ts{INT64_MIN};
          for (auto cur_entity_id : sub_grp.second) {
            auto en_item = partition->getEntityItem(cur_entity_id);
            if (en_item->block_count == 0) {
              continue;
            }
            p_min_ts = std::max(en_item->min_ts, ts_span.begin);
            p_max_ts = std::min(en_item->max_ts, ts_span.end);
            LOG_ERROR("p_min_ts:%ld, p_max_ts: %ld, partition[%ld-%ld], entity ts[%ld-%ld]", p_min_ts, p_max_ts,
                        partition->minTimestamp(), partition->maxTimestamp(), en_item->min_ts, en_item->max_ts);
            if (p_min_ts >= p_max_ts) {
              continue;
            }
            if (p_min_ts < partition->minTimestamp() * 1000 || p_max_ts > partition->maxTimestamp() * 1000) {
              continue;
            }
            if (partitions_mid_ts.find(partition_time) != partitions_mid_ts.end()) {
              partitions_mid_ts[partition_time] = (partitions_mid_ts[partition_time] + (p_min_ts + p_max_ts) / 2) / 2;
            } else {
              partitions_mid_ts[partition_time] = (p_min_ts + p_max_ts) / 2;
            }
          }
        }
        ReleaseTable(partition);
      }
    }
  }
  if (total_rows == 0) {
    LOG_ERROR("no row left, cannot split. table [%lu]", table_id_);
    return KStatus::FAIL;
  }
  // todo(liangbo01): temp code for debug. will delete later.
  auto it_tmp = partitions_rows.begin();
  for (; it_tmp != partitions_rows.end(); it_tmp++) {
    LOG_DEBUG("partition [%ld] has rows [%lu], midts:%ld.",
              it_tmp->first, it_tmp->second, partitions_mid_ts[it_tmp->first]);
  }
  // caclulte half ts, ts must multiple of partition_interval
  size_t scan_pt_rows = 0;
  int64_t find_min_ts = 0;
  for (auto &iter : partitions_rows) {
    find_min_ts = iter.first;
    if (scan_pt_rows + iter.second > total_rows / 2) {
      *half_ts = iter.first * 1000;
      break;
    } else {
      scan_pt_rows += iter.second;
    }
  }
  // The distribution is severely uneven, using middle ts of partition.
  if (scan_pt_rows * 3 < total_rows || *half_ts < ts_span.begin) {
    *half_ts = partitions_mid_ts[find_min_ts];
  }
  if (!isTimestampInSpans({ts_span}, *half_ts, *half_ts)) {
    LOG_ERROR("GetDataVolumeHalfTS faild. range{%lu/%ld - %lu/%ld}, half ts [%ld]",
              begin_hash, ts_span.begin, end_hash, ts_span.end, *half_ts);
    return KStatus::FAIL;
  }
  LOG_DEBUG("GetDataVolumeHalfTS end. range{%lu/%ld - %lu/%ld}, total_rows:%lu, half ts [%ld]",
            begin_hash, ts_span.begin, end_hash, ts_span.end, total_rows, *half_ts);
  return KStatus::SUCCESS;
}

KStatus TsTable::GetRangeRowCount(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                  KwTsSpan ts_span, uint64_t* count) {
  *count = 0;
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;

  std::vector<AttributeInfo> data_schema;
  s = GetDataSchemaIncludeDropped(ctx, &data_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get GetDataSchemaIncludeDropped.");
    return s;
  }
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      TsPartitonIteratorParams params{entity_grp.first, sub_grp.first, {},
                                      {ts_span}, {0}, {0}, data_schema};
      for (auto& item : sub_grp.second) {
        params.entity_ids.push_back(item);
      }
      auto cur_entity_iter_ = new TsSubGroupIterator(cur_sub_group, params);
      while (true) {
        ResultSet res(1);
        uint32_t count1;
        cur_entity_iter_->Next(&res, &count1, false, nullptr);
        if (count1 == 0) {
          break;
        }
        *count += count1;
      }
      delete cur_entity_iter_;
    }
  }
  LOG_DEBUG("GetRangeRowCount range{hash[%lu - %lu] ts[%ld - %ld]} row count: %lu.",
            begin_hash, end_hash, ts_span.begin, ts_span.end, *count);
  return KStatus::SUCCESS;
}

KStatus TsTable::DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
               KwTsSpan ts_span, uint64_t mtr_id) {
  uint64_t row_num_bef = 0;
  uint64_t row_num_aft = 0;
#ifdef K_DEBUG
  GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_bef);
#endif
  // get matched entities.
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> group_ids;
  for (auto& entity : entity_store) {
    group_ids[entity.entityGroupId][entity.subGroupId].push_back(entity.entityId);
  }
  ErrorInfo err_info;
  uint64_t total_del_rows = 0;
  for (auto& egrp : group_ids) {
    std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
    s = GetEntityGroup(ctx, egrp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      cur_entity_group.reset();
      LOG_ERROR("cannot get entitygroup [%lu]", egrp.first);
      return s;
    }
    for (auto& sub_grp : egrp.second) {
      err_info.clear();
      auto cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, egrp.first);
        return KStatus::FAIL;
      }
      std::vector<TsTimePartition*> partitions;
      partitions = cur_sub_group->GetPartitionTables({ts_span.begin / 1000, ts_span.end / 1000}, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%u]", sub_grp.first);
          return KStatus::FAIL;
      }
      for (auto& partition : partitions) {
        for (auto& entity_id : sub_grp.second) {
          uint64_t del_rows = 0;
          int ret = partition->DeleteData(entity_id, 0, {ts_span}, nullptr, &del_rows, err_info);
          if (err_info.errcode < 0) {
              LOG_ERROR("partition DeleteData failed. egrp [%lu], partition [%ld]",
                          egrp.first, partition->minTimestamp());
              break;
          }
          total_del_rows += del_rows;
        }
        ReleaseTable(partition);
      }
    }
  }
#ifdef K_DEBUG
  GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_aft);
#endif
  if (row_num_bef != total_del_rows) {
    LOG_ERROR("DeleteTotalRange failed. rows: %lu, deleted:%lu", row_num_bef, total_del_rows);
  }
  LOG_DEBUG("DeleteTotalRange range{hash[%lu - %lu] ts[%ld - %ld]} delete rows [%lu], del before:%lu, del after: %lu.",
            begin_hash, end_hash, ts_span.begin, ts_span.end,
            total_del_rows, row_num_bef, row_num_aft);
  // todo(liangbo01): check if entity data is all deleted. if so, we can delete entity tag.
  //  we cannot know deleted data is (1) delted by user. (2)range migrate. so we cannot drop tag info.
  // so we cannot mark entity deleted.
  return KStatus::SUCCESS;
}

KStatus TsTable::ConvertRowTypePayload(kwdbContext_p ctx,  TSSlice payload_row, TSSlice* payload) {
  uint32_t pl_version = Payload::GetTsVsersionFromPayload(&payload_row);
  MMapMetricsTable* root_table = entity_bt_manager_->GetRootTable(pl_version);
  if (root_table == nullptr) {
    LOG_ERROR("table[%lu] cannot found version[%u].", table_id_, pl_version);
    return KStatus::FAIL;
  }
  const std::vector<AttributeInfo>& data_schema = root_table->getSchemaInfoExcludeDropped();
  PayloadStTypeConvert pl(payload_row, data_schema);
  auto s = pl.build(entity_bt_manager_, payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not parse current row-based payload.");
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                             std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                             std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                             TsTableIterator** iter, std::vector<timestamp64> ts_points,
                             bool reverse, bool sorted) {
  KWDB_DURATION(StStatistics::Get().get_iterator);
  if (scan_cols.empty()) {
    // LOG_ERROR("TsTable::GetIterator Error : no column");
    // return KStatus::FAIL;
  }
  auto ts_table_iterator = new TsTableIterator();
  KStatus s;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};

  auto actual_cols = entity_bt_manager_->GetIdxForValidCols(table_version);
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : scan_cols) {
    if (col >= actual_cols.size()) {
      // In the concurrency scenario, after the storage has deleted the column,
      // kwsql sends query again
      LOG_ERROR("GetIterator Error : TsTable no column %d", col);
      return KStatus::FAIL;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  std::map<uint64_t, std::map<SubGroupID, std::vector<EntityID>>> group_ids;
  for (auto& entity : entity_ids) {
    group_ids[entity.entityGroupId][entity.subGroupId].push_back(entity.entityId);
  }
  std::shared_ptr<TsEntityGroup> entity_group;
  for (auto& group_iter : group_ids) {
    s = GetEntityGroup(ctx, group_iter.first, &entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("can not found entitygroup [%lu].", group_iter.first);
      return s;
    }
    for (auto& sub_group_iter : group_iter.second) {
      TsIterator* ts_iter;
      s = entity_group->GetIterator(ctx, sub_group_iter.first, sub_group_iter.second, ts_spans,
                                    scan_cols, ts_scan_cols, scan_agg_types, table_version,
                                    &ts_iter, entity_group, ts_points, reverse, sorted, false);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot create iterator for entitygroup[%lu], subgroup[%u]", group_iter.first, sub_group_iter.first);
        return s;
      }
      ts_table_iterator->AddEntityIterator(ts_iter);
    }
  }
  LOG_DEBUG("TsTable::GetIterator success.agg: %lu, iter num: %lu",
              scan_agg_types.size(), ts_table_iterator->GetIterNumber());
  (*iter) = ts_table_iterator;
  return KStatus::SUCCESS;
}

KStatus TsTable::CreateSnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t begin_hash, uint64_t end_hash,
                                uint64_t* snapshot_id) {
  LOG_INFO("CreateSnapshot begin! [Snapshot ID:%lu, Ranggroup ID: %lu]", *snapshot_id, range_group_id);
  std::shared_ptr<TsEntityGroup> entity_group_src;
  KStatus s = GetEntityGroup(ctx, range_group_id, &entity_group_src);
  if (s == KStatus::FAIL || entity_group_src == nullptr) {
    // Add error messages
    LOG_ERROR("GetEntityGroup failed during CreateSnapshot, range_group_id=%lu, snapshot_id=%lu",
              range_group_id, *snapshot_id);
    return KStatus::FAIL;
  }

  SnapshotInfo ts_snapshot_info{};
  ts_snapshot_info.begin_hash = begin_hash;
  ts_snapshot_info.end_hash = end_hash;
  ts_snapshot_info.type = 0;
  // type = 0, source node build snapshot, need to generate snapshot id
  auto now = std::chrono::system_clock::now();
  // Converts time to milliseconds that have elapsed since January 1, 1970
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  ts_snapshot_info.id = timestamp;
  std::shared_ptr<TsTableSnapshot> ts_snapshot_table =
      std::make_shared<TsTableSnapshot>(db_path_, table_id_, tbl_sub_path_, entity_group_src,
                                        entity_bt_manager_, ts_snapshot_info);
  s = ts_snapshot_table->Init(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Init Snapshot table failed, range_group_id=%lu, snapshot_id=%lu", range_group_id, *snapshot_id);
    return KStatus::FAIL;
  }
  *snapshot_id = ts_snapshot_table->GetSnapshotId();
  snapshot_manage_pool_[ts_snapshot_table->GetSnapshotId()] = ts_snapshot_table;
  LOG_INFO("CreateSnapshot success! [Snapshot ID:%lu, Ranggroup ID: %lu]", *snapshot_id, range_group_id);
  return KStatus::SUCCESS;
}

KStatus TsTable::DropSnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id) {
  TsTableSnapshot* ts_snapshot_table = nullptr;
  MUTEX_LOCK(snapshot_manage_mtx_);
  if (snapshot_manage_pool_.find(snapshot_id) != snapshot_manage_pool_.end()) {
    ts_snapshot_table = snapshot_manage_pool_[snapshot_id].get();
  } else {
    LOG_ERROR("Snapshot table not found, range_group_id=%lu, snapshot_id=%lu", range_group_id, snapshot_id);
    MUTEX_UNLOCK(snapshot_manage_mtx_);
    return KStatus::FAIL;
  }

  if (ts_snapshot_table == nullptr) {
    LOG_ERROR("Snapshot table not found, range_group_id=%lu, snapshot_id=%lu", range_group_id, snapshot_id);
    MUTEX_UNLOCK(snapshot_manage_mtx_);
    return KStatus::FAIL;
  }

  KStatus s = ts_snapshot_table->DropAll();
  if (s == KStatus::FAIL) {
    LOG_ERROR("DropAll Snapshot, range_group_id=%lu, snapshot_id=%lu", range_group_id, snapshot_id);
    MUTEX_UNLOCK(snapshot_manage_mtx_);
    return KStatus::FAIL;
  }
  snapshot_manage_pool_.erase(snapshot_id);
  MUTEX_UNLOCK(snapshot_manage_mtx_);
  return KStatus::SUCCESS;
}

KStatus TsTable::GetSnapshotData(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id,
                                 size_t offset, size_t limit, TSSlice* data, size_t* total) {
  LOG_INFO("GetSnapshotData begin! snapshot_id:%ld, range_group_id: %ld", snapshot_id, range_group_id);
  if (snapshot_manage_pool_.find(snapshot_id) == snapshot_manage_pool_.end()) {
    LOG_ERROR("GetSnapshotData failed, range_group_id=%lu, snapshot_id=%ld", range_group_id, snapshot_id);
    return KStatus::FAIL;
  }
  std::shared_ptr<TsTableSnapshot> snap_shot = snapshot_manage_pool_.find(snapshot_id)->second;
  KStatus s = KStatus::FAIL;
  if (snapshot_get_size_pool_.find(snapshot_id) == snapshot_get_size_pool_.end()) {
    // todo(lfl): LSN needs to be added in the future
    TS_LSN lsn = 9999999999999;
    s = snap_shot->BuildSnapshot(ctx, lsn);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("BuildSnapshot failed, range_group_id=%lu, snapshot_id=%lu", range_group_id, snapshot_id);
      return s;
    }
    s = snap_shot->CompressSnapshot(ctx, total);
    if (s == KStatus::FAIL) {
      LOG_ERROR("CompressSnapshot failed during GetSnapshotData, range_group_id=%lu, snapshot_id=%ld",
                range_group_id, snapshot_id);
      return KStatus::FAIL;
    }
    snapshot_get_size_pool_[snapshot_id] = std::move(*total);
  }

  TsEntityGroup* snap_shot_gp = snap_shot->GetSnapshotEntityGroup();
  if (snap_shot_gp->GetSubEntityGroupTagbt()->size() == 0) {
    *total = 0;
    return KStatus::SUCCESS;
  }
  *total = snapshot_get_size_pool_[snapshot_id];
  s = snap_shot->GetSnapshotData(ctx, range_group_id, offset, limit, data, total);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetSnapshotData failed, range_group_id=%lu, snapshot_id=%ld",
              range_group_id, snapshot_id);
    return KStatus::FAIL;
  }
  LOG_INFO("GetSnapshotData success! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  return KStatus::SUCCESS;
}

KStatus TsTable::WriteSnapshotData(kwdbContext_p ctx, const uint64_t range_group_id, uint64_t snapshot_id,
                                   size_t offset, TSSlice data, bool finished) {
  LOG_INFO("WriteSnapshotData begin! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  std::shared_ptr<TsEntityGroup> entity_group_src;
  KStatus s = GetEntityGroup(ctx, range_group_id, &entity_group_src);
  if (s == KStatus::FAIL || entity_group_src == nullptr) {
    LOG_ERROR("GetEntityGroup failed during WriteSnapshotData, range_group_id=%lu, snapshot_id=%lu",
              range_group_id, snapshot_id);
    return KStatus::FAIL;
  }

#ifndef WITH_TESTS
  // The original leader role of the node does not allow snapshot data to be written
  if (entity_group_src->HashRange().typ == 0) {
    LOG_ERROR("The leader role of the original EntityGroup on the node is not allowed to write data, "
              "range_group_id=%lu, snapshot_id=%lu", range_group_id, snapshot_id);
    return KStatus::FAIL;
  }
#endif

  string tbl_sub_path = to_string(range_group_id) + "_" + to_string(snapshot_id);
  string range_tbl_sub_path = db_path_ + tbl_sub_path_;
  KString dir_path = range_tbl_sub_path + tbl_sub_path;
  KString file_name = dir_path + ".sqfs";

#ifndef WITH_TESTS
  std::ofstream write_data_handle(file_name, std::ios::binary | std::ios::app);
  if (!write_data_handle.is_open()) {
    LOG_ERROR("Open file failed during InitWrite, range_group_id[%ld], snapshot_id=%ld.",
              range_group_id, snapshot_id)
    return KStatus::FAIL;
  }
  write_data_handle.write(data.data, data.len);
  if (write_data_handle.fail()) {
    LOG_ERROR("Write compressed file failed, range_group_id=%lu, snapshot_id=%ld", range_group_id, snapshot_id);
    return KStatus::FAIL;
  }
  if (finished) {
    write_data_handle.close();
  }
#endif

  LOG_INFO("Write compressed file data range during WriteSnapshotData[snapshotId:%ld], start address:%ld, end address=%ld",
           snapshot_id, offset, offset + data.len - 1);

  if (finished) {
    KString cmd = "unsquashfs -q -n -d " + dir_path + " " + file_name;
    LOG_INFO("%s", cmd.c_str());
    int result = std::system(cmd.c_str());
    if (result == -1) {
      LOG_ERROR("Uncompressed file failed, range_group_id=%lu", range_group_id);
      return KStatus::FAIL;
    }

    KString rm_cmd = "rm -rf  " + file_name;
    result = std::system(rm_cmd.c_str());
    if (result == -1) {
      LOG_ERROR("Remove compressed file failed, range_group_id=%lu", range_group_id);
      return KStatus::FAIL;
    }
    s = ApplySnapshot(ctx, range_group_id, snapshot_id, true);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  }
  LOG_INFO("WriteSnapshotData success! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  return KStatus::SUCCESS;
}

KStatus TsTable::ApplySnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id, bool delete_after_apply) {
  LOG_INFO("ApplySnapshot begin! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  std::shared_ptr<TsEntityGroup> entity_group_src;
  KStatus s = GetEntityGroup(ctx, range_group_id, &entity_group_src);
  if (s == KStatus::FAIL || entity_group_src == nullptr) {
    LOG_ERROR("GetEntityGroup failed during ApplySnapshot, range_group_id=%lu, snapshot_id=%lu",
              range_group_id, snapshot_id);
    return KStatus::FAIL;
  }

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  SnapshotInfo ts_snapshot_info{};
  ts_snapshot_info.begin_hash = 0;
  ts_snapshot_info.end_hash = UINT64_MAX;
  ts_snapshot_info.type = 1;
  ts_snapshot_info.id = snapshot_id;

  std::shared_ptr<TsTableSnapshot> ts_snapshot_table =
      std::make_shared<TsTableSnapshot>(db_path_, table_id_, tbl_sub_path_, entity_group_src,
                                        entity_bt_manager_, ts_snapshot_info);
  s = ts_snapshot_table->Init(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Apply Snapshot failed, range_group_id=%lu", range_group_id);
    return KStatus::FAIL;
  }

  s = ts_snapshot_table->Apply();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Apply Snapshot failed, range_group_id=%lu", range_group_id);
    return KStatus::FAIL;
  }
  snapshot_id = ts_snapshot_table->GetSnapshotId();
  snapshot_manage_pool_[snapshot_id] = ts_snapshot_table;

  if (delete_after_apply) {
    s = ts_snapshot_table->DropAll();
    if (s == KStatus::FAIL) {
      LOG_ERROR("Drop Snapshot table failed, range_group_id=%lu", range_group_id);
      return KStatus::FAIL;
    }
    snapshot_manage_pool_.erase(snapshot_id);
  }
  LOG_INFO("ApplySnapshot success! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  return KStatus::SUCCESS;
}

KStatus TsTable::EnableSnapshot(kwdbContext_p ctx, uint64_t range_group_id, uint64_t snapshot_id) {
  LOG_INFO("EnableSnapshot begin! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  std::shared_ptr<TsEntityGroup> entity_group;
  KStatus s = GetEntityGroup(ctx, range_group_id, &entity_group);
  if (s == KStatus::FAIL || entity_group == nullptr) {
    LOG_ERROR("GetEntityGroup failed during ApplySnapshot, range_group_id=%lu, snapshot_id=%lu",
              range_group_id, snapshot_id);
    return KStatus::FAIL;
  }
  // set subgroup available
  entity_group->SetAllSubgroupAvailable();
  LOG_INFO("EnableSnapshot success! [Snapshot ID:%ld, Ranggroup ID: %ld]", snapshot_id, range_group_id);
  return KStatus::SUCCESS;
}

KStatus TsTable::DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                     uint64_t* count, uint64_t mtr_id) {
  KStatus s = KStatus::FAIL;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto range_group = entity_groups_.find(range_group_id);
  if (range_group == entity_groups_.end()) {
    LOG_ERROR("hash range not found, range_group_id=%lu", range_group_id);
    return s;
  }
  s = range_group->second->DeleteRangeEntities(ctx, hash_span, count, mtr_id);
  return s;
}

KStatus TsTable::DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                                 const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsEntityGroup> table_range;
  auto s = GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetEntityGroup failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return s;
  }

  if (table_range) {
    s = table_range->DeleteRangeData(ctx, hash_span, 0, ts_spans, nullptr, count, mtr_id, false);
    if (s == KStatus::FAIL) {
      LOG_ERROR("DeleteRangeData failed, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
                table_id_, range_group_id, hash_span.begin, hash_span.end)
      return s;
    } else {
      LOG_INFO("DeleteRangeData succeed, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
               table_id_, range_group_id, hash_span.begin, hash_span.end);
      return KStatus::SUCCESS;
    }
  } else {
    LOG_ERROR("DeleteRangeData failed, range_group disappear, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
              table_id_, range_group_id, hash_span.begin, hash_span.end)
    return KStatus::FAIL;
  }
}

KStatus TsTable::DeleteData(kwdbContext_p ctx, uint64_t range_group_id, std::string& primary_tag,
                            const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsEntityGroup> table_range;
  auto s = GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetEntityGroup failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return s;
  }

  if (table_range) {
    s = table_range->DeleteData(ctx, primary_tag, 0, ts_spans, nullptr, count, mtr_id, false);
    if (s == KStatus::FAIL) {
      LOG_ERROR("DeleteData failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
      return s;
    } else {
      LOG_INFO("DeleteData succeed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id);
      return KStatus::SUCCESS;
    }
  } else {
    LOG_ERROR("DeleteData failed, range_group disappear, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return KStatus::FAIL;
  }
}

KStatus TsTable::DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts) {
  KStatus s = KStatus::FAIL;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  // Traverse through all entity groups to delete expired data
  for (auto& entity_group : entity_groups_) {
    s = entity_group.second->DeleteExpiredData(ctx, end_ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("EntityGroup delete expired data failed.");
      return s;
    }
  }
  return s;
}

KStatus TsTable::GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                                 const std::vector<uint32_t>& scan_tags,
                                 std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    // if (tbl_range.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(tbl_range.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (tbl_range.second->hashRange().typ != EntityGroupType::LEADER) {
    //   // not leader
    //   continue;
    // }
    tbl_range.second->GetEntityIdList(ctx, primary_tags, scan_tags, entity_id_list, res, count);
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::vector<uint32_t> hps,
                                TagIterator** iter, k_uint32 table_version) {
  std::vector<EntityGroupTagIterator*> eg_tag_iters;
  EntityGroupTagIterator* eg_tag_iter = nullptr;

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    // if (tbl_range.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                     "] range group[" + std::to_string(tbl_range.first) +
    //                     "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (tbl_range.second->hashRange().typ != EntityGroupType::LEADER) {
    //   // not leader
    //   continue;
    // }
    if (!EngineOptions::isSingleNode()) {
      tbl_range.second->GetTagIterator(ctx, scan_tags, &eg_tag_iter, hps);
    } else {
      tbl_range.second->GetTagIterator(ctx, scan_tags, &eg_tag_iter);
    }

    if (!eg_tag_iter) {
      return KStatus::FAIL;
    }
    eg_tag_iters.emplace_back(std::move(eg_tag_iter));
    eg_tag_iter = nullptr;
  }

  TagIterator* tag_iter = new TagIterator(eg_tag_iters);
  if (KStatus::SUCCESS != tag_iter->Init()) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetMetaIterator(kwdbContext_p ctx, MetaIterator** iter, k_uint32 table_version) {
  std::vector<EntityGroupMetaIterator*> iters;
  EntityGroupMetaIterator* eg_meta_iter = nullptr;
  for (const auto tbl_range : entity_groups_) {
    // if (tbl_range.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(tbl_range.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (tbl_range.second->hashRange().typ != EntityGroupType::LEADER) {
    //   // not leader
    //   continue;
    // }
    if (KStatus::SUCCESS != tbl_range.second->GetMetaIterator(ctx, &eg_meta_iter)) {
      continue;
    }
    iters.emplace_back(std::move(eg_meta_iter));
    eg_meta_iter = nullptr;
  }
  MetaIterator* meta_iter = new MetaIterator(iters);
  if (KStatus::SUCCESS != meta_iter->Init()) {
    delete meta_iter;
    meta_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = meta_iter;
  return KStatus::SUCCESS;
}

KStatus TsTable::TSxClean(kwdbContext_p ctx) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& tbl_range : entity_groups_) {
    KStatus s = tbl_range.second->TSxClean(ctx);
    if (s == FAIL) {
      LOG_ERROR("Failed to clean the entity group %lu of table %lu",
                tbl_range.second->HashRange().range_group_id, table_id_)
      return s;
    }
  }

  return SUCCESS;
}

void TsTable::UpdateTagTsVersion(uint32_t new_version) {
  for (auto& entity_group : entity_groups_) {
    entity_group.second->UpdateTagVersion(new_version);
  }
}

KStatus TsTable::AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                            uint32_t cur_version, uint32_t new_version, string& msg) {
  AttributeInfo attr_info;
  KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, *column, attr_info, false);
  if (s != KStatus::SUCCESS) {
    msg = "Unknown column/tag type";
    return s;
  }
  if (alter_type == AlterType::ALTER_COLUMN_TYPE) {
    getDataTypeSize(attr_info);  // update max_len
  }
  if (attr_info.isAttrType(COL_GENERAL_TAG)) {
    s = AlterTableTag(ctx, alter_type, attr_info, cur_version, new_version, msg);
  } else if (attr_info.isAttrType(COL_TS_DATA)) {
    s = AlterTableCol(ctx, alter_type, attr_info, cur_version, new_version, msg);
  }

  return s;
}

KStatus TsTable::AlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                               uint32_t cur_version, uint32_t new_version, string& msg) {
  KStatus s;
  ErrorInfo err_info;
  TagInfo tag_schema = {attr_info.id, attr_info.type,
                        static_cast<uint32_t>(attr_info.length), 0,
                        static_cast<uint32_t>(attr_info.size), GENERAL_TAG};
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    switch (alter_type) {
      case ADD_COLUMN:
        s = entity_group.second->AddTagInfo(ctx, tag_schema, err_info, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("add tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          msg = err_info.errmsg;
          return s;
        }
        break;
      case DROP_COLUMN:
        s = entity_group.second->DropTagInfo(ctx, tag_schema, err_info, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("drop tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          msg = err_info.errmsg;
          return s;
        }
        break;
      case ALTER_COLUMN_TYPE:
        s = entity_group.second->AlterTagInfo(ctx, tag_schema, err_info, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("alter tag type failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          msg = err_info.errmsg;
          return s;
        }
        break;
      default:
        return KStatus::FAIL;
    }
  }
  s = entity_bt_manager_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    msg = "Update table version error";
    return s;
  }
  return SUCCESS;
}

KStatus TsTable::AlterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                               uint32_t cur_version, uint32_t new_version, string& msg) {
  ErrorInfo err_info;
  auto col_idx = entity_bt_manager_->GetColumnIndex(attr_info);
  auto latest_version = entity_bt_manager_->GetCurrentTableVersion();
  vector<AttributeInfo> schema;
  KStatus s = entity_bt_manager_->GetSchemaInfoIncludeDropped(&schema, cur_version);
  if (s != KStatus::SUCCESS) {
    msg = "schema version " + to_string(cur_version) + " does not exists";
    return s;
  }
  switch (alter_type) {
    case ADD_COLUMN:
      if ((col_idx >= 0) && (latest_version == new_version)) {
        return KStatus::SUCCESS;
      }
      schema.emplace_back(attr_info);
      break;
    case DROP_COLUMN:
      if ((col_idx < 0) && (latest_version == new_version)) {
        return KStatus::SUCCESS;
      }
      schema[col_idx].setFlag(AINFO_DROPPED);
      break;
    case ALTER_COLUMN_TYPE: {
      if (col_idx < 0) {
        LOG_ERROR("alter column type failed: column (id %u) does not exists, table id = %lu", attr_info.id, table_id_);
        msg = "column does not exist";
        return KStatus::FAIL;
      } else if (latest_version == new_version) {
        return KStatus::SUCCESS;
      }
      auto& col_info = schema[col_idx];
      col_info.type = attr_info.type;
      col_info.size = attr_info.size;
      col_info.length = attr_info.length;
      col_info.max_len = attr_info.max_len;
      break;
    }
    default:
      return KStatus::FAIL;
  }
  s = entity_bt_manager_->CreateRootTable(schema, new_version, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    msg = err_info.errmsg;
    LOG_ERROR("add new version schema failed for alter table: table id = %lu, new_version = %u",  table_id_, new_version);
    return s;
  }
  UpdateTagTsVersion(new_version);
  return SUCCESS;
}

KStatus TsTable::AddSchemaVersion(kwdbContext_p ctx, roachpb::CreateTsTable* meta, MMapMetricsTable ** version_schema) {
  *version_schema = nullptr;
  KStatus s;
  ErrorInfo err_info;
  auto latest_version = entity_bt_manager_->GetCurrentTableVersion();
  auto upper_version = meta->ts_table().ts_version();
  // skip in case current table has that schema version
  LOG_DEBUG("uppper version[%u], latest schema version [%u]", upper_version, latest_version);
  if (upper_version <= latest_version) {
    auto root_table = entity_bt_manager_->GetRootTable(upper_version, true);
    if (root_table != nullptr) {
      *version_schema = root_table;
      return KStatus::SUCCESS;
    }
    LOG_DEBUG("uppper version[%u] no exists, need create.", upper_version);
  }

  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    s = TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                             static_cast<uint32_t>(col_var.length), 0,
                                             static_cast<uint32_t>(col_var.size),
                                             col_var.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG}));
    } else {
      metric_schema.push_back(std::move(col_var));
    }
  }
  if (upper_version > latest_version) {
    s = entity_bt_manager_->CreateRootTable(metric_schema, upper_version, err_info);
  } else {
    s = entity_bt_manager_->AddRootTable(metric_schema, upper_version, err_info);
  }
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("add new version schema failed for alter table: table id = %lu, new_version = %u, err_msg: %s",
              table_id_, upper_version, err_info.errmsg.c_str());
    return s;
  }
  *version_schema = entity_bt_manager_->GetRootTable(upper_version, true);
  // todo(liangbo01)  tag version change, also need upper tag schema.
  UpdateTagTsVersion(upper_version);
  return SUCCESS;
}

KStatus TsTable::UndoAlterTable(kwdbContext_p ctx, LogEntry* log) {
  auto alter_log = reinterpret_cast<DDLAlterEntry*>(log);
  auto alter_type = alter_log->getAlterType();
  switch (alter_type) {
    case AlterType::ADD_COLUMN:
    case AlterType::DROP_COLUMN:
    case AlterType::ALTER_COLUMN_TYPE: {
      auto cur_version = alter_log->getCurVersion();
      auto new_version = alter_log->getNewVersion();
      auto slice = alter_log->getColumnMeta();
      roachpb::KWDBKTSColumn column;
      bool res = column.ParseFromArray(slice.data, slice.len);
      if (!res) {
        LOG_ERROR("Failed to parse the WAL log")
        return KStatus::FAIL;
      }
      if (undoAlterTable(ctx, alter_type, &column, cur_version, new_version) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
    case ALTER_PARTITION_INTERVAL: {
      uint64_t interval = 0;
      memcpy(&interval, alter_log->getData(), sizeof(interval));
      if (AlterPartitionInterval(ctx, interval) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::undoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                                uint32_t cur_version, uint32_t new_version) {
  AttributeInfo attr_info;
  ErrorInfo err_info;
  KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, *column, attr_info, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  if (alter_type == AlterType::ALTER_COLUMN_TYPE) {
    getDataTypeSize(attr_info);  // update max_len
  }

  if (attr_info.isAttrType(COL_GENERAL_TAG)) {
    s = undoAlterTableTag(ctx, alter_type, attr_info, cur_version, new_version);
  } else if (attr_info.isAttrType(COL_TS_DATA)) {
    s = undoAlterTableCol(ctx, cur_version, new_version);
  }

  return s;
}

KStatus TsTable::undoAlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                   uint32_t cur_version, uint32_t new_version) {
  KStatus s;
  TagInfo tag_schema = {attr_info.id, attr_info.type,
                        static_cast<uint32_t>(attr_info.length), 0,
                        static_cast<uint32_t>(attr_info.size), GENERAL_TAG};
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    switch (alter_type) {
      case AlterType::ADD_COLUMN:
        s = entity_group.second->UndoAddTagInfo(ctx, tag_schema, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("undo add tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          return s;
        }
        break;
      case AlterType::DROP_COLUMN:
        s = entity_group.second->UndoDropTagInfo(ctx, tag_schema, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("undo drop tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          return s;
        }
        break;
      case AlterType::ALTER_COLUMN_TYPE:
        s = entity_group.second->UndoAlterTagInfo(ctx, tag_schema, new_version);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("undo alter tag type failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          return s;
        }
        break;
      default:
        return KStatus::FAIL;
    }
  }
  if (entity_bt_manager_->RollBack(cur_version, new_version) < 0) {
    LOG_ERROR("undo alter tag failed: rollback metric table[%lu] version failed, new version[%d]", table_id_, new_version);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::undoAlterTableCol(kwdbContext_p ctx, uint32_t cur_version, uint32_t new_version) {
  auto s = entity_bt_manager_->RollBack(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  UpdateTagTsVersion(cur_version);
  return KStatus::SUCCESS;
}

KStatus TsTable::AlterPartitionInterval(kwdbContext_p ctx, uint64_t partition_interval) {
  entity_bt_manager_->SetPartitionInterval(partition_interval);
  return KStatus::SUCCESS;
}

uint64_t TsTable::GetPartitionInterval() {
  return entity_bt_manager_->GetPartitionInterval();
}

void TsTable::SetDropped() {
  is_dropped_.store(true);
  entity_bt_manager_->SetDropped();
}

bool TsTable::IsDropped() {
  return is_dropped_.load();
}

uint64_t beginOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;  // 24 hours * 60 minutes * 60 seconds * 1000 ms
  return timestamp - (timestamp % day_ms);
}

uint64_t endOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;
  return beginOfDay(timestamp) + day_ms;
}

KStatus TsTable::CompactData(kwdbContext_p ctx, uint64_t range_group_id, const KwTsSpan& ts_span) {
  // refer to CreateSnapshot
  EnterFunc()
  KStatus s = KStatus::FAIL;
  LOG_INFO("CompactData begin! [Range group ID: %ld]", range_group_id);
  std::shared_ptr<TsEntityGroup> entity_group_src;

  s = GetEntityGroup(ctx, range_group_id, &entity_group_src);
  if (s == KStatus::FAIL || entity_group_src == nullptr) {
    // Add error messages
    LOG_ERROR("GetEntityGroup failed during CompactData, range_group_id=%lu", range_group_id);
    Return(KStatus::FAIL);
  }

  KwTsSpan ts_span_slot;
  // Assuming that the partition time is 1 day, if the incoming time range is from 8:00 on 1st to 5:00 on 4th,
  // the data of 2nd and 3rd will be reorganized, but the data of 1st and 4th will not be reorganized
  ts_span_slot.begin = endOfDay(ts_span.begin);
  ts_span_slot.end = beginOfDay(ts_span.end);
  if (ts_span_slot.begin >= ts_span_slot.end) {
    LOG_ERROR("ts_span range is invalid after conversion: %lu~%lu, range_group_id=%lu",
              ts_span_slot.begin, ts_span_slot.end, range_group_id);
    Return(s);
  }

  SnapshotInfo ts_snapshot_info;
  // Tips: type = 0, source node build snapshot, need to generate snapshot id
  auto now = std::chrono::system_clock::now();
  // Converts time to milliseconds that have elapsed since January 1, 1970
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  ts_snapshot_info.id = timestamp;

  ts_snapshot_info.begin_hash = 0;
  ts_snapshot_info.end_hash = UINT64_MAX;
  ts_snapshot_info.type = 0;  // Build a new EntityGroup(snapshot) at source node
  ts_snapshot_info.reorder = true;
  ts_snapshot_info.partition_ts = ts_span_slot;
  std::shared_ptr<TsTableSnapshot> ts_snapshot_table =
      std::make_shared<TsTableSnapshot>(db_path_, table_id_, tbl_sub_path_,
                                        entity_group_src, entity_bt_manager_, ts_snapshot_info);
  s = ts_snapshot_table->Init(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Init Snapshot table failed, range_group_id=%lu", range_group_id);
    Return(KStatus::FAIL)
  }
  std::map<SubGroupID, std::vector<uint32_t>> subgroup_row;  // for genMigratePayloadByBuilder

  // map[subgroup_id][partition_ts][entity_id]block_id, records each entity's max block_id when data reorganization
  std::map<SubGroupID, std::map<timestamp64, std::map<uint32_t, BLOCK_ID>>> obsolete_max_block;

  // map[subgroup_id][partition_ts] {segment_id...}, records segment_ids from source table that will be reorganized
  std::map<SubGroupID, std::map<timestamp64, std::vector<BLOCK_ID>>> obsolete_segment_ids;

  // map[subgroup_id]{KwTsSpan...}, records all the partitions of each subgroup that will be reorganized,
  // and convert them to KwTsSpan for BuildCompactData
  std::map<SubGroupID, std::vector<KwTsSpan>> subgroup_ts_span;
  ts_snapshot_table->PrepareCompactData(ctx, obsolete_max_block, obsolete_segment_ids, subgroup_row, subgroup_ts_span);

//  if obsolete_max_block.size() > 0 or subgroup_row.size() > 0 represents that there is data that needs to be reorganized,
//  and if obsolete_segment_ids.size() > 0, represents that there are segments that needs to be dropped,
//  If there is data that needs to be reorganized, then there must be segments that need to be deleted.
//  But not vice versa, because the data of the segments may have been all deleted.
//  That is to say, subgroup_ids of `obsolete_segment_ids` must contain subgroup_ids of `obsolete_max_block`,
//  so we need to loop obsolete_segment_ids.

  if (obsolete_segment_ids.empty()) {
    s = ts_snapshot_table->DropAll();
    if (s != KStatus::SUCCESS) {
      LOG_INFO("snapshot DropAll ! [Snapshot ID:%ld, Range group ID: %ld]",
               ts_snapshot_table->GetSnapshotId(), range_group_id);
      Return(KStatus::FAIL)
    }
    Return(KStatus::SUCCESS)
  }
  for (auto it = obsolete_segment_ids.begin(); it != obsolete_segment_ids.end(); it++) {
    SubGroupID subgroup_id = it->first;
    if (subgroup_row.find(subgroup_id) != subgroup_row.end()) {
      LOG_INFO("BuildCompactData start! [Snapshot ID:%ld, Range group ID: %ld]",
               ts_snapshot_table->GetSnapshotId(), range_group_id);
      s = ts_snapshot_table->BuildCompactData(ctx, subgroup_row[subgroup_id], subgroup_ts_span[subgroup_id]);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("BuildCompactData failed, range_group_id=%lu", range_group_id);
        ts_snapshot_table->DropAll();
        Return(s);
      }
      LOG_INFO("BuildCompactData success! [Snapshot ID:%ld, Range group ID: %ld]",
               ts_snapshot_table->GetSnapshotId(), range_group_id);
    }

    RW_LATCH_X_LOCK(entity_groups_mtx_);
    LOG_INFO("ApplyCompactData start! [Snapshot ID:%ld, Range group ID: %ld]",
             ts_snapshot_table->GetSnapshotId(), range_group_id);
    // entity_ids of different tags may belong to one subgroup_id, when applying all reorganized partitions
    // under a subgroup, it also includes all entity_ids within the time range, so each subgroup only needs
    // to select one entity_id to apply and all entities in the same group will be processed.

    s = ts_snapshot_table->ApplyCompactData(ctx, subgroup_id, obsolete_max_block[subgroup_id], it->second);
    if (s != KStatus::SUCCESS) {
      RW_LATCH_UNLOCK(entity_groups_mtx_);
      LOG_ERROR("ApplyCompactData failed, range_group_id=%lu", range_group_id);
      ts_snapshot_table->DropAll();
      Return(KStatus::FAIL)
    }
    LOG_INFO("ApplyCompactData success! [Snapshot ID:%ld, Range group ID: %ld]",
             ts_snapshot_table->GetSnapshotId(), range_group_id);
    RW_LATCH_UNLOCK(entity_groups_mtx_);
  }
  s = ts_snapshot_table->DropAll();
  if (s != KStatus::SUCCESS) {
    LOG_INFO("snapshot DropAll ! [Snapshot ID:%ld, Range group ID: %ld]",
             ts_snapshot_table->GetSnapshotId(), range_group_id);
    Return(KStatus::FAIL)
  }
  Return(KStatus::SUCCESS)
}

KStatus TsTable::GetEntityNum(kwdbContext_p ctx, uint64_t* entity_num) {
  *entity_num = 0;
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  for (auto& it : entity_groups_) {
    *entity_num += it.second->GetTagCount();
  }
  RW_LATCH_UNLOCK(entity_groups_mtx_);
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataRowNum(kwdbContext_p ctx, const KwTsSpan& ts_span, uint64_t* row_num) {
  LOG_INFO("GetDataRowNum begin, table[%lu] time [%ld - %ld]", table_id_, ts_span.begin, ts_span.end);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  *row_num = 0;
  // get all entity group
  std::vector<std::shared_ptr<TsEntityGroup>> entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    for (auto &it : entity_groups_) {
      it.second->RdDropLock();
      entity_groups.push_back(it.second);
    }
    RW_LATCH_UNLOCK(entity_groups_mtx_);
  }
  ErrorInfo err_info;
  timestamp64 block_min_ts = INT64_MAX;
  timestamp64 block_max_ts = INT64_MIN;
  KwTsSpan p_span{ts_span.begin / 1000, ts_span.end / 1000};
  // Traverse all entity groups
  for (int entity_group_idx = 0; entity_group_idx < entity_groups.size(); ++entity_group_idx) {
    auto& entity_group = entity_groups[entity_group_idx];
    Defer defer([&]{entity_group->DropUnlock();});
    auto subgroup_manager = entity_group->GetSubEntityGroupManager();
    // Traverse all subgroups
    for (SubGroupID sub_group_id = 1; sub_group_id <= subgroup_manager->GetMaxSubGroupId(); ++sub_group_id) {
      TsSubEntityGroup* subgroup = subgroup_manager->GetSubGroup(sub_group_id, err_info);
      if (!subgroup) {
        err_info.clear();
        continue;
      }
      if (subgroup && !subgroup->IsAvailable()) {
        err_info.setError(KWEOTHER, "subgroup[" + std::to_string(sub_group_id) + "] is unavailable");
        LOG_ERROR("subgroup is unavailable");
        break;
      }
      auto partition_tables = subgroup->GetPartitionTables(p_span, err_info);
      if (err_info.errcode < 0) {
        LOG_ERROR("GetPartitionTables failed, error_info: %s", err_info.errmsg.c_str());
        break;
      }
      // Traverse all partitions
      for (auto& partition : partition_tables) {
        usleep(10);
        if (!partition || partition->getObjectStatus() != OBJ_READY) {
          err_info.setError(KWEOTHER, "partition table is nullptr or not ready");
          break;
        }
        std::shared_ptr<MMapSegmentTable> segment_table = nullptr;
        for (BLOCK_ID block_id = 1; block_id <= partition->GetMaxBlockID(); ++block_id) {
          BlockItem* block_item = partition->GetBlockItem(block_id);
          // Skip deleted entity
          if (partition->getEntityItem(block_item->entity_id)->is_deleted) {
            continue;
          }
          // Switch segment table
          if (!segment_table || block_id >= segment_table->segment_id() + segment_table->metaData()->block_num_of_segment) {
            segment_table = partition->getSegmentTable(block_item->block_id);
            if (!segment_table) {
              continue;
            }
          }
          // Calculate the number of rows
          timestamp64 max_ts = segment_table->getBlockMaxTs(block_item->block_id);
          timestamp64 min_ts = segment_table->getBlockMinTs(block_item->block_id);
          timestamp64 intersect_ts = intersectLength(ts_span.begin, ts_span.end, min_ts, max_ts + 1);
          uint32_t count = 1.0L * intersect_ts / (max_ts - min_ts + 1) * block_item->publish_row_count;
          if (intersect_ts > 0 && block_item->publish_row_count > 0 && count == 0) {
            // The estimated data volume is 0, but in cases where there is a very small amount of data,
            // it is considered as one piece of data by default
            count = 1;
          }
          *row_num += count;
          // Calculate the maximum and minimum time range of all blocks
          if (count != 0 && max_ts > block_max_ts) {
            block_max_ts = max_ts;
          }
          if (count != 0 && min_ts < block_min_ts) {
            block_min_ts = min_ts;
          }
        }
      }
      for (auto& partition : partition_tables) {
        if (partition) {
          ReleaseTable(partition);
        }
      }
      if (err_info.errcode < 0) {
        break;
      }
    }
    if (err_info.errcode < 0) {
      break;
    }
  }
  if (err_info.errcode < 0) {
    *row_num = 0;
    return KStatus::FAIL;
  }
  // If the queried data falls within the ts_stpan range (such as in a scenario where data has just been written),
  // it is necessary to estimate the daily data write volume
  if (block_min_ts != INT64_MAX && block_min_ts > ts_span.begin) {
    double ratio = 1.0L * (ts_span.end - block_min_ts) / (ts_span.end - ts_span.begin);
    *row_num = (*row_num) / ratio;
  }

  LOG_INFO("GetDataRowNum succeeded");
  return KStatus::SUCCESS;
}

uint32_t TsTable::GetCurrentTableVersion() {
  return entity_bt_manager_->GetCurrentTableVersion();
}

KStatus TsEntityGroup::getTagTable(ErrorInfo& err_info) {
  if (tag_bt_ == nullptr) {
    tag_bt_ = OpenTagTable(db_path_, tbl_sub_path_, table_id_, err_info);
  }
  if (tag_bt_ == nullptr) {
    LOG_ERROR("open tag table error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  tag_bt_->mutexLock();
  bool is_valid = tag_bt_->isValid();
  tag_bt_->mutexUnlock();
  if (!is_valid) {
    // avoid droping tag table
    err_info.errcode = -2;
    err_info.errmsg = "tag table status is invalid";
    LOG_ERROR("tag table status is invalid");
    return KStatus::FAIL;
  }
  tag_bt_->incRefCount();
  return KStatus::SUCCESS;
}

void TsEntityGroup::releaseTagTable() {
  if (tag_bt_ == nullptr) {
    LOG_WARN("release tag table object is nullptr.");
    return;
  }
  int ref_cnt = tag_bt_->decRefCount();
  if (ref_cnt <= 1) {
    KW_COND_SIGNAL(tag_bt_->m_ref_cnt_cv_);
  }
}

bool TsEntityGroup::payloadNextSlice(TsSubEntityGroup* sub_group, Payload& payload, timestamp64 last_p_time, int start_row,
                                     int32_t* end_row, timestamp64* cur_p_time) {
  // Inspect whether the initial row falls outside the data range
  if (start_row >= payload.GetRowCount()) {
    return false;
  }
  timestamp64 p_time = 0;

  // Search through the rows in the payload to locate the next partition time with a different timestamp
  for (int32_t row_id = start_row; row_id < payload.GetRowCount(); row_id++) {
    KTimestamp cur_ts_ms = payload.GetTimestamp(row_id);
    KTimestamp cur_ts = cur_ts_ms / 1000;

    timestamp64 max_ts;

    // Calculate the partition time for the current row and compare it with the previous partition time.
    // If there is a difference, it indicates the need to switch partitions.
    // First, return the information of the current partition to the upper layer for writing,
    // and then continue to search for the next partition.
    p_time = sub_group->PartitionTime(cur_ts, max_ts);
    if (p_time != last_p_time) {
      *cur_p_time = p_time;
      *end_row = row_id;
      return true;  // Locate the next valid partition time
    }
  }

  // If no partition time change is found after traversing all rows, set the last row as the end row
  // and return the current timestamp
  *end_row = payload.GetRowCount();
  *cur_p_time = p_time;
  return true;
}

bool TsEntityGroup::findPartitionPayload(TsSubEntityGroup* sub_group, Payload& payload,
                                         std::multimap<timestamp64, PartitionPayload>* partition_map) {
  // Inspect whether the initial row falls outside the data range
  int start_row = payload.GetStartRowId();
  KTimestamp first_ts_ms = payload.GetTimestamp(payload.GetStartRowId());
  KTimestamp first_ts = first_ts_ms / 1000;
  timestamp64 first_max_ts;

  timestamp64 last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);

  if (start_row >= payload.GetRowCount()) {
    return false;
  }
  timestamp64 p_time = 0;
  // Search through the rows in the payload to locate the next partition time with a different timestamp
  for (int32_t row_id = payload.GetStartRowId(); row_id < payload.GetRowCount(); row_id++) {
    KTimestamp cur_ts_ms = payload.GetTimestamp(row_id);
    KTimestamp cur_ts = cur_ts_ms / 1000;

    timestamp64 max_ts;
    // Calculate the partition time for the current row and compare it with the previous partition time.
    // If there is a difference, it indicates the need to switch partitions.
    // First, return the information of the current partition to the upper layer for writing,
    // and then continue to search for the next partition.
    p_time = sub_group->PartitionTime(cur_ts, max_ts);
    if (p_time != last_p_time) {
      partition_map->insert({p_time, {start_row, row_id}});
      start_row = row_id;
    }
  }
  // If no partition time change is found after traversing all rows, set the last row as the end row and
  // return the current timestamp

  auto end_row = payload.GetRowCount();
  partition_map->insert({p_time, {start_row, end_row}});
  return true;
}

KStatus TsEntityGroup::FlushBuffer(kwdbContext_p ctx) {
  return SUCCESS;
}

KStatus TsEntityGroup::CreateCheckpoint(kwdbContext_p ctx) {
  return SUCCESS;
}

}  //  namespace kwdbts
