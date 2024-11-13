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

#include <cstdio>
#include <algorithm>
#include <cstring>
#include <atomic>
#include <sys/mman.h>
#include <stdexcept>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
#else
  #include <filesystem>
#endif
#include <st_config.h>

#include "cm_func.h"
#include "dirent.h"
#include "ts_time_partition.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "engine.h"
#include "utils/compress_utils.h"
#include "lg_api.h"
#include "perf_stat.h"


void markDeleted(char* delete_flags, size_t row_index) {
  size_t byte = (row_index - 1) >> 3;
  size_t bit = (row_index - 1) & 7;
  delete_flags[byte] |= (1 << bit);
}

inline bool ReachMetaMaxBlock(BLOCK_ID cur_block_id) {
  return cur_block_id >= INT32_MAX;
}

impl_latch_virtual_func(TsTimePartition, vacuum_query_lock_)

TsTimePartition::~TsTimePartition() {
  meta_manager_.release();
  releaseSegments();
  if (vacuum_query_lock_) {
    delete vacuum_query_lock_;
    vacuum_query_lock_ = nullptr;
  }
  if (vacuum_insert_lock_) {
    delete vacuum_insert_lock_;
    vacuum_insert_lock_ = nullptr;
  }
  if (vacuum_delete_lock_) {
    delete vacuum_delete_lock_;
    vacuum_delete_lock_ = nullptr;
  }
  if (active_segment_lock_) {
    delete active_segment_lock_;
    active_segment_lock_ = nullptr;
  }
  delete m_ref_cnt_mtx_;
  delete m_ref_cnt_cv_;
  m_ref_cnt_cv_ = nullptr;
  m_ref_cnt_mtx_ = nullptr;
}

int TsTimePartition::open(const string& path, const std::string& db_path, const string& tbl_sub_path,
                          int flags, ErrorInfo& err_info) {
  assert(root_table_manager_ != nullptr);
  file_path_ = getTsFilePath(path);
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  name_ = getTsObjectName(file_path_);

  // load EntityMeta
  if (openBlockMeta(flags, err_info) < 0) {
    return err_info.errcode;
  }

  if (DeleteFlag()) {
    err_info.setError(KWEDROPPEDOBJ, "TsTimePartition[" + tbl_sub_path_ +  "] is deleted");
    return err_info.errcode;
  }

  loadSegments(err_info);
  setObjectReady();

  return err_info.errcode;
}

int TsTimePartition::loadSegment(BLOCK_ID segment_id, ErrorInfo& err_info) {
  std::shared_ptr<MMapSegmentTable> segment_tbl;
  MMapSegmentTable *tbl = new MMapSegmentTable();
  string file_path = name_ + ".bt";
  if (tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_tbl_sub_path(segment_id),
                MMAP_OPEN_NORECURSIVE, true, err_info) < 0) {
    delete tbl;
    err_info.errmsg = "open segment table failed. " + segment_tbl_sub_path(segment_id);
    err_info.errcode = KWENFILE;
    return err_info.errcode;
  }
  segment_tbl.reset(tbl);
  data_segments_.Insert(segment_id, segment_tbl);
  return 0;
}

std::shared_ptr<MMapSegmentTable> TsTimePartition::reloadSegment(std::shared_ptr<MMapSegmentTable> old_segment,
                                                                 bool lazy_open, ErrorInfo& err_info) {
  std::shared_ptr<MMapSegmentTable> segment_tbl;
  MMapSegmentTable *tbl = new MMapSegmentTable();
  string file_path = name_ + ".bt";
  BLOCK_ID segment_id = old_segment->segment_id();
  if (tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_tbl_sub_path(segment_id),
                MMAP_OPEN_NORECURSIVE, lazy_open, err_info) < 0) {
    delete tbl;
    err_info.errmsg = "open segment table failed. " + segment_tbl_sub_path(segment_id);
    err_info.errcode = KWENFILE;
    old_segment->mutexUnlock();
    return segment_tbl;
  }
  segment_tbl.reset(tbl);
  data_segments_.Insert(segment_id, segment_tbl);
  old_segment->setNotLatestOpened();
  return segment_tbl;
}

int TsTimePartition::loadSegments(ErrorInfo& err_info) {
  releaseSegments();
  string real_path = db_path_ + tbl_sub_path_;
  set<BLOCK_ID> segment_ids;
  BLOCK_ID last_segment_id = 0;
  // Scan and load existing segments
  {
    DIR* dir_ptr = opendir(real_path.c_str());
    if (dir_ptr) {
      struct dirent* entity;
      while ((entity = readdir(dir_ptr)) != nullptr) {
        BLOCK_ID segment_id = 0;
        if (entity->d_type == DT_REG) {
          int dn_len = strlen(entity->d_name);
          dn_len = dn_len - 5;
          if (strcmp(entity->d_name + dn_len, ".sqfs") == 0) {
            string part_name = string(entity->d_name, dn_len);
            segment_id = stoi(part_name);
          }
        } else if (entity->d_type == DT_DIR) {
          if (strcmp(entity->d_name, ".") == 0 || strcmp(entity->d_name, "..") == 0
              || entity->d_name[0] == '_') {  // _log directory  _tmp directory
            continue;
          }
          segment_id = std::stoi(entity->d_name);
        }
        if (segment_id > 0 && segment_ids.count(segment_id) == 0) {
          int err_code = loadSegment(segment_id, err_info);
          if (err_code < 0) {
            break;
          }
          segment_ids.insert(segment_id);
          if (segment_id > last_segment_id) {
            last_segment_id = segment_id;
          }
        }
      }
      closedir(dir_ptr);
    }
  }

  if (err_info.errcode < 0) {
    releaseSegments();
    return err_info.errcode;
  }

  std::shared_ptr<MMapSegmentTable> last_sta_tbl = nullptr;
  if (!segment_ids.empty()) {
    std::shared_ptr<MMapSegmentTable> segment_table = getSegmentTable(last_segment_id, true);
    if (segment_table && !segment_table->sqfsIsExists()
        && segment_table->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), last_segment_id, name_ + ".bt",
                               db_path_, tbl_sub_path_ + std::to_string(last_segment_id) + "/", MMAP_OPEN_NORECURSIVE,
                               false, err_info) >= 0
        && segment_table->getSegmentStatus() == ActiveSegment) {
      last_sta_tbl = segment_table;
    }
  }

  if (last_sta_tbl != nullptr) {
    active_segment_ = last_sta_tbl;
  }
  return err_info.errcode;
}

int TsTimePartition::init(const vector<AttributeInfo>& schema, int encoding, bool init_data, ErrorInfo& err_info) {
  return err_info.errcode;
}

MMapSegmentTable* TsTimePartition::createSegmentTable(BLOCK_ID segment_id,  uint32_t table_version, ErrorInfo& err_info,
  uint32_t max_rows_per_block, uint32_t max_blocks_per_segment) {
  // Check if the metadata manager has a valid first meta entry.
  if (meta_manager_.GetFirstMeta() == nullptr) {
    err_info.setError(KWEINVALPATH, db_path_ + tbl_sub_path_ + " is not data path.");
    return nullptr;
  }
  string segment_sand = segment_tbl_sub_path(segment_id);
  string dir_path = db_path_ + segment_sand;
  // Attempt to access the directory, create it if it doesn't exist.
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path, err_info)) {
      return nullptr;
    }
  }
  MMapSegmentTable* segment_tbl = new MMapSegmentTable();

  // Define a cleanup action to handle errors and resource cleanup.
  Defer defer{[&]() {
    if (err_info.errcode < 0) {
      if (err_info.errcode != KWEEXIST) {
        segment_tbl->remove();
      }
      delete segment_tbl;
    }
  }};
  // Prepare file path and attempt to open the segment table with exclusive creation.
  string file_path = name_ + ".bt";
  if (segment_tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_sand, MMAP_CREAT_EXCL, true, err_info) < 0) {
    return nullptr;
  }

  vector<string> key = {};
  string key_order = "";
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  {
    std::vector<AttributeInfo> schema;
    root_table_manager_->GetSchemaInfoIncludeDropped(&schema, table_version);
    int ret = segment_tbl->create(&meta_manager_, schema, table_version, encoding, err_info, max_rows_per_block, max_blocks_per_segment);
    if (ret < 0) {
      return nullptr;
    }
  }
  // reserve files to has enough space storing data
  err_info.errcode = segment_tbl->reserve(segment_tbl->getReservedRows());
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode, segment_sand);
    return nullptr;
  }
  segment_tbl->minTimestamp() = INVALID_TS;
  segment_tbl->maxTimestamp() = INVALID_TS;
  return segment_tbl;
}

int TsTimePartition::openBlockMeta(const int flags, ErrorInfo& err_info) {
  string meta_name = name_ + ".meta";
  // if create segment , we need create meta.0 file first.
  if (!(flags & O_CREAT)) {
    struct stat buff;
    string meta_path = db_path_ + tbl_sub_path_ + meta_name + ".0";
    if (stat(meta_path.c_str(), &buff) != 0) {
      err_info.setError(KWENOOBJ, meta_path);
      return err_info.errcode;
    }
  }

  int ret = meta_manager_.Open(meta_name, db_path_, tbl_sub_path_, true);
  if (ret < 0) {
    err_info.setError(ret, tbl_sub_path_ + meta_name);
    return err_info.errcode;
  }
  return 0;
}

int TsTimePartition::remove(bool exclude_segment) {
  int error_code = 0;
  if (!exclude_segment) {
    data_segments_.Traversal([&](BLOCK_ID id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
      error_code = tbl->remove();
      if (error_code < 0) {
        LOG_ERROR("remove segment[%s] failed!", tbl->realFilePath().c_str());
        return false;
      }
      LOG_INFO("remove segment[%s] success!", tbl->realFilePath().c_str());
      return true;
    });
  }
  releaseSegments();
  error_code = meta_manager_.remove();
  return error_code;
}

void TsTimePartition::sync(int flags) {
  meta_manager_.sync(flags);
  MUTEX_LOCK(active_segment_lock_);
  data_segments_.Traversal([flags](BLOCK_ID id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->getObjectStatus() == OBJ_READY) {
      tbl->sync(flags);
    }
    return true;
  });
  MUTEX_UNLOCK(active_segment_lock_);
}

bool TsTimePartition::JoinOtherPartitionTable(TsTimePartition* other) {
  MUTEX_LOCK(active_segment_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};
  // mark all segments cannot write. when joining, we cannot write rows.
  if (active_segment_ != nullptr) {
    active_segment_->setSegmentStatus(InActiveSegment);
    active_segment_ = nullptr;
  }
  data_segments_.Traversal([&](BLOCK_ID s_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->sqfsIsExists()) {
      return true;
    }
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                     db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                     false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }
    if (tbl->getSegmentStatus() == ActiveSegment) {
      tbl->setSegmentStatus(InActiveSegment);
    }
    return true;
  });
  auto other_p_header = other->meta_manager_.getEntityHeader();
  auto this_p_header = this->meta_manager_.getEntityHeader();

  std::vector<BLOCK_ID> other_segment_ids;
  other->data_segments_.GetAllKey(&other_segment_ids);
  std::sort(other_segment_ids.begin(), other_segment_ids.end());

  BLOCK_ID current_file_id = this_p_header->cur_block_id + 1;
  for (size_t i = 0; i < other_segment_ids.size(); i++) {
    std::string other_segment_path = other->GetPath() + intToString(other_segment_ids[i]);
    std::string  new_segment_path = this->GetPath() + intToString(this_p_header->cur_block_id + other_segment_ids[i]);
    std::string cmd = "mv " + other_segment_path + " " + new_segment_path;
    ErrorInfo err_info;
    if (!System(cmd, err_info)) {
      LOG_ERROR("exec [%s] failed.", cmd.c_str());
      return false;
    }
    auto err_code = loadSegment(this_p_header->cur_block_id + other_segment_ids[i], err_info);
    if (err_code < 0) {
      LOG_ERROR("loadSegment file failed. msg: %s.", err_info.errmsg.c_str());
      return false;
    }
    current_file_id = this_p_header->cur_block_id + other_segment_ids[i];
  }
  BlockItem *new_blk;
  for (size_t i = 1; i <= other_p_header->cur_block_id; i++) {
    auto blk_item = other->GetBlockItem(i);
    this->meta_manager_.AddBlockItem(blk_item->entity_id, &new_blk);
    new_blk->CopyMetricMetaInfo(*blk_item);
    this->meta_manager_.UpdateEntityItem(blk_item->entity_id, new_blk);
  }

  this_p_header->cur_datafile_id = current_file_id;
  return true;
}

size_t TsTimePartition::size(uint32_t entity_id) const {
  return meta_manager_.getEntityItem(entity_id)->row_written;
}

ostream& TsTimePartition::printRecord(uint32_t entity_id, std::ostream& os, size_t row) {
  os << "[ not support ]\n";
  return os;
}

int TsTimePartition::ProcessDuplicateData(kwdbts::Payload* payload, size_t start_in_payload, size_t count,
                                          const BlockSpan span, DedupInfo& dedup_info,
                                          DedupResult* dedup_result, ErrorInfo& err_info, int* deleted_rows) {
  int err_code = 0;
  if (payload->dedup_rule_ == DedupRule::REJECT || payload->dedup_rule_ == DedupRule::DISCARD) {
    for (size_t i = 0; i < span.row_num; i++) {
      // If it is REJECT and there is already duplicate data in the current partition table,
      // insertion is prohibited: mark the duplicate data that has been written to the current payload as deleted and return an error message.
      // If DISCARD is specified, the existing duplicate data in the current payload will be marked for deletion without generating an error message.
      auto cur_ts = KTimestamp(payload->GetColumnAddr(start_in_payload + count + i, 0));
      if (dedup_info.table_real_rows.find(cur_ts) != dedup_info.table_real_rows.end()) {
        if (payload->dedup_rule_ == DedupRule::REJECT) {
          err_code = KWEDUPREJECT;
          err_info.errcode = err_code;
          err_info.errmsg = "dedup_rule_ is reject, and payload has deduplicate ts.";
          return err_code;
        }
        SetDeleted();
        span.block_item->setDeleted(span.start_row + i + 1);
        *deleted_rows += 1;
        if (dedup_result->discard_bitmap.len != 0) {
          setRowDeleted(dedup_result->discard_bitmap.data, i + 1);
        }
        dedup_result->dedup_rows += 1;
      }
    }
  }
  // The duplicate data found will be saved in dedup_info.payload_rows,
  // and the rows that need to be deleted during import will be saved in dedup_result.discard_bitmap.
  // need deleted rows in payload, can delete here.
  for (auto kv: dedup_info.payload_rows) {
    if (kv.second.size() > 1) {
      for (int i = kv.second.size() - 2; i >= 0; i--) {
        if (kv.second[i] >= start_in_payload + count && kv.second[i] < start_in_payload + count + span.row_num) {
          SetDeleted();
          span.block_item->setDeleted(span.start_row + (kv.second[i] - start_in_payload - count) + 1);
          *deleted_rows += 1;
          if (!(payload->dedup_rule_ == DedupRule::REJECT || payload->dedup_rule_ == DedupRule::DISCARD)
              || dedup_info.table_real_rows.find(kv.first) == dedup_info.table_real_rows.end()) {
            // only count when this ts is not processed before.
            if (dedup_result->discard_bitmap.len != 0) {
              setRowDeleted(dedup_result->discard_bitmap.data, kv.second[i] + 1);
            }
            dedup_result->dedup_rows += 1;
          }
        }
      }
    }
  }
  return err_code;
}


int64_t TsTimePartition::push_back_payload(kwdbts::kwdbContext_p ctx, uint32_t entity_id, kwdbts::Payload* payload,
                                           size_t start_in_payload, size_t num,
                                           std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
                                           ErrorInfo& err_info, uint32_t* inc_unordered_cnt, DedupResult* dedup_result) {
  KWDB_DURATION(StStatistics::Get().push_payload);
  int64_t err_code = 0;
  err_info.errcode = 0;
  alloc_spans->clear();

  // 1.Calculate the minimum and maximum timestamps for the payload to prepare for subsequent deduplication.
  // If the current payload's timestamp range does not overlap with the entity's, there is no need to deduplicate.
  KTimestamp pl_min = INT64_MAX, pl_max = INT64_MIN;
  for (size_t i = start_in_payload; i < start_in_payload + num; i++) {
    KTimestamp cur_ts = KTimestamp(payload->GetColumnAddr(i, 0));
    if (pl_min > cur_ts) {
      pl_min = cur_ts;
    }
    if (pl_max < cur_ts) {
      pl_max = cur_ts;
    }
  }

  size_t count = 0;
  kwdbts::DedupInfo dedup_info;
  int deleted_rows = 0;
  {
    MUTEX_LOCK(vacuum_insert_lock_);
    // 2.Allocate data space for writing to the corresponding entity and save the allocation result to the passed in spans array.
    // If an error occurs during the allocation process, revert the space allocated before and return an error code.
    // If the current segment is full, switch to a new segment.
    err_code = AllocateAllDataSpace(entity_id, num, alloc_spans, payload->GetTsVersion());
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "AllocateSpace failed : " + string(strerror(errno));
      MUTEX_UNLOCK(vacuum_insert_lock_);
      return err_code;
    }
    EntityItem* entity_item = getEntityItem(entity_id);
    // The need_scan_table is used to identify whether there is duplicate data that needs to be scanned for partitions,
    // If the timestamp range of the current payload does not fall within the timestamp range of the current entity,
    // there is no need to scan the entire partition.
    if (pl_min > entity_item->max_ts || entity_item->max_ts == INVALID_TS
        || pl_max < entity_item->min_ts) {
      dedup_info.need_scan_table = false;
    } else {
      dedup_info.need_scan_table = true;
    }
    // During writing, a determination is made as to whether the current payload contains disordered data.
    // If present, entity_item is marked as being disordered.
    // During subsequent queries, a judgment will be made. If the result set is unordered,
    // a secondary HASH aggregation based on AGG SCAN is required.
    if (payload->IsDisordered(start_in_payload, num) ||
        (entity_item->max_ts != INVALID_TS && pl_min < entity_item->max_ts)) {
      entity_item->is_disordered = true;
      // As long as there is data disorder in this partition, it needs to be reorganized
      SetDisordered();
    }
    // Update the maximum and minimum timestamps of entity_items to prepare for subsequent payload deduplication.
    if (pl_max > entity_item->max_ts || entity_item->max_ts == INVALID_TS) {
      entity_item->max_ts = pl_max;
    }
    if (pl_min < entity_item->min_ts || entity_item->min_ts == INVALID_TS) {
      entity_item->min_ts = pl_min;
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }

  int dedup_rows_orgin = dedup_result->dedup_rows;

  // before return, need publish inserted data and update meta datas.
  Defer defer{[&]() {
    if (err_code < 0) {
      // Note the number of rows that failed during import, as it is only used during the import process
      dedup_result->dedup_rows = dedup_rows_orgin + payload->GetRowCount();
    }
    std::vector<size_t> full_block_idx;
    MUTEX_LOCK(vacuum_insert_lock_);
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    entity_item_latch->Lock(entity_id);
    EntityItem* entity_item = getEntityItem(entity_id);
    entity_item->row_written += (num - deleted_rows);
    entity_item_latch->Unlock(entity_id);
    for (size_t i = 0; i < alloc_spans->size(); i++) {
      (*alloc_spans)[i].block_item->publish_row_count += (*alloc_spans)[i].row_num;
      if ((*alloc_spans)[i].block_item->publish_row_count >= (*alloc_spans)[i].block_item->max_rows_in_block) {
        full_block_idx.push_back(i);
      }
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
    for (size_t i = 0; i < full_block_idx.size(); i++) {
      std::shared_ptr<MMapSegmentTable> tbl = getSegmentTable((*alloc_spans)[full_block_idx[i]].block_item->block_id);
      if (tbl == nullptr) {
        LOG_ERROR("getSegmentTable failed, block_id: %u", (*alloc_spans)[full_block_idx[i]].block_item->block_id);
        return;
      }
      const BlockSpan& span = (*alloc_spans)[full_block_idx[i]];
      tbl->updateAggregateResult(span, span.block_item->getDeletedCount() > 0);
    }
  }};

  // 3.Deduplication is performed and the payload is updated according to different deduplication rules (KEEP, REJECT, MERGE).
  // Inspect the payload for any duplicate timestamps and apply various treatments, such as checking for duplicate rows and merging them.
  err_code = updatePayloadUsingDedup(entity_id, (*alloc_spans)[0], payload, start_in_payload, num, dedup_info);
  if (err_code < 0) {
    err_info.errcode = err_code;
    err_info.errmsg = "updatePayloadUsingDedup failed : " + string(strerror(errno));
    return err_code;
  }

  // 4.Retrieve the allocated block_item from alloc_spans and write the data to the pre-allocated space
  // After the writing is completed, corresponding processing (error reporting, data deletion)
  // will be performed according to the deduplication mode (REJECT, DISCARD).
  for (size_t idx = 0; idx < alloc_spans->size(); idx++) {
    const BlockSpan& span = (*alloc_spans)[idx];
    MetricRowID entity_num_node = meta_manager_.GetFileStartRow(span);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(span.block_item->block_id);
    if (!segment_tbl) {
      err_info.errcode = KWENOOBJ;
      err_info.errmsg = "Segment [" + db_path_ + segment_tbl_sub_path(span.block_item->block_id) + "] is null";
      return err_code;
    }
    // Write in a columnar format and update the aggregated results
    err_code = segment_tbl->PushPayload(entity_id, entity_num_node, payload,
                                        start_in_payload + count, span, inc_unordered_cnt, dedup_info);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "PushPayload failed : " + string(strerror(errno));
      return err_code;
    }
    err_code = ProcessDuplicateData(payload, start_in_payload, count, span, dedup_info, dedup_result, err_info, &deleted_rows);
    if (err_code < 0) {
      return err_code;
    }
    count += span.row_num;
  }
  if (payload->dedup_rule_ == DedupRule::OVERRIDE || payload->dedup_rule_ == DedupRule::MERGE) {
    // In the deduplication mode, it is necessary to record the rows that need to be deleted after deduplication for subsequent deletion.
    // return: to be deleted rows in tables.
    for (auto kv : dedup_info.table_real_rows) {
      todo_markdel->insert(todo_markdel->end(), kv.second.begin(), kv.second.end());
    }
  }

  err_info.errcode = err_code;
  return err_code;
}

bool TsTimePartition::EvaluateVacuum(const timestamp64& ts, uint32_t ts_version) {
  std::vector<BLOCK_ID> segment_ids;
  data_segments_.GetAllKey(&segment_ids);
  if (segment_ids.size() > 2) {
    return true;
  }
  if (IsModifiedRecent(ts)) {
    return false;
  }
  // DDL
  for (auto& seg_id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(seg_id);
    if (segment_tbl == nullptr) {
      continue;
    }
    auto segment_version = segment_tbl->schemaVersion();
    if (segment_version != ts_version) {
      return true;
    }
  }
  return ShouldVacuum();
}

KStatus TsTimePartition::PrepareTempPartition(uint32_t max_rows_per_block, uint32_t max_blocks_per_seg,
                                              uint32_t ts_version, TsTimePartition** dest_pt) {
  ErrorInfo err_info;
  std::string tmp_dir = GetPath();
  tmp_dir[tmp_dir.size() - 1] = '_';
  // if tmp_dir exists, remove the garbage files left over from the last vacuum
  if (IsExists(tmp_dir)) {
    Remove(tmp_dir);
  }
  // Create a temporary partition and segment to store the data after vacuum
  if (!MakeDirectory(tmp_dir, err_info)) {
    LOG_ERROR("Compact partition[%s] failed, couldn't create temporary directory.", tmp_dir.c_str());
    return FAIL;
  }
  *dest_pt = new TsTimePartition(root_table_manager_, meta_manager_.max_entities_per_subgroup);
  std::string tmp_partition_sub_path = tbl_sub_path_ + "/";
  tmp_partition_sub_path[tmp_partition_sub_path.size()-2] = '_';
  (*dest_pt)->open(file_path_, db_path_, tmp_partition_sub_path, MMAP_CREAT_EXCL, err_info);
  if (!err_info.isOK()) {
    delete *dest_pt;
    LOG_ERROR("Compact partition[%s] failed, couldn't open temporary table.", tmp_dir.c_str());
    return FAIL;
  }
  // create segment use max_rows_per_block and max_blocks_per_seg
  MMapSegmentTable* segment_tb = (*dest_pt)->createSegmentTable(1, ts_version, err_info,
                                                                max_rows_per_block, max_blocks_per_seg);
  if (segment_tb == nullptr || err_info.errcode < 0) {
    delete *dest_pt;
    LOG_ERROR("createSegmentTable error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  // initialize segment
  segment_tb->setSegmentStatus(ActiveSegment);
  std::shared_ptr<MMapSegmentTable> segment_table(segment_tb);
  (*dest_pt)->data_segments_.Insert(1, segment_table);

  (*dest_pt)->active_segment_ = segment_table;
  (*dest_pt)->meta_manager_.getEntityHeader()->cur_datafile_id = 1;
  return SUCCESS;
}

KStatus TsTimePartition::GetVacuumData(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* cur_block_item,
                                       size_t block_start_idx, k_uint32 row_count, uint32_t ts_version, ResultSet* res) {
  std::vector<AttributeInfo> tb_schema;
  KStatus s = root_table_manager_->GetSchemaInfoExcludeDropped(&tb_schema, ts_version);
  if (s != SUCCESS) {
    LOG_ERROR("Couldn't get schema info of version %u", ts_version);
    return s;
  }
  auto segment_schema = segment_tbl->getSchemaInfo();
  std::vector<uint32_t> scan_cols = root_table_manager_->GetIdxForValidCols(ts_version);
  Batch* b;
  for (int i = 0; i < scan_cols.size(); i++) {
    auto ts_col = scan_cols[i];
    if (ts_col >= 0 && segment_tbl->isColExist(ts_col)) {
      void* bitmap_addr = segment_tbl->getBlockHeader(cur_block_item->block_id, ts_col);
      if (tb_schema[ts_col].type != VARSTRING && tb_schema[ts_col].type != VARBINARY) {
        // not varlen column
        if (segment_schema[ts_col].type != tb_schema[ts_col].type) {
          // convert other types to fixed-length type
          char* value = static_cast<char*>(malloc(tb_schema[ts_col].size * row_count));
          memset(value, 0, tb_schema[ts_col].size * row_count);
          bool need_free_bitmap = false;
          s = ConvertToFixedLen(segment_tbl, value, cur_block_item->block_id,
                                        static_cast<DATATYPE>(segment_schema[ts_col].type),
                                        static_cast<DATATYPE>(tb_schema[ts_col].type),
                                        tb_schema[ts_col].size, block_start_idx, row_count, ts_col, &bitmap_addr, need_free_bitmap);
          if (s != KStatus::SUCCESS) {
            free(value);
            return s;
          }
          b = new Batch(static_cast<void *>(value), row_count, bitmap_addr, block_start_idx, segment_tbl);
          b->is_new = true;
          b->need_free_bitmap = need_free_bitmap;
        } else {
          if (segment_schema[ts_col].size != tb_schema[ts_col].size) {
            // convert same fixed-length type to different length
            char* value = static_cast<char*>(malloc(tb_schema[ts_col].size * row_count));
            memset(value, 0, tb_schema[ts_col].size * row_count);
            for (int idx = 0; idx < row_count; idx++) {
              memcpy(value + idx * tb_schema[ts_col].size,
                     segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx + idx - 1, ts_col),
                     segment_schema[ts_col].size);
            }
            b = new Batch(static_cast<void *>(value), row_count, bitmap_addr, block_start_idx, segment_tbl);
            b->is_new = true;
          } else {
            b = new Batch(segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx - 1, ts_col),
                          row_count, bitmap_addr, block_start_idx, segment_tbl);
          }
        }
      } else {
        // varlen column
        b = new VarColumnBatch(row_count, bitmap_addr, block_start_idx, segment_tbl);
        for (k_uint32 j = 0; j < row_count; ++j) {
          std::shared_ptr<void> data = nullptr;
          bool is_null;
          if (b->isNull(j, &is_null) != KStatus::SUCCESS) {
            delete b;
            b = nullptr;
            return KStatus::FAIL;
          }
          if (is_null) {
            data = nullptr;
          } else {
            if (segment_schema[ts_col].type != tb_schema[ts_col].type) {
              // convert other types to variable length
              data = ConvertToVarLen(segment_tbl, cur_block_item->block_id,
                                     static_cast<DATATYPE>(segment_schema[ts_col].type),
                                     static_cast<DATATYPE>(tb_schema[ts_col].type), block_start_idx + j - 1, ts_col);
            } else {
              data = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, block_start_idx + j - 1, ts_col);
            }
          }
          b->push_back(data);
        }
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      b = new Batch(bitmap, row_count, bitmap, block_start_idx, segment_tbl);
    }
    res->push_back(i, b);
  }
  return SUCCESS;
}

KStatus
TsTimePartition::WriteVacuumData(TsTimePartition* dest_pt, uint32_t entity_id, ResultSet* res, uint32_t row_count) {
  std::shared_ptr<MMapSegmentTable> dest_segment = dest_pt->getSegmentTable(1);
  if (!dest_segment) {
    LOG_ERROR("Get segment table[%u] failed", 1);
    return FAIL;
  }
  // allocate block item
  std::vector<BlockSpan> dst_spans;
  dest_pt->AllocateAllDataSpace(entity_id, row_count, &dst_spans, dest_segment->schemaVersion());
  vector<uint32_t> cols_idx = dest_segment->getIdxForValidCols();
  vector<AttributeInfo> cols_info = dest_segment->GetColsInfoWithoutHidden();
  for (int i = 0; i < cols_idx.size(); ++i) {
    uint32_t batch_row_idx = 0;
    const Batch* batch = res->data[i][0];
    if (!isVarLenType(cols_info[i].type)) {
      // fixed length
      uint32_t total_row = 0;
      for (BlockSpan block_span : dst_spans) {
        MetricRowID row_id{block_span.block_item->block_id, block_span.start_row + 1};
        if (i) {  // timestamp col is not null
          for (int j = 0; j < block_span.row_num; j++) {
            bool is_null = false;
            batch->isNull(batch_row_idx, &is_null);
            if (is_null) {
              dest_segment->setNullBitmap(row_id, cols_idx[i]);
            }
            batch_row_idx++;
            row_id.offset_row++;
          }
        }
        if (batch->mem != nullptr) {
          row_id.offset_row = block_span.start_row + 1;  // reset row_id to first row of block span
          memcpy(dest_segment->columnAddr(row_id, cols_idx[i]), (void*)((char*)batch->mem + total_row * cols_info[i].size),
                 block_span.row_num * cols_info[i].size);
          total_row += block_span.row_num;
        }
      }
    } else {
      int block_span_idx = 0;
      size_t loc = 0;
      int count = 0;
      // var length data, write one by one
      uint32_t cur_block_row = dst_spans[block_span_idx].start_row;
      for (uint32_t row_idx = 0; row_idx < row_count; ++row_idx, ++cur_block_row) {
        bool is_null = false;
        batch->isNull(batch_row_idx, &is_null);
        batch_row_idx++;
        if (is_null) {
          MetricRowID row_id{dst_spans[block_span_idx].block_item->block_id, cur_block_row + 1};
          dest_segment->setNullBitmap(row_id, cols_idx[i]);
          continue;
        }
        char* var_addr = (char*)batch->getVarColData(row_idx);
        uint16_t var_c_len = batch->getVarColDataLen(row_idx);
        MMapStringColumn* dest_str_file = dest_segment->GetStringFile();
        // check string file size
        if (var_c_len + dest_str_file->size() >= dest_str_file->fileLen()) {
          int err_code = 0;
          if (dest_str_file->fileLen() > 1024 * 1024 * 1024) {
            size_t new_size = dest_str_file->size() / 2;
            err_code = dest_str_file->reserve(dest_str_file->size(), 3 * new_size, 2);
          } else {
            err_code = dest_str_file->reserve(dest_str_file->size(), 2 * dest_str_file->fileLen(), 2);
          }
          if (err_code < 0) {
            LOG_ERROR("MMapStringColumn[%s] reserve failed.", dest_str_file->strFile().realFilePath().c_str());
            return FAIL;
          }
        }
        // write data to string file
        if (cols_info[i].type == VARSTRING) {
          loc = dest_str_file->push_back(var_addr, var_c_len);
        } else {
          loc = dest_str_file->push_back_binary(var_addr, var_c_len);
        }
        if (loc == 0) {
          LOG_ERROR("StringFile push bach failed.");
          return FAIL;
        }
        if (dst_spans[block_span_idx].row_num == count) {
          ++block_span_idx;
          count = 0;
          cur_block_row = dst_spans[block_span_idx].start_row;
        }
        // write string location to column file
        MetricRowID row_id{dst_spans[block_span_idx].block_item->block_id, cur_block_row + 1};
        memcpy(dest_segment->columnAddr(row_id, cols_idx[i]), &loc, cols_info[i].size);
        ++count;
      }
    }
  }
  // update aggregate result
  for (BlockSpan block_span : dst_spans) {
    block_span.block_item->publish_row_count += block_span.row_num;
    if (block_span.block_item->publish_row_count >= block_span.block_item->max_rows_in_block) {
      dest_segment->updateAggregateResult(block_span, true);
    } else {
      AggDataAddresses addresses{};
      MetricRowID row_id{block_span.block_item->block_id, block_span.start_row + 1};
      dest_segment->columnAggCalculate(block_span, row_id, 0, addresses, block_span.row_num, false);
      block_span.block_item->max_ts_in_block = KTimestamp(addresses.max);
      block_span.block_item->min_ts_in_block = KTimestamp(addresses.min);
      // Update the maximum and minimum timestamp information of the current segment,
      // which will be used for subsequent compression and other operations
      if (KTimestamp(addresses.max) > maxTimestamp() || maxTimestamp() == INVALID_TS) {
        dest_segment->maxTimestamp() = KTimestamp(addresses.max);
      }
      if (KTimestamp(addresses.min) > minTimestamp() || minTimestamp() == INVALID_TS) {
        dest_segment->minTimestamp() = KTimestamp(addresses.min);
      }
    }
  }
  return SUCCESS;
}

KStatus TsTimePartition::CopyConcurrentSegments(const std::vector<std::shared_ptr<MMapSegmentTable>>& segment_tables,
  uint32_t max_segment_id, uint32_t max_block_id, TsTimePartition* dest_pt) {
  // Segment IDs are arranged in descending order from largest to smallest
  for (const auto& segment : segment_tables) {
    if (segment->segment_id() <= max_segment_id) {
      break;
    }
    std::string segment_dir_path = GetPath() + std::to_string(segment->segment_id()) + '/';
    std::string cmd = "cp -rf " + segment_dir_path + " " + dest_pt->GetPath();
    if (!System(cmd)) {
      LOG_ERROR("Copy segment[%s] files failed, %s", segment_dir_path.c_str(), strerror(errno));
      return FAIL;
    }
  }
  //  Rebuild added block_item
  bool first_loop = true;
  for (BLOCK_ID block_id = max_block_id + 1; block_id <= GetMaxBlockID(); ++block_id) {
    BlockItem* block_item = GetBlockItem(block_id);
    auto entity_item = dest_pt->meta_manager_.getEntityItem(block_item->entity_id);
    if (!entity_item->entity_id) {
      entity_item->entity_id = block_item->entity_id;
    }

    BlockItem* new_block_item;
    if (first_loop) {
      // the block item is allocated from max_block_id + 1.
      EntityHeader* dest_header = dest_pt->meta_manager_.getEntityHeader();
      dest_header->cur_block_id = max_block_id;
      first_loop = false;
    }
    dest_pt->meta_manager_.AddBlockItem(block_item->entity_id, &new_block_item);

    memcpy(&new_block_item->crc, &block_item->crc, sizeof(uint32_t) + 4 * sizeof(bool) + sizeof(BLOCK_ID));
    memcpy(&new_block_item->publish_row_count, &block_item->publish_row_count, 3 * sizeof(uint32_t) + 128 + 28);
    // Update entity item, otherwise, all new blocks added in the future point to the same prev
    dest_pt->meta_manager_.UpdateEntityItem(new_block_item->entity_id, new_block_item);
  }
  return KStatus::SUCCESS;
}

KStatus TsTimePartition::ProcessVacuum(const timestamp64& ts, uint32_t ts_version) {
  // Step 1: Change segment status and get the necessary information
  vacuumLock();
  if (!EvaluateVacuum(ts, ts_version)) {
    vacuumUnlock();
    return SUCCESS;
  }
  auto entities = GetEntities();
  // check not inserting data.
  if (IsWriting()) {
    LOG_WARN("partition[%s] is inserting, ref count [%d], cannot vacuum now.", file_path_.c_str(), writing_count_.load());
    vacuumUnlock();
    return SUCCESS;
  }
  // set revacuum flag false, if the data written after unlocking is disordered or deleted, indicates that need to
  // vacuum again, and the flag will be set to true
  need_revacuum_ = false;
  std::vector<BLOCK_ID> segment_ids;
  data_segments_.GetAllKey(&segment_ids);
  BLOCK_ID max_segment_id = 0;
  if (active_segment_ == nullptr) {
    if (segment_ids.empty()) {
      vacuumUnlock();
      return SUCCESS;
    }
    max_segment_id = segment_ids.front();
  } else {
    max_segment_id = active_segment_->segment_id();
  }
  BLOCK_ID max_block_id = meta_manager_.getEntityHeader()->cur_block_id;
  ChangeSegmentStatus();
  vacuumUnlock();

  // Step 2: Read all valid data of the current entities and record the number of rows
  std::unordered_map<uint32_t, std::deque<BlockSpan>> src_spans;
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back({INT64_MIN, INT64_MAX});
  uint32_t max_block_num = 0;
  for (uint32_t entity_id : entities) {
    std::deque<BlockSpan> block_spans;
    uint32_t count = GetAllBlockSpans(entity_id, ts_spans, block_spans, max_block_id);
    // calculate temporary partition's segment need how many blocks
    max_block_num += (count + CLUSTER_SETTING_MAX_ROWS_PER_BLOCK - 1) / CLUSTER_SETTING_MAX_ROWS_PER_BLOCK;
    src_spans[entity_id] = block_spans;
  }

  if (max_block_num == 0) {
    vacuumLock();

    // All the data of segment_ids has been deleted, drop segments
    if (IsSegmentsBusy(segment_ids)) {
      vacuumUnlock();
      return SUCCESS;
    }
    if (DropSegmentDir(segment_ids) < 0) {
      LOG_ERROR("Drop old segments failed");
      vacuumUnlock();
      return FAIL;
    }

    // reset all entity item block info.
    for (uint32_t entity_id : entities) {
      auto entity_item = getEntityItem(entity_id);
      if (0 == entity_item->cur_block_id) {
        continue;
      }
      std::shared_ptr<MMapSegmentTable> value;
      BLOCK_ID key;
      bool ret = data_segments_.Seek(entity_item->cur_block_id, key, value);
      if (ret && max_segment_id >= key) {
        entity_item->block_count = 0;
        entity_item->cur_block_id = 0;
        entity_item->is_deleted = false;
        entity_item->is_disordered = false;
        entity_item->max_ts = 0;
        entity_item->min_ts = 0;
      } else {
        auto cur_block_id = entity_item->cur_block_id;
        while (true) {
          auto block = GetBlockItem(cur_block_id);
          assert(block->entity_id == entity_id);
          if (block->prev_block_id == 0) {
            break;
          }
          bool ret = data_segments_.Seek(block->prev_block_id, key, value);
          if (ret && max_segment_id >= key) {
            block->prev_block_id = 0;
            break;
          }
          cur_block_id = block->prev_block_id;
        }
      }
    }

    ErrorInfo err_info;
    loadSegments(err_info);
    segment_ids.clear();
    data_segments_.GetAllKey(&segment_ids);
    if (segment_ids.empty()) {
      meta_manager_.getEntityHeader()->cur_block_id = 0;
    }
    vacuumUnlock();
    return SUCCESS;
  }

  // Step 3: Create temporary partition.
  TsTimePartition* dest_pt = nullptr;
  KStatus s = PrepareTempPartition(CLUSTER_SETTING_MAX_ROWS_PER_BLOCK, max_block_num, ts_version, &dest_pt);
  if (s != SUCCESS) {
    return s;
  }
  std::string partition_dir = GetPath();
  std::string dest_path = dest_pt->GetPath();
  Defer defer_delete{[&]() {
    delete dest_pt;
    dest_pt = nullptr;
    Remove(dest_path);
  }};

  //Step 4: Traverse all read data and write to the temporary partition
  if (max_block_num != 0) {
    uint32_t num_col = root_table_manager_->GetIdxForValidCols(ts_version).size();
    for (const auto& iter : src_spans) {
      // Check if it is necessary to suspend this vacuum.
      if (need_revacuum_) {
        LOG_WARN("During the vacuum process, it was discovered that the partition needed to be revacuum,"
                 " and this vacuum is now suspended");
        return SUCCESS;
      }
      // Traverse every entity's data
      uint32_t entity_id = iter.first;
      auto block_spans = iter.second;
      // Traverse every block span of this entity
      for (auto block_span : block_spans) {
        BlockItem* cur_block_item = block_span.block_item;
        uint32_t block_start_row = block_span.start_row;
        uint32_t row_count = block_span.row_num;
        std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(cur_block_item->block_id);
        if (segment_tbl == nullptr) {
          LOG_ERROR("Can not find segment use block [%u], in partition [%s]", cur_block_item->block_id, partition_dir.c_str());
          return FAIL;
        }
        ResultSet res{num_col};
        // Step 4.1: Read data after vacuum
        s = GetVacuumData(segment_tbl, cur_block_item, block_start_row + 1, row_count, ts_version, &res);
        if (s != SUCCESS) {
          LOG_ERROR("Get vacuum data failed");
          // Defer will clear data
          return s;
        }
        // Step 4.2: Write into des_pt.
        s = WriteVacuumData(dest_pt, entity_id, &res, row_count);
        if (s != SUCCESS) {
          LOG_ERROR("Write vacuum data failed");
          // Defer will clear data
          return s;
        }
      }
      EntityItem* entity_item = dest_pt->getEntityItem(entity_id);
      entity_item->max_ts = getEntityItem(entity_id)->max_ts;
      entity_item->min_ts = getEntityItem(entity_id)->min_ts;
    }
  }

  // Step 5: Copy added segment files from original partition to the temporary and update status
  dest_pt->minTimestamp() = minTimestamp();
  dest_pt->maxTimestamp() = maxTimestamp();
  std::shared_ptr<MMapSegmentTable> dest_segment = dest_pt->getSegmentTable(1);
  dest_segment->setSegmentStatus(InActiveSegment);

  // Copy added segment files
  vacuumLock();
  Defer defer{[&]() { vacuumUnlock(); }};
  if (need_revacuum_) {
    LOG_WARN("During the vacuum process, it was discovered that the partition needed to be revacuum,"
         " and this vacuum is now suspended");
    return SUCCESS;
  }
  if (IsWriting()) {
    LOG_WARN("partition[%s] is inserting [%d], cancel vacuum.", file_path_.c_str(), writing_count_.load());
    return SUCCESS;
  }
  std::vector<std::shared_ptr<MMapSegmentTable>> segment_tables;
  data_segments_.GetAllValue(&segment_tables);
  if (segment_tables.front()->segment_id() != max_segment_id) {
    s = CopyConcurrentSegments(segment_tables, max_segment_id, max_block_id, dest_pt);
    if (s != SUCCESS) {
      return s;
    }
  } else {
    dest_segment->setSegmentStatus(ActiveSegment);
  }

  // count(*) use entity_item->row_written
  auto latest_entities = GetEntities();
  for (auto& entity_id : latest_entities) {
    EntityItem* entity_item = dest_pt->getEntityItem(entity_id);
    entity_item->row_written = getEntityItem(entity_id)->row_written;
  }

  // Step 6: Drop partition's old segments and move vacuumed segments to original partition
  segment_ids.clear();
  data_segments_.GetAllKey(&segment_ids);

  if (IsSegmentsBusy(segment_ids)) {
    vacuumUnlock();
    return SUCCESS;
  }
  if (DropSegmentDir(segment_ids) < 0) {
    LOG_ERROR("Drop old segments failed");
    return FAIL;
  }
  data_segments_.Clear();
  meta_manager_.release(); // avoid memory leak

  std::string cmd = "mv " + dest_path + "* " + partition_dir;
  if (!System(cmd)) {
    LOG_ERROR("mv tmp partition dir failed");
    // mv failed, still need to reload meta and segments, not return fail
  }
  // Step 7: Reload partition
  // reopen meta.0
  ErrorInfo err_info;
  if (openBlockMeta(MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    LOG_ERROR("Reopen block meta failed, error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  // reload segments
  if (loadSegments(err_info) < 0) {
    LOG_ERROR("Reload segments failed, error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  if (root_table_manager_->GetTableVersionOfLatestData() < ts_version) {
    root_table_manager_->UpdateTableVersionOfLastData(ts_version);
  }
  ResetVacuumFlags();
  return SUCCESS;
}

// Get segments that require compression processing
std::vector<std::shared_ptr<MMapSegmentTable>> TsTimePartition::GetAllSegmentsForCompressing(){
  vector<std::shared_ptr<MMapSegmentTable>> segment_tables;
  data_segments_.Traversal([&](BLOCK_ID segment_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    // Compressed segment
    if (tbl->sqfsIsExists()) {
      if (tbl->getObjectStatus() == OBJ_READY && tbl->getSegmentStatus() == ImmuSegment) {
        // Segments that have been compressed but not cleaned up from the original data directory
        // need to be cleaned up from the original data directory
        segment_tables.emplace_back(tbl);
      }
      // Compressed and cleaned segments from the original data directory do not take any action
      return true;
    }
    // If the uncompressed segment is not ready,
    // it needs to be reopened and subsequent compression operations should be performed
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                    db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                    false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }
    segment_tables.emplace_back(tbl);
    return true;
  });
  return segment_tables;
}

void TsTimePartition::ImmediateCompress(ErrorInfo& err_info){
  auto segment_tables = GetAllSegmentsForCompressing();
  int num_of_active_segments = 0;
  for (auto iSegmentTable : segment_tables) {
    if (iSegmentTable->getSegmentStatus() == ActiveSegment) {
      num_of_active_segments++;
      iSegmentTable->setSegmentStatus(InActiveSegment);
      MUTEX_LOCK(active_segment_lock_);
      if (active_segment_ && active_segment_->segment_id() == iSegmentTable->segment_id()) {
        active_segment_ = nullptr;
      }
      MUTEX_UNLOCK(active_segment_lock_);
    }
  }

  if (num_of_active_segments != 0) {
    // waiting for unfinished writing threads on active thread.
    using std::chrono_literals::operator""s;
    std::this_thread::sleep_for(1s);
  }

  // compress all the InActiveSegments and make sure they are mounted
  for (auto iSegmentTable : segment_tables) {
    if (iSegmentTable->getSegmentStatus() != InActiveSegment) {
      continue;
    }
    // Sync before compressing
    iSegmentTable->sync(MS_SYNC);
    LOG_INFO("MMapSegmentTable[%s] compress start", iSegmentTable->realFilePath().c_str());
    iSegmentTable->setSegmentStatus(ImmuSegment);
    bool ok = compress(db_path_, tbl_sub_path_, std::to_string(iSegmentTable->segment_id()),
                       g_mk_squashfs_option.processors_immediate, err_info);
    if (!ok) {
      // If compression fails, restore segment state
      iSegmentTable->setSegmentStatus(InActiveSegment);
      LOG_ERROR("MMapSegmentTable[%s] compress failed", iSegmentTable->realFilePath().c_str());
      return;
    }
    iSegmentTable->setSqfsIsExists();

    // Mount the compressed segment
    if (!isMounted(db_path_ + iSegmentTable->tbl_sub_path())) {
      if (!reloadSegment(iSegmentTable, false, err_info)) {
        LOG_ERROR("MMapSegmentTable[%s] reload failed", iSegmentTable->realFilePath().c_str());
        return;
      }
    }
    LOG_INFO("MMapSegmentTable[%s] compress succeeded", iSegmentTable->realFilePath().c_str());
  }
}

void TsTimePartition::ScheduledCompress(timestamp64 compress_ts, ErrorInfo& err_info) {
  auto segment_tables = GetAllSegmentsForCompressing();
  // Traverse the segments that need to be processed
  // There are three types of processing for segments
  //   1.ActiveSegment/ActiveInWriteSegment
  //     Segments that meet compression conditions will be set to inactive
  //   2.InActiveSegment
  //     Attempt to truncate files, compress data, and clean up the original data directory for inactive segments
  //   3.ImmuSegment
  //     Attempting to clean up the original data directory for compressed segments
  for (auto& segment_tbl : segment_tables) {
    switch (segment_tbl->getSegmentStatus()) {
      case ActiveSegment:
        if (maxTimestamp() >= compress_ts) {
          // For segments in partitions where the timestamp of some data is less than compress_ts,
          // if the following two conditions are met, they can be set as inactive segment.
          //   1. the maximum timestamp of the data saved by the segment is less than compress_ts
          //   2. the pre allocated space of the segment is 90% full
          if (segment_tbl->maxTimestamp() / 1000 < compress_ts
              && segment_tbl->size() >= 0.9 * segment_tbl->getReservedRows()) {
            segment_tbl->setSegmentStatus(InActiveSegment);
            MUTEX_LOCK(active_segment_lock_);
            if (active_segment_ && active_segment_->segment_id() == segment_tbl->segment_id()) {
              active_segment_ = nullptr;
            }
            MUTEX_UNLOCK(active_segment_lock_);
          }
        } else {
          // For segments in partitions with all data timestamps less than compress_ts,
          // one of the following two conditions can be met to set the segment to inactive state.
          //   1. segment did not perform data writing within one compression cycle
          //   2. the last write time of segment data is less than or equal to compress_ts
          string ts_file_path = db_path_ + segment_tbl->tbl_sub_path() + name_ + ".0";
          int64_t modify_time = ModifyTime(ts_file_path);
          if (modify_time <= now() - g_compress_interval || modify_time <= compress_ts) {
            segment_tbl->setSegmentStatus(InActiveSegment);
            MUTEX_LOCK(active_segment_lock_);
            if (active_segment_ && active_segment_->segment_id() == segment_tbl->segment_id()) {
              active_segment_ = nullptr;
            }
            MUTEX_UNLOCK(active_segment_lock_);
          }
        }
        break;
      case InActiveSegment: {
        // Truncate the data file for the segment.
        // The reason is that there may be unused block items in the segment, which can be removed from the free space
        // if (segment_tbl->truncate() < 0) {
        //   continue;
        // }
        // Compress segment data
        LOG_INFO("MMapSegmentTable[%s] compress start", segment_tbl->realFilePath().c_str());
        segment_tbl->setSegmentStatus(ImmuSegment);
        bool ok = compress(db_path_, tbl_sub_path_, std::to_string(segment_tbl->segment_id()),
                           g_mk_squashfs_option.processors_scheduled, err_info);
        if (!ok) {
          // If compression fails, restore segment state
          segment_tbl->setSegmentStatus(InActiveSegment);
          LOG_ERROR("MMapSegmentTable[%s] compress failed", segment_tbl->realFilePath().c_str());
          return;
        }
        segment_tbl->setSqfsIsExists();
        // Check if it is mounted. If it is not, try cleaning up the original data directory before compression.
        // It is necessary to ensure that the original data directory is not used by other threads before cleaning up.
        BLOCK_ID segment_id = segment_tbl->segment_id();
        if (!isMounted(db_path_ + segment_tbl->tbl_sub_path())) {
          if (!reloadSegment(segment_tbl, false, err_info)) {
            LOG_ERROR("MMapSegmentTable[%s] reload failed", segment_tbl->realFilePath().c_str());
            return;
          }
        }
        LOG_INFO("MMapSegmentTable[%s] compress succeeded", segment_tbl->realFilePath().c_str());
        break;
      }
      case ImmuSegment: {
        // Check if it is mounted. If it is not, try cleaning up the original data directory before compression.
        // It is necessary to ensure that the original data directory is not used by other threads before cleaning up.
        BLOCK_ID segment_id = segment_tbl->segment_id();
        if (!isMounted(db_path_ + segment_tbl->tbl_sub_path())) {
          if (!reloadSegment(segment_tbl, false, err_info)) {
            LOG_ERROR("MMapSegmentTable[%s] reload failed", segment_tbl->realFilePath().c_str());
            return;
          }
        }
        break;
      }
      default:
        break;
    }
  }
}

void TsTimePartition::Compress(const timestamp64& compress_ts, ErrorInfo& err_info){
  if (compress_ts == INT64_MAX){
    ImmediateCompress(err_info);
  } else {
    ScheduledCompress(compress_ts, err_info);
  }
}

int TsTimePartition::Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  meta_manager_.sync(MS_SYNC);
  return 0;
}

int TsTimePartition::Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows, ErrorInfo& err_info) {
  sync(MS_SYNC);
  meta_manager_.sync(MS_SYNC);

  std::vector<uint32_t> entities = meta_manager_.getEntities();
  for (auto entity_id: entities) {
    rows.insert(std::make_pair(entity_id, size(entity_id)));
  }

  return 0;
}

int TsTimePartition::DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  *count = 0;
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->cur_block_id == 0) {
    deleteEntityItem(entity_id);
    SetDeleted();
    return 0;
  }
  meta_manager_.lock();
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  // Delete from current block in reverse order
  while (true) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item->block_id == 0) {
      LOG_WARN("BlockItem[%u, %u] error: No space has been allocated", entity_id, block_item_id)
      continue;
    }
    // Add deleted rows.
    uint32_t block_row_cnt = block_item->publish_row_count - block_item->getDeletedCount();
    *count += block_row_cnt;

    if (block_item->prev_block_id <= 0) {
      break;
    }
    // Point to the previous block
    block_item_id = block_item->prev_block_id;
  }

  // Mark the entity item deleted.
  deleteEntityItem(entity_id);
  // As long as there is data deletion in this partition, it needs to be reorganized
  SetDeleted();
  meta_manager_.unlock();
  return 0;
}

int TsTimePartition::DeleteData(uint32_t entity_id, kwdbts::TS_LSN lsn, const std::vector<KwTsSpan>& ts_spans,
                                vector<DelRowSpan>* delete_rows, uint64_t* count,
                                ErrorInfo& err_info, bool evaluate_del) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  // 1. using primary_tag\start\end, get all satisfied rows
  // 2.mark rows delete flag
  EntityItem* entity_item = getEntityItem(entity_id);
  BLOCK_ID block_item_id = entity_item->cur_block_id;

  while (block_item_id != 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_WARN("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }

    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    if (!isTimestampInSpans(ts_spans, block_min_ts, block_max_ts)) {
      // blockitem ts span  not cross with ts_spans, no need scan data.
      block_item_id = block_item->prev_block_id;
      continue;
    }

    // scan all data in this blockitem
    DelRowSpan row_span;
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    int delete_num = 0;
    for (k_uint32 row_idx = 1; row_idx <= block_item->alloc_row_count ; ++row_idx) {
      bool has_been_deleted = !segment_tbl->IsRowVaild(block_item, row_idx);
      auto cur_ts = KTimestamp(segment_tbl->columnAddr(block_item->getRowID(row_idx), 0));
      if (has_been_deleted || !isTimestampInSpans(ts_spans, cur_ts, cur_ts)) {
        continue;
      }
      if (!evaluate_del) {
        if (block_item->setDeleted(row_idx) < 0) {
          entity_item_latch->Lock(entity_id);
          entity_item->row_written -= delete_num;
          entity_item_latch->Unlock(entity_id);
          return -1;
        }
      } else if (delete_rows) {
        markDeleted(row_span.delete_flags, row_idx);
      }
      (*count)++;
      delete_num++;
      // As long as there is data deletion in this partition, it needs to be reorganized
      SetDeleted();
    }
    if (!evaluate_del) {
      entity_item_latch->Lock(entity_id);
      entity_item->row_written -= delete_num;
      entity_item_latch->Unlock(entity_id);
    }
    if (evaluate_del && delete_rows) {
      row_span.blockitem_id = block_item_id;
      delete_rows->emplace_back(row_span);
    }

    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

int TsTimePartition::UndoPut(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t start_row, size_t num,
                             kwdbts::Payload* payload, ErrorInfo& err_info) {
  size_t p_count = num;
  if (p_count == 0) {
    return 0;
  }
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item == nullptr) {
    LOG_ERROR("EntityItem[%u] error: entity_item is null", entity_id);
    return KWENOOBJ;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  auto start_time = payload->GetTimestamp(start_row);
  auto end_time = payload->GetTimestamp(num - 1);
  if (start_time > entity_item->max_ts || end_time < entity_item->min_ts) {
    LOG_ERROR("Payload data is not within this entity, entity_id = %u", entity_id);
    return KWENOOBJ;
  }
  // Iterate through all blocks and verify if there is matching data (with identical timestamp and LSN).
  // If such data is found, an undo operation is required to mark the data as deleted.
  while (block_item_id != 0 && p_count > 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item == nullptr) {
      LOG_ERROR("BlockItem[%u, %u] error: block_item is null", entity_id, block_item_id);
      return KWENOOBJ;
    }
    if (block_item->block_id == 0) {
      LOG_WARN("BlockItem[%u, %u] error: No space has been allocated", entity_id, block_item_id)
      block_item_id = block_item->prev_block_id;
      continue;
    }

    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_WARN("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }
    if (segment_tbl->getSegmentStatus() >= InActiveSegment) {
      LOG_WARN("Segment [%s] is not ActiveSegment", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }

    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    if (block_max_ts < start_time || block_min_ts > end_time) {
      block_item_id = block_item->prev_block_id;
      continue;
    }
    if (segment_tbl->getSchemaInfo()[0].type != DATATYPE::TIMESTAMP64_LSN) {
      LOG_ERROR("The data type in the first column is not TIMESTAMP64_LSN.");
      return KWEDATATYPEMISMATCH;
    }

    // scan all data in blockitem
    k_uint32 row_count = block_item->publish_row_count;
    int undo_num = 0;
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    for (k_uint32 row_idx = 1; row_idx <= row_count; ++row_idx) {
      if (p_count == 0) {
        entity_item_latch->Lock(entity_id);
        entity_item->row_written -= undo_num;
        entity_item_latch->Unlock(entity_id);
        return 0;
      }
      TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(segment_tbl->columnAddr(
          MetricRowID{block_item->block_id, row_idx}, 0));
      if (ts_lsn->lsn != lsn) {
        continue;
      }
      SetDeleted();
      block_item->setDeleted(row_idx);
      block_item->is_agg_res_available = false;
      p_count--;
      undo_num++;
    }
    entity_item_latch->Lock(entity_id);
    entity_item->row_written -= undo_num;
    entity_item_latch->Unlock(entity_id);
    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

int TsTimePartition::UndoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn,
                                const vector<DelRowSpan>* rows, ErrorInfo& err_info) {
  uint32_t undo_num = 0;
  /*
     1. struct BlockItem.rows_delete_flags records the delete flags for 1000 rows in the data block.
        A bit of 1 indicates that the row has been deleted
     2. struct DelRowSpan uniquely locates a block item and records which rows in the data block are deleted
        when DeleteData is called. The deleted row bit is set to 1
     3. undo delete needs to change the bit corresponding to the row that was set to 1 during deletion back to 0
     */
  for (auto row_span : *rows) {
    BlockItem* block_item = GetBlockItem(row_span.blockitem_id);
    for (int i = 0; i < block_item->publish_row_count; i++) {
      if (isRowDeleted(row_span.delete_flags, i + 1)) {
        setRowValid(block_item->rows_delete_flags, i + 1);
        undo_num++;
      }
    }
    if (block_item->publish_row_count >= block_item->max_rows_in_block) {
      block_item->is_agg_res_available = false;
    }
  }
  TsHashLatch* entity_item_latch = GetEntityItemLatch();
  entity_item_latch->Lock(entity_id);
  EntityItem* entity_item = getEntityItem(entity_id);
  entity_item->row_written += undo_num;
  entity_item_latch->Unlock(entity_id);
  return 0;
}

int TsTimePartition::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  int err_code = 0;
  EntityItem* entity_item = getEntityItem(entity_id);
  entity_item->is_deleted = false;
  return err_code;
}

int TsTimePartition::RedoPut(kwdbts::kwdbContext_p ctx, uint32_t entity_id, kwdbts::TS_LSN lsn,
                             uint64_t start_row, size_t num, kwdbts::Payload* payload,
                             std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
                             std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map, KTimestamp p_time,
                             ErrorInfo& err_info) {
  int64_t err_code = 0;
  err_info.errcode = 0;
  alloc_spans->clear();
  // Inspect whether the partition table's current state is valid
  if (!isValid()) {
    err_info.errcode = KWENOOBJ;
    return KWENOOBJ;
  }

  // 1.Calculate the minimum and maximum timestamps for the payload to prepare for subsequent deduplication.
  // If the current payload's timestamp range does not overlap with the entity's, there is no need to deduplicate.
  KTimestamp pl_min = INT64_MAX, pl_max = INT64_MIN;
  for (size_t i = start_row; i < start_row + num; i++) {
    KTimestamp cur_ts = KTimestamp(payload->GetColumnAddr(i, 0));
    if (pl_min > cur_ts) {
      pl_min = cur_ts;
    }
    if (pl_max < cur_ts) {
      pl_max = cur_ts;
    }
  }

  size_t count = 0;
  kwdbts::DedupInfo dedup_info;
  int deleted_rows = 0;
  {
    MUTEX_LOCK(vacuum_insert_lock_);
    // 2.GetAndAllocateAllDataSpace finds the block corresponding to the data that has been written,
    // rolls it back, writes it to alloc_spans,
    // and then requests the block needed for the data that has not been written, and writes it to alloc_spans.
    err_code = GetAndAllocateAllDataSpace(entity_id, num, start_row, payload, lsn, partition_ts_map, p_time,
                                          alloc_spans);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "AllocateSpace failed : " + string(strerror(errno));
      MUTEX_UNLOCK(vacuum_insert_lock_);
      return err_code;
    }
    EntityItem* entity_item = getEntityItem(entity_id);
    // The need_scan_table is used to identify whether there is duplicate data that needs to be scanned for partitions,
    // If the timestamp range of the current payload does not fall within the timestamp range of the current entity,
    // there is no need to scan the entire partition.
    if (pl_min > entity_item->max_ts || entity_item->max_ts == INVALID_TS
        || pl_max < entity_item->min_ts) {
      dedup_info.need_scan_table = false;
    } else {
      dedup_info.need_scan_table = true;
    }
    // During writing, a determination is made as to whether the current payload contains disordered data.
    // If present, entity_item is marked as being disordered.
    // During subsequent queries, a judgment will be made. If the result set is unordered,
    // a secondary HASH aggregation based on AGG SCAN is required.
    if (payload->IsDisordered(start_row, num) ||
        (entity_item->max_ts != INVALID_TS && pl_min <= entity_item->max_ts)) {
      entity_item->is_disordered = true;
      SetDisordered();
    }
    // Update the maximum and minimum timestamps of entity_items to prepare for subsequent payload deduplication.
    if (pl_max > entity_item->max_ts || entity_item->max_ts == INVALID_TS) {
      entity_item->max_ts = pl_max;
    }
    if (pl_min < entity_item->min_ts || entity_item->min_ts == INVALID_TS) {
      entity_item->min_ts = pl_min;
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }
  DedupResult* dedup_result = new DedupResult();
  // before return, need publish inserted data and update meta datas.
  Defer defer{[&]() {
    delete dedup_result;
    std::vector<size_t> full_block_idx;
    MUTEX_LOCK(vacuum_insert_lock_);
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    entity_item_latch->Lock(entity_id);
    EntityItem* entity_item = getEntityItem(entity_id);
    entity_item->row_written += (num - deleted_rows);
    entity_item_latch->Unlock(entity_id);
    for (size_t i = 0; i < alloc_spans->size(); i++) {
      (*alloc_spans)[i].block_item->publish_row_count = (*alloc_spans)[i].block_item->alloc_row_count;
      std::shared_ptr<MMapSegmentTable> tbl = getSegmentTable((*alloc_spans)[i].block_item->block_id);
      if (tbl == nullptr) {
        LOG_ERROR("getSegmentTable failed, block_id: %u", (*alloc_spans)[i].block_item->block_id);
        return;
      }
      const BlockSpan& span = (*alloc_spans)[i];
      tbl->updateAggregateResult(span, true);
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }};

  // 3.Deduplication is performed and the payload is updated according
  // to different deduplication rules (KEEP, REJECT, MERGE).
  // Inspect the payload for any duplicate timestamps
  // and apply various treatments, such as checking for duplicate rows and merging them.
  err_code = updatePayloadUsingDedup(entity_id, (*alloc_spans)[0], payload, start_row, num, dedup_info);
  if (err_code < 0) {
    err_info.errcode = err_code;
    err_info.errmsg = "updatePayloadUsingDedup failed : " + string(strerror(errno));
    return err_code;
  }

  // 4.Retrieve the allocated block_item from alloc_spans and write the data to the pre-allocated space
  // After the writing is completed, corresponding processing (error reporting, data deletion)
  // will be performed according to the deduplication mode (REJECT, DISCARD).
  for (size_t idx = 0; idx < alloc_spans->size(); idx++) {
    const BlockSpan& span = (*alloc_spans)[idx];
    MetricRowID entity_num_node = meta_manager_.GetFileStartRow(span);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(span.block_item->block_id);
    if (!segment_tbl) {
      err_info.errcode = KWENOOBJ;
      err_info.errmsg = "Segment [" + db_path_ + segment_tbl_sub_path(span.block_item->block_id) + "] is null";
      return err_code;
    }
    // Write in a columnar format and update the aggregated results
    uint32_t inc_unordered_cnt = 0;
    err_code = segment_tbl->PushPayload(entity_id, entity_num_node, payload,
                                        start_row + count, span, &inc_unordered_cnt, dedup_info);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "PushPayload failed : " + string(strerror(errno));
      return err_code;
    }
    err_code = ProcessDuplicateData(payload, start_row, count, span, dedup_info, dedup_result, err_info, &deleted_rows);
    if (err_code < 0) {
      return err_code;
    }
    count += span.row_num;
  }
  if (payload->dedup_rule_ == DedupRule::OVERRIDE || payload->dedup_rule_ == DedupRule::MERGE) {
    // In the deduplication mode, it is necessary to
    // record the rows that need to be deleted after deduplication for subsequent deletion.
    // return: to be deleted rows in tables.
    for (auto kv : dedup_info.table_real_rows) {
      todo_markdel->insert(todo_markdel->end(), kv.second.begin(), kv.second.end());
    }
  }

  err_info.errcode = err_code;
  return err_code;
}

int TsTimePartition::RedoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn,
                                const vector<DelRowSpan>* rows, ErrorInfo& err_info) {
  for (auto row_span : *rows) {
    // Retrieve the block item based on its ID
    BlockItem* block_item = GetBlockItem(row_span.blockitem_id);
    for (int i = 0; i < 128; ++i) {
      // Iterate through each bit in the delete flags
      if (row_span.delete_flags[i] == 0) {
        continue;
      }
      block_item->rows_delete_flags[i] |= row_span.delete_flags[i];
    }
  }

  return 0;
}

int TsTimePartition::GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse) {
  return meta_manager_.GetAllBlockItems(entity_id, block_items, reverse);
}

int TsTimePartition::AllocateAllDataSpace(uint entity_id, size_t batch_num, std::vector<BlockSpan>* spans,
                                          uint32_t payload_table_version) {
  int err_code = 0;
  size_t left_count = batch_num;
  BlockItem* block_item = nullptr;
  do {
    // Acquire or allocate a BlockItem
    err_code = allocateBlockItem(entity_id, &block_item, payload_table_version);
    if (err_code < 0) {
      // If allocation fails, exit the loop
      break;
    }
    BlockSpan span;
    // Count the number of rows allocated this time and update the span
    span.start_row = block_item->alloc_row_count;
    span.row_num = std::min(left_count, size_t(block_item->max_rows_in_block - block_item->alloc_row_count));
    span.block_item = block_item;
    block_item->alloc_row_count += span.row_num;
    // block_item->publish_row_count = block_item->alloc_row_count;
    spans->push_back(span);

    left_count -= span.row_num;
    // If there are remaining rows, continue the loop
  } while (left_count > 0);

  // rollback all allocated space. mark deleted now.
  if (err_code < 0) {
    for (auto & span : *spans) {
      meta_manager_.MarkSpaceDeleted(entity_id, &span);
    }
    spans->clear();
  }
  // Update entity's row_allocated
  auto entity = getEntityItem(entity_id);
  entity->entity_id = entity_id;
  entity->row_allocated += batch_num;

  return err_code;
}

int TsTimePartition::GetAndAllocateAllDataSpace(uint entity_id, size_t batch_num, uint64_t start_row,
                                                kwdbts::Payload* payload, kwdbts::TS_LSN lsn,
                                                std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map,
                                                KTimestamp p_time, std::vector<BlockSpan>* spans) {
  int err_code = 0;
  size_t left_count = batch_num;
  BlockItem* block_item = nullptr;
  BlockSpan span;
  std::deque<BlockItem*> block_items;

  void* ts_begin = payload->GetColumnAddr(start_row, 0);
  timestamp64 start_payload_ts = *(reinterpret_cast<timestamp64*>((intptr_t) (ts_begin)));

  uint32_t pre_block_row = 0;
  BlockItem* pre_block_item = nullptr;

  FindFirstBlockItem(entity_id, lsn, start_payload_ts, block_items, &pre_block_item, &pre_block_row,
                     partition_ts_map, p_time);
  EntityItem* entity_item = getEntityItem(entity_id);
  do {
    // If pre_block_item is not null, it indicates that the block_item has already been allocated space,
    // eliminating the need for another allocation. If it is null,
    // it signifies that the previously requested space has been exhausted, necessitating the acquisition of a new one.
    if (pre_block_item != nullptr) {
      TsHashLatch* entity_item_latch = GetEntityItemLatch();
      entity_item_latch->Lock(entity_id);
      entity_item->row_written -= (pre_block_item->publish_row_count - pre_block_row);
      entity_item_latch->Unlock(entity_id);
      batch_num -= std::min(batch_num, size_t(pre_block_item->alloc_row_count));
      pre_block_item->alloc_row_count = pre_block_row;
      pre_block_item->publish_row_count = pre_block_row;
      pre_block_item->is_agg_res_available = false;
      block_item = pre_block_item;
      auto first_row = pre_block_row > 0 ? pre_block_row : 1;
      auto row_count = std::min(left_count, size_t(getMaxRowsInBlock(block_item) - block_item->alloc_row_count));
      setBatchValid(block_item->rows_delete_flags, first_row, row_count);
    } else {
      err_code = allocateBlockItem(entity_id, &block_item, payload->GetTsVersion());
      if (err_code < 0) {
        // If allocation fails, exit the loop
        break;
      }
      // Count the number of rows allocated this time and update the span
    }
    span.start_row = block_item->alloc_row_count;
    span.row_num = std::min(left_count, size_t(block_item->max_rows_in_block - block_item->alloc_row_count));
    span.block_item = block_item;
    block_item->alloc_row_count += span.row_num;
    // block_item->publish_row_count = block_item->alloc_row_count;
    spans->push_back(span);
    left_count -= span.row_num;
    if (!block_items.empty()) {
      pre_block_item = block_items.front();
      block_items.pop_front();
    } else {
      pre_block_item = nullptr;
    }
    pre_block_row = 0;
    // If there are remaining rows, continue the loop
  } while (left_count > 0);

  // rollback all allocated space. mark deleted now. later, we can reuse this space.
  if (err_code < 0) {
    for (size_t i = 0; i < spans->size(); i++) {
      meta_manager_.MarkSpaceDeleted(entity_id, &(spans->at(i)));
    }
    spans->clear();
  }
  partition_ts_map->insert(
      std::make_pair(p_time, MetricRowID{block_item->block_id, span.start_row + span.row_num + 1}));
  // Update entity's row_allocated
  auto entity = getEntityItem(entity_id);
  entity->row_allocated += batch_num;
  return err_code;
}

int TsTimePartition::allocateBlockItem(uint entity_id, BlockItem** blk_item, uint32_t payload_table_version) {
  int err_code = 0;
  BlockItem* block_item = nullptr;
  bool need_add_block_item = false;
  uint32_t new_segment_table_version = payload_table_version;
  uint32_t segment_table_version = payload_table_version;

  auto entity_item = getEntityItem(entity_id);

  // Determines whether a new BlockItem needs to be added.
  if (entity_item->cur_block_id == 0) {
    need_add_block_item = true;
  } else {
    block_item = GetBlockItem(entity_item->cur_block_id);
    // If all the space in the current BlockItem has been allocated or is unavailable, then it is necessary to add a new BlockItem.
    if (isReadOnly(block_item, payload_table_version)) {
      need_add_block_item = true;
    }
  }

  if (need_add_block_item) {
    // Attempts to add a new BlockItem to the metadata.
    err_code = meta_manager_.AddBlockItem(entity_id, &block_item);
    if (err_code < 0) return err_code;

    // Allocates a new data block.
    MUTEX_LOCK(active_segment_lock_);
    Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};
    // Checks if the current segment is already full.
    BLOCK_ID segment_block_num = meta_manager_.getEntityHeader()->cur_block_id
                                 - meta_manager_.getEntityHeader()->cur_datafile_id;
    if (active_segment_) {
      segment_table_version = active_segment_->schemaVersion();
    }
    if (segment_table_version > payload_table_version) {
      new_segment_table_version = segment_table_version;
    }
    uint32_t max_blocks_per_segment = 0;
    if (LIKELY(active_segment_ != nullptr)) {
      max_blocks_per_segment = active_segment_->getBlockMaxNum();
    } else {
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(meta_manager_.getEntityHeader()->cur_block_id - 1);
      if (segment_tbl) {
        max_blocks_per_segment = segment_tbl->getBlockMaxNum();
      }
    }
    if (segment_block_num >= max_blocks_per_segment || segment_table_version < payload_table_version) {
      active_segment_ = nullptr;
    }

    // If there is no active segment currently, or if it's already read-only, then it's necessary to create a new active segment.
    if (active_segment_ == nullptr || active_segment_->getSegmentStatus() >= InActiveSegment) {
      ErrorInfo err_info;
      // Uses the current maximum block ID plus one as the segment ID.
      BLOCK_ID next_segment_id = meta_manager_.getEntityHeader()->cur_block_id;
      MMapSegmentTable* tbl = createSegmentTable(next_segment_id, new_segment_table_version, err_info);
      if (tbl == nullptr || err_info.errcode < 0) {
        meta_manager_.getEntityHeader()->cur_block_id--;
        LOG_ERROR("createSegmentTable error: %s", err_info.errmsg.c_str());
        return err_info.errcode;
      }
      tbl->setSegmentStatus(ActiveSegment);
      std::shared_ptr<MMapSegmentTable> segment_table(tbl);
      data_segments_.Insert(next_segment_id, segment_table);

      active_segment_ = segment_table;
      meta_manager_.getEntityHeader()->cur_datafile_id = next_segment_id;
      root_table_manager_->UpdateTableVersionOfLastData(new_segment_table_version);
    }

    block_item->is_overflow = false;
    block_item->is_agg_res_available = false;
    block_item->max_rows_in_block = active_segment_->getBlockMaxRows();

    // Updates the metadata of the active segment.
    active_segment_->metaData()->block_num_of_segment++;
    // Checks if the partition table has reached its maximum Block quantity.
    if (ReachMetaMaxBlock(meta_manager_.getEntityHeader()->cur_block_id)) {
      LOG_FATAL("fatal: partition table block num reach max.");
      return KWENOSPC;
    }
    // Updates the corresponding BlockItem information.
    meta_manager_.UpdateEntityItem(entity_id, block_item);
  }
  // Returns the allocated BlockItem.
  *blk_item = block_item;
  return err_code;
}

void TsTimePartition::publish_payload_space(const std::vector<BlockSpan>& alloc_spans,
                                            const std::vector<MetricRowID>& delete_rows, uint32_t entity_id,
                                            bool success) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  // All requested space is marked as deleted space.
  if (!success) {
    for (auto alloc_span : alloc_spans) {
      meta_manager_.MarkSpaceDeleted(entity_id, const_cast<BlockSpan*>(&alloc_span));
    }
  } else {
    int delete_num = 0;
    for (auto delete_row : delete_rows) {
      if (delete_row.block_id == 0) {
        continue;
      }
      BlockItem* cur_blk_item = meta_manager_.GetBlockItem(delete_row.block_id);
      SetDeleted();
      cur_blk_item->setDeleted(delete_row.offset_row);
      delete_num++;
    }
    EntityItem* entity_item = getEntityItem(entity_id);
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    entity_item_latch->Lock(entity_id);
    entity_item->row_written -= delete_num;
    entity_item_latch->Unlock(entity_id);
  }
}

int TsTimePartition::PrepareDup(kwdbts::DedupRule dedup_rule, uint32_t entity_id,
                                   const std::vector<KwTsSpan>& ts_spans) {
  if (dedup_rule == kwdbts::DedupRule::OVERRIDE) {
    uint64_t cnt = 0;
    ErrorInfo err_info;
    return DeleteData(entity_id, 0, ts_spans, nullptr, &cnt, err_info, false);
  }
  return 0;
}

const size_t PAYLOAD_VARCOLUMN_TUPLE_LEN = 8;

int TsTimePartition::mergeToPayload(kwdbts::Payload* payload, size_t merge_row, kwdbts::DedupInfo& dedup_info) {
  KTimestamp cur_ts = KTimestamp(payload->GetColumnAddr(merge_row, 0));
  // Check the timestamp. If it is not present in the deduplication set, simply return.
  if (dedup_info.payload_rows.find(cur_ts) == dedup_info.payload_rows.end()) {
    // filtered by entity min and max ts.
    return 0;
  }
  auto schema_info = payload->GetSchemaInfo();
  auto actual_cols = payload->GetActualCols();
  const std::vector<size_t>& payload_dedup = dedup_info.payload_rows[cur_ts];
  for (size_t column = 0; column < actual_cols.size(); ++column) {
    if (!payload->IsNull(column, merge_row)) {
      // column value is not null, so no need merge from duplicate rows.
      continue;
    }
    if (payload_dedup.size() > 1) {
      // payload has duplicate rows of this ts.
      for (int i = payload_dedup.size() - 1; i >= 0; i--) {
        // find begin from the latest rows.
        if (payload_dedup[i] >= merge_row || payload->IsNull(column, payload_dedup[i])) {
          continue;
        }
        if (schema_info[column].type != DATATYPE::VARSTRING && schema_info[column].type != DATATYPE::VARBINARY) {
          memcpy(payload->GetColumnAddr(merge_row, column),
                 payload->GetColumnAddr(payload_dedup[i], column),
              schema_info[column].size);
        } else {
          memcpy(payload->GetColumnAddr(merge_row, column),
                 payload->GetColumnAddr(payload_dedup[i], column), PAYLOAD_VARCOLUMN_TUPLE_LEN);
        }
        // The merged column now contains values; the bitmap needs to be updated accordingly
        setRowValid(payload->GetNullBitMapAddr(column), merge_row + 1);
        break;
      }
    }
    if (!payload->IsNull(column, merge_row)) {
      continue;
    }
    // find from table entity, start from latest.
    if (dedup_info.table_real_rows.find(cur_ts) != dedup_info.table_real_rows.end()) {
      for (int i = dedup_info.table_real_rows[cur_ts].size() - 1; i >= 0; i--) {
        // The actual row number of the stored data
        MetricRowID dup_row = dedup_info.table_real_rows[cur_ts][i];
        if (dup_row.block_id == 0) {
          continue;
        }
        std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(dup_row.block_id);
        if (segment_tbl == nullptr) {
          LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(dup_row.block_id)).c_str());
          return -1;
        }
        if (segment_tbl->isNullValue(dedup_info.table_real_rows[cur_ts][i], actual_cols[column])) {
          continue;
        }
        // Verify the column types for compatibility, and perform type conversion if they do not match
        if (segment_tbl->getSchemaInfo()[column].type != schema_info[column].type
            || segment_tbl->getSchemaInfo()[column].size != schema_info[column].size) {
          MergeValueInfo merge_value;
          merge_value.attr = segment_tbl->getSchemaInfo()[actual_cols[column]];
          if (merge_value.attr.type == VARSTRING || merge_value.attr.type == VARBINARY) {
            merge_value.value = segment_tbl->varColumnAddr(dedup_info.table_real_rows[cur_ts][i], actual_cols[column]);
          } else {
            void* segment_merge_data = segment_tbl->columnAddr(dedup_info.table_real_rows[cur_ts][i], actual_cols[column]);
            CopyFixedData(static_cast<DATATYPE>(segment_tbl->getSchemaInfo()[actual_cols[column]].type),
                          static_cast<char*>(segment_merge_data), &merge_value.value);
          }
          if (payload->tmp_col_values_4_dedup_merge_.find(column) == payload->tmp_col_values_4_dedup_merge_.end()) {
            payload->tmp_col_values_4_dedup_merge_.insert({column, {}});
          }
          payload->tmp_col_values_4_dedup_merge_[column].insert({merge_row, merge_value});
        } else {
          if (schema_info[column].type != DATATYPE::VARSTRING && schema_info[column].type != DATATYPE::VARBINARY) {
            memcpy(payload->GetColumnAddr(merge_row, column),
                   segment_tbl->columnAddr(dedup_info.table_real_rows[cur_ts][i], actual_cols[column]),
              schema_info[column].size);
          } else {
            MergeValueInfo merge_value;
            merge_value.attr = segment_tbl->getSchemaInfo()[actual_cols[column]];
            merge_value.value = segment_tbl->varColumnAddr(dedup_info.table_real_rows[cur_ts][i], actual_cols[column]);
            if (payload->tmp_col_values_4_dedup_merge_.find(column) == payload->tmp_col_values_4_dedup_merge_.end()) {
              payload->tmp_col_values_4_dedup_merge_.insert({column, {}});
            }
            payload->tmp_col_values_4_dedup_merge_[column].insert({merge_row, merge_value});
          }
        }
        // The merged column now contains values; the bitmap needs to be updated accordingly
        setRowValid(payload->GetNullBitMapAddr(column), merge_row + 1);
        break;
      }
    }
  }
  return 0;
}

int TsTimePartition::updatePayloadUsingDedup(uint32_t entity_id, const BlockSpan& first_span,
                                             kwdbts::Payload* payload, size_t start_in_payload, size_t num,
                                             kwdbts::DedupInfo& dedup_info) {
  int err_code = 0;
  // If the deduplication rule for the payload is KEEP, it will be returned directly
  if (payload->dedup_rule_ == DedupRule::KEEP) {
    return err_code;
  }
  bool ts_with_lsn = payload->GetSchemaInfo()[0].type == DATATYPE::TIMESTAMP64_LSN;

  void* ts_begin = payload->GetColumnAddr(0, 0);
  dedup_info.need_dup = true;
  // Search for data elements that require updating, and handle duplicate data in accordance with the deduplication pattern
  for (int i = 0; i < num; ++i) {
    timestamp64 current_ts = *(reinterpret_cast<timestamp64*>((intptr_t) (ts_begin)
                             + (start_in_payload + i) * payload->GetSchemaInfo()[0].size));
    // If it is in REJECT deduplication mode and duplicate lines are found, an error code will be returned
    if (payload->dedup_rule_ == DedupRule::REJECT && dedup_info.payload_rows.find(current_ts) != dedup_info.payload_rows.end()) {
      LOG_ERROR("payload has duplicated rows. ts %lu", current_ts);
      // payload has duplicated rows. in reject mode, need failed.
      return KWEDUPREJECT;
    }
    // Record the information of duplicate rows
    dedup_info.payload_rows[current_ts].push_back(start_in_payload + i);
  }
  // If there is no need to scan the table, simply return
  if (!dedup_info.need_scan_table) {
    return err_code;
  }

  // find all duplicate rows.
  // Within the specified BlockSpan range, retrieve unique rows that fall within a specific timestamp range.
  err_code = GetDedupRows(entity_id, first_span, dedup_info, ts_with_lsn);
  if (err_code < 0) {
    return err_code;
  }
  if (payload->dedup_rule_ == DedupRule::MERGE) {
    // If the deduplication rule is MERGE and the current payload column is empty,
    // traverse the duplicate data to find the latest duplicate, and update the payload.
    // When updating the payload, it is necessary to check for any type changes in the merge data and perform type conversion.
    // update payload.
    for (size_t i = start_in_payload; i < start_in_payload + num; i++) {
      err_code = mergeToPayload(payload, i, dedup_info);
      if (err_code < 0) {
        return err_code;
      }
    }
  }
  return err_code;
}

int TsTimePartition::GetDedupRowsInBlk(uint entity_id, BlockItem* block_item, int read_count, kwdbts::DedupInfo& dedup_info, bool has_lsn) {

  for (uint32_t i = 1; i <= read_count; i++) {
    bool is_deleted;
    if (block_item->isDeleted(i, &is_deleted) < 0) {
      LOG_ERROR("BlockItem::isDeleted failed");
      return KWELENLIMIT;
    }
    if (is_deleted) {
      continue;
    }
    MetricRowID real_row = block_item->getRowID(i);

    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (!segment_tbl) {
      LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      return KWENOOBJ;
    }

    // Retrieve the timestamp address of the current row and parse it
    char* ts_addr = reinterpret_cast<char*>(segment_tbl->columnAddr(real_row, 0));
    timestamp64 cur_ts = KTimestamp(ts_addr);
    // If the current timestamp is already present in the deduplication information, record the ID of the row
    if (dedup_info.payload_rows.find(cur_ts) != dedup_info.payload_rows.end()) {
      dedup_info.table_real_rows[cur_ts].push_back(real_row);
    }
  }
  return 0;
}

void TsTimePartition::waitBlockItemDataFilled(uint entity_id, BlockItem* block_item, int read_count, bool has_lsn) {
  time_t begin_time = time(nullptr);
  // If the number of allocated rows equals the number of published rows, return immediately
  if (block_item->alloc_row_count == block_item->publish_row_count) {
    return;
  }
  size_t cur_read_rownum = 1;
  std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
  if (!segment_tbl) {
    LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
    return;
  }
  MetricRowID real_row = block_item->getRowID(cur_read_rownum);
  // When the number of allocated rows in a Block differs from the number of published rows, and the current row's
  // offset_row does not exceed the expected number of rows to be read, a loop check is performed.
  while (block_item->alloc_row_count != block_item->publish_row_count && real_row.offset_row < read_count) {
    bool move_step = false;
    char* ts_addr = reinterpret_cast<char*>(segment_tbl->columnAddr(real_row, 0));
    // If an LSN is present, verify that it is greater than 0; if not, ascertain if the timestamp is greater than 0
    if (has_lsn) {
      uint64_t lsn = KUint64(ts_addr + 8);
      if (lsn > 0) {
        move_step = true;
      }
    } else {
      if (KTimestamp(ts_addr) > 0) {  // timestampe is set, so data is inserted ok.
        move_step = true;
      }
    }
    // If the current row's waiting time exceeds 10 seconds, it is marked as deleted
    if (time(nullptr) - begin_time > 10) {  // check overtime, mark row deleted.
      SetDeleted();
      block_item->setDeleted(cur_read_rownum);
      LOG_WARN("check lsn overtime, mark this row deleted. entity:%u, block:%u, row: %u", entity_id,
               real_row.block_id, real_row.offset_row);
      move_step = true;
    }
    // If it is necessary to move to the next row, update the current row number and row ID
    if (move_step) {
      real_row.offset_row++;
      cur_read_rownum++;
    } else {
      // Otherwise, the thread is relinquished to avoid excessive CPU consumption
      std::this_thread::yield();
    }
  }
}

int TsTimePartition::GetDedupRows(uint entity_id, const BlockSpan& first_span, kwdbts::DedupInfo& dedup_info, bool has_lsn) {
  int err_code = 0;
  timestamp64 ts_min = INT64_MAX, ts_max = 0;
  // filter blockItem with payload min and max ts.
  for (auto kv : dedup_info.payload_rows) {
    if (kv.first > ts_max) {
      ts_max = kv.first;
    }
    if (kv.first < ts_min) {
      ts_min = kv.first;
    }
  }

  const KwTsSpan& payload_span = {ts_min, ts_max};

  std::deque<BlockItem*> block_items;
  // Get all the block items belonging to the specified entity_id.
  meta_manager_.GetAllBlockItems(entity_id, block_items);

  bool find_newest_block = false;

  // Traverse each block item to determine if there are any duplicate records that need to be removed.
  for (size_t i = 0; i < block_items.size(); i++) {
    BlockItem* block_item = block_items[i];
    int read_count = block_item->publish_row_count;

    // If the current block item matches the specified first span, update the number of records read.
    if (block_item == first_span.block_item) {
      find_newest_block = true;
      read_count = first_span.start_row;
    }
    // Wait for the block item to be filled completely.
    waitBlockItemDataFilled(entity_id, block_item, read_count, has_lsn);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (!segment_tbl) {
      LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      return KWENOOBJ;
    }
    // Check if the timestamp range of the block item overlaps with the timestamps of the deduplication information.
    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    const KwTsSpan& ts_span = {block_min_ts, block_max_ts};
    if (ts_span.begin > payload_span.end || ts_span.end < payload_span.begin) {
    // no match rows. so no need do anything.
    } else {
      // If there are records that can be read from the block item, obtain the deduplication information for these records.
      if (read_count > 0) {
        err_code = GetDedupRowsInBlk(entity_id, block_item, read_count, dedup_info, has_lsn);
      }
    }
    // Encounter an error or find the latest block item, then return the operational results.
    if (err_code < 0 || find_newest_block) {
      return err_code;
    }
  }
  return err_code;
}

int TsTimePartition::CopyFixedData(DATATYPE old_type, char* old_mem, std::shared_ptr<void>* new_mem) {
  switch (old_type) {
    case DATATYPE::INT16 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int16_t)));
      memcpy(data, old_mem, sizeof(int16_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::INT32 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int32_t)));
      memcpy(data, old_mem, sizeof(int32_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::INT64 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int64_t)));
      memcpy(data, old_mem, sizeof(int64_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::FLOAT : {
      char* data = static_cast<char*>(std::malloc(sizeof(float)));
      memcpy(data, old_mem, sizeof(float));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::DOUBLE : {
      char* data = static_cast<char*>(std::malloc(sizeof(double)));
      memcpy(data, old_mem, sizeof(double));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      char* data = static_cast<char*>(std::malloc(strlen(old_mem) + 1));
      strcpy(data, old_mem);
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    default:
      break;
  }
  return 0;
}

int TsTimePartition::ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size, void* old_mem,
                                             std::shared_ptr<void> old_var_mem, std::shared_ptr<void>* new_mem) {
  ErrorInfo err_info;
  if (!isVarLenType(new_type)) {
    void* temp_new_mem = malloc(new_type_size + 1);
    memset(temp_new_mem, 0, new_type_size + 1);
    if (!isVarLenType(old_type)) {
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, (char*)old_mem, (char*)temp_new_mem, err_info);
      } else {
        err_info.errcode = convertFixedToNum(old_type, new_type, (char*) old_mem, (char*) temp_new_mem, err_info);
      }
      if (err_info.errcode < 0) {
        free(temp_new_mem);
        return err_info.errcode;
      }
    } else {
      uint16_t var_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
      std::string var_value((char*)old_var_mem.get() + MMapStringColumn::kStringLenLen);
      convertStrToFixed(var_value, new_type, (char*) temp_new_mem, var_len, err_info);
    }
    std::shared_ptr<void> ptr(temp_new_mem, free);
    *new_mem = ptr;
  } else {
    if (!isVarLenType(old_type)) {
      auto cur_var_data = convertFixedToVar(old_type, new_type, (char*)old_mem, err_info);
      *new_mem = cur_var_data;
    } else {
      if (old_type == VARSTRING) {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get()) - 1;
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen);
        *reinterpret_cast<uint16_t*>(var_data) = old_len;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      } else {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen + 1));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen + 1);
        *reinterpret_cast<uint16_t*>(var_data) = old_len + 1;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      }
    }
  }
  return 0;
}

string convertFixedToStr(DATATYPE old_type, char* data, ErrorInfo& err_info) {
  std::string res;
  switch (old_type) {
    case DATATYPE::INT16 : {
      res = std::to_string(KInt16(data));
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(data));
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(data));
      break;
    }
    case DATATYPE::FLOAT : {
      std::ostringstream oss;
      oss.clear();
      oss.precision(7);
      oss << KFloat32(data);
      res = oss.str();
      break;
    }
    case DATATYPE::DOUBLE : {
      std::stringstream ss;
      ss << std::setprecision(16) << KDouble64(data);
      res = ss.str();
      break;
    }
    case DATATYPE::CHAR:
    case DATATYPE::BINARY: {
      string cur_s(data, strlen(data));
      res = cur_s;
      break;
    }
    default:
      err_info.setError(KWEPERM, "Incorrect integer value");
      break;
  }
  return res;
}

int TsTimePartition::tryAlterType(const std::string& str, DATATYPE new_type, ErrorInfo& err_info) {
  std::size_t pos{};
  int res = 0;
  std::string incorrect_str = "Incorrect integer value";
  std::string value_str = " '" + str + "'";
  try {
    switch (new_type) {
      case DATATYPE::INT16 : {
        res = std::stoi(str, &pos);
        break;
      }
      case DATATYPE::INT32 : {
        auto value = std::stoi(str, &pos);
        break;
      }
      case DATATYPE::INT64 : {
        auto value = std::stoll(str, &pos);
        break;
      }
      case DATATYPE::FLOAT : {
        incorrect_str = "Incorrect floating value";
        auto value = std::stof(str, &pos);
        break;
      }
      case DATATYPE::DOUBLE : {
        incorrect_str = "Incorrect floating value";
        auto value = std::stod(str, &pos);
        break;
      }
      default:
        break;
    }
  }
  catch (std::invalid_argument const &ex) {
    return err_info.setError(KWEPERM, incorrect_str + value_str);
  }
  catch (std::out_of_range const &ex) {
    return err_info.setError(KWEPERM, "Out of range value" + value_str);
  }
  if (pos < str.size()) {
    return err_info.setError(KWEPERM, incorrect_str + value_str);
  }
  if (new_type == DATATYPE::INT16) {
    if (res > INT16_MAX || res < INT16_MIN) {
      return err_info.setError(KWEPERM, "Out of range value" + value_str);
    }
  }
  return err_info.errcode;
}

int TsTimePartition::FindFirstBlockItem(uint32_t entity_id, kwdbts::TS_LSN lsn, timestamp64 start_payload_ts,
                                        std::deque<BlockItem*>& block_items,
                                        BlockItem** blk_item, uint32_t* block_start_row,
                                        std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map,
                                        KTimestamp p_time) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  if (entity_item->cur_block_id == 0) {
    // This indicates that no data has been written and a normal request to block has been made
    return 0;
  }
  BLOCK_ID pre_block_id = 0;
  auto iter = partition_ts_map->find(p_time);
  if (iter != partition_ts_map->end()) {
    pre_block_id = iter->second.block_id;
  }
  if (start_payload_ts <= entity_item->max_ts && start_payload_ts >= entity_item->min_ts &&
      entity_item->max_ts != INVALID_TS) {
    // Initiate a search from the start of the block queue. If the timestamps and LSNs within a block are identical,
    // it signifies that data has already been written, allowing the reuse of the current block,
    // and the associated starting position should be noted. If no match is found, continue the traversal.
    // If data is retrieved from the map, it implies that this batch of payloads was previously written to
    // the current partition. To prevent retrieving duplicate data,
    // initiate the search from the starting position documented in the map.
    GetAllBlockItems(entity_id, block_items);
    uint32_t offset_row = 1;
    timestamp64 block_min_ts, block_max_ts;
    while (!block_items.empty()) {
      auto block_item = block_items.front();
      block_items.pop_front();
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
        return KWENOOBJ;
      }
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
      KwTsSpan ts_span = {block_min_ts, block_max_ts};
      if (!(start_payload_ts >= ts_span.begin && start_payload_ts <= ts_span.end) ||
          pre_block_id > block_item->block_id) {
        continue;
      }
      if (pre_block_id == block_item->block_id) {
        offset_row = iter->second.offset_row;
      }
      for (uint32_t j = offset_row; j <= block_item->alloc_row_count; j++) {
        MetricRowID real_row = block_item->getRowID(j);
        TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(segment_tbl->columnAddr(real_row, 0));
        if (start_payload_ts == ts_lsn->ts64 && lsn == ts_lsn->lsn) {
          *blk_item = block_item;
          *block_start_row = j - 1;
          return 0;
        }
      }
    }
  }
  block_items.clear();
  // There may be a situation where space is requested but no data is written.
  // Determine whether this is the case based on publish_row_count and alloc_row_count.
  GetAllBlockItems(entity_id, block_items);
  while (!block_items.empty()) {
    auto block_item = block_items.front();
    block_items.pop_front();
    if (block_item->alloc_row_count == 0 || block_item->publish_row_count < block_item->alloc_row_count) {
      *blk_item = block_item;
      *block_start_row = block_item->publish_row_count;
      return 0;
    }
  }
  return 0;
}

int TsTimePartition::GetAllBlockSpans(uint32_t entity_id, std::vector<KwTsSpan>& ts_spans,
                                         std::deque<BlockSpan>& block_spans, uint32_t max_block_id, bool reverse) {
  std::deque<BlockItem*> block_queue;
  GetAllBlockItems(entity_id, block_queue);
  EntityItem *entity = getEntityItem(entity_id);

  uint32_t count = 0;
  if (!entity->is_disordered) {
    while (!block_queue.empty()) {
      BlockItem* block_item = block_queue.front();
      block_queue.pop_front();
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, GetPath().c_str());
        return -1;
      }

      uint32_t first_row = 1;
      for (uint32_t i = 1; i <= block_item->publish_row_count; ++i) {
        MetricRowID row_id = block_item->getRowID(i);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
        if (!(CheckIfTsInSpan(cur_ts, ts_spans)) || !segment_tbl->IsRowVaild(block_item, i)) {
          if (i > first_row) {
            reverse ? block_spans.push_front({block_item, first_row - 1, i - first_row}) :
                      block_spans.push_back({block_item, first_row - 1, i - first_row});
            count += i - first_row;
          }
          first_row = i + 1;
          continue;
        }
      }

      if (first_row <= block_item->publish_row_count) {
        reverse ? block_spans.push_front({block_item, first_row - 1, block_item->publish_row_count - first_row + 1}) :
                  block_spans.push_back({block_item, first_row - 1, block_item->publish_row_count - first_row + 1});
        count += block_item->publish_row_count - first_row + 1;
      }
    }
    return count;
  }

  vector<vector<timestamp64>> intervals;
  map<pair<timestamp64, timestamp64>, vector<BlockItem*>> interval_block_map;
  while (!block_queue.empty()) {
    BlockItem* cur_block = block_queue.front();
    block_queue.pop_front();
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(cur_block->block_id);
    timestamp64 min_ts, max_ts;
    GetBlkMinMaxTs(cur_block, segment_tbl.get(), min_ts, max_ts);
    if (isTimestampInSpans(ts_spans, min_ts, max_ts) && cur_block->publish_row_count > 0) {
      if (!interval_block_map.count({min_ts, max_ts})) {
        intervals.push_back({min_ts, max_ts});
      }
      interval_block_map[{min_ts, max_ts}].push_back(cur_block);
    }
  }

  sort(intervals.begin(), intervals.end());
  vector<pair<pair<timestamp64, timestamp64>, vector<BlockItem*>>> block_items;
  for (auto interval : intervals) {
    if (block_items.empty() || interval[0] >= block_items.end()->first.second) {
      block_items.push_back({{interval[0], interval[1]}, {interval_block_map[{interval[0], interval[1]}]}});
    } else {
      if (interval[1] > block_items.back().first.second) {
        block_items.back().first.second = interval[1];
      }
      block_items.back().second.insert(block_items.back().second.end(),
                                       interval_block_map[{interval[0], interval[1]}].begin(),
                                       interval_block_map[{interval[0], interval[1]}].end());
    }
  }
  intervals.clear();
  interval_block_map.clear();

  for (auto it : block_items) {
    vector<BlockItem*> blocks = it.second;
    if (blocks.size() == 1 && !blocks[0]->unordered_flag) {
      BlockSpan block_span = BlockSpan{blocks[0], 0, blocks[0]->publish_row_count};
      reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
      count += block_span.row_num;
    } else {
      std::multimap<timestamp64, MetricRowID> ts_order;  // Save every undeleted row_id under each timestamp
      for (auto block : blocks) {
        if (block->block_id > max_block_id) {
          // When vacuum calls this function, the max_block_id passed in indicates that the traversal is over
          // at the end of the block, and the other time, max_block_id = INT32_MAX.
          break;
        }
        std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block->block_id);
        if (segment_tbl == nullptr) {
          LOG_ERROR("Can not find segment use block [%u], in path [%s]", block->block_id, GetPath().c_str());
          return -1;
        }

        bool all_within_spans = isTimestampWithinSpans(ts_spans,
                                   KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MIN)),
                                   KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MAX)));

        for (uint32_t i = 1; i <= block->publish_row_count; ++i) {
          MetricRowID row_id = block->getRowID(i);
          timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
          if (all_within_spans || (CheckIfTsInSpan(cur_ts, ts_spans))) {
            if (!segment_tbl->IsRowVaild(block, i)) {
              continue;
            }
            ts_order.insert(std::make_pair(cur_ts, row_id));
          }
        }
      }
      BlockSpan block_span;
      MetricRowID last_row_id;
      for (auto& iter : ts_order) {
        // Convert a time-sorted row_id into a BlockSpan that represents the continuity of the data
        MetricRowID& cur_row_id = iter.second;
        if (last_row_id + 1 != cur_row_id) {
          // When data continuity is interrupted,
          // the data that has just been checked to be continuous is stored and the next check is started
          if (block_span.row_num > 0) {
            reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
            count += block_span.row_num;
          }
          block_span = BlockSpan{GetBlockItem(cur_row_id.block_id), cur_row_id.offset_row - 1, 1};
        } else {
          block_span.row_num++;
        }
        last_row_id = cur_row_id;
      }

      if (block_span.row_num > 0) {
        reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
        count += block_span.row_num;
      }
    }
  }
  return count;
}

void TsTimePartition::ChangeSegmentStatus() {
  // Stop writing of the segment when vacuum
  MUTEX_LOCK(active_segment_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};

  if (active_segment_ != nullptr) {
    active_segment_->setSegmentStatus(InActiveSegment);
    active_segment_ = nullptr;
  }
  // Set all the segments of this partition to InActiveSegment
  data_segments_.Traversal([&](BLOCK_ID s_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->sqfsIsExists()) {
      return true;
    }
    // reopen will change status to OBJ_READY, if not do this, setSegmentStatus will crash
    // because TsTableMMapObject::meta_data_ is nullptr
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                     db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                     false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }

    if (tbl->getSegmentStatus() < InActiveSegment) {
      tbl->setSegmentStatus(InActiveSegment); // include original active_segment_
    }
    return true;
  });
}

int TsTimePartition::DropSegmentDir(const std::vector<BLOCK_ID>& segment_ids) {
  for (unsigned int segment_id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> segment_table;
    BLOCK_ID key;
    bool ret = data_segments_.Seek(segment_id, key, segment_table);
    if (ret) {
      // set all blockitem in segment invalid.
      auto block_id = segment_id;
      while (true) {
        std::shared_ptr<MMapSegmentTable> segment_block;
        BLOCK_ID key_block;
        if (data_segments_.Seek(block_id, key_block, segment_block)) {
          if (segment_block.get() != segment_table.get()) {
            break;
          }
        } else {
          LOG_INFO("cannot found segment for block[%u]", block_id);
          break;
        }
        memset(GetBlockItem(block_id), 0, sizeof(BlockItem));
        block_id++;
        if (block_id > meta_manager_.getEntityHeader()->cur_block_id) {
          break;
        }
      }
      // delete segment directory.
      int error_code = segment_table->remove();
      if (error_code < 0) {
        LOG_ERROR("remove segment[%s] failed!", segment_table->realFilePath().c_str());
        return error_code;
      }
      LOG_INFO("remove segment[%s] success!", segment_table->realFilePath().c_str());
    }
  }
  return 0;
}

bool TsTimePartition::IsSegmentsBusy(const std::vector<BLOCK_ID>& segment_ids) {
  for (auto id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> value;
    BLOCK_ID key;
    bool res = data_segments_.Seek(id, key, value);
    if (res) {
      if (value.use_count() == 2) {
        continue;
      } else if (value.use_count() > 2) {
        LOG_WARN("segment[%s] is using, cannot vacuum now", value->realFilePath().c_str());
        return true;
      }
    }
  }
  return false;
}
