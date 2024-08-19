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
#include <lg_api.h>
#include "dirent.h"
#include "mmap/mmap_segment_table.h"
#include "utils/big_table_utils.h"
#include "date_time_util.h"
#include "cm_func.h"
#include "utils/compress_utils.h"
#include "engine.h"
#include "ts_time_partition.h"
#include "lt_rw_latch.h"
#include "perf_stat.h"
#include "sys_utils.h"

MMapSegmentTable::MMapSegmentTable() : rw_latch_(RWLATCH_ID_MMAP_SEGMENT_TABLE_RWLOCK) {
  reserved_rows_ = 1024;
}

MMapSegmentTable::~MMapSegmentTable() {
  string real_path;
  if(meta_data_) {
    real_path = bt_file_.realFilePath();
  }
  ErrorInfo err_info;
  if (close(err_info) < 0) {
    LOG_DEBUG("~MMapSegmentTable[%s] failed", real_path.c_str());
  }
}

impl_latch_virtual_func(MMapSegmentTable, &rw_latch_)

int MMapSegmentTable::create(TsEntityIdxManager* entity_idx, const vector<AttributeInfo>& schema,
                             int encoding, ErrorInfo& err_info) {
  if (init(entity_idx, schema, encoding, err_info) < 0)
    return err_info.errcode;

  meta_data_->magic = magic();
  meta_data_->struct_type |= (ST_COLUMN_TABLE);
  meta_data_->has_data = true;
  if (initColumn(bt_file_.flags(), err_info) < 0)
    return err_info.errcode;

  actual_writed_count_.store(meta_data_->num_node);
  setObjectReady();
  return err_info.errcode;
}

int MMapSegmentTable::addColumnFile(int col, int flags, ErrorInfo& err_info) {
  string col_file_name = name_ + '.' + intToString(col);
  MMapFile* col_file = new MMapFile();
  col_files_[col] = col_file;
  err_info.errcode = col_file->open(col_file_name, db_path_ + tbl_sub_path_ + col_file_name, flags);
  return err_info.setError(err_info.errcode);
}

void MMapSegmentTable::verifySchema(const vector<AttributeInfo>& root_schema, bool& is_consistent) {
  auto& schema = getSchemaInfo();
  if (schema.size() != root_schema.size()) {
    if (getSegmentStatus() != InActiveSegment) {
      // if segment status is active, we should change to inactive. so this segment cannot insert new data.
      setSegmentStatus(InActiveSegment);
    }
    is_consistent = false;
  } else {
    for (int i = 0; i < schema.size(); ++i) {
      if (schema[i].isEqual(root_schema[i])) {
        continue;
      }
      if (getSegmentStatus() != InActiveSegment) {
        setSegmentStatus(InActiveSegment);
        is_consistent = false;
        break;
      }
    }
  }
}

int MMapSegmentTable::initColumn(int flags, ErrorInfo& err_info) {
  col_files_.resize(cols_info_with_hidden_.size());
  col_block_header_size_.resize(cols_info_with_hidden_.size());
  col_block_size_.resize(cols_info_with_hidden_.size());
  for (size_t i = 0 ; i < cols_info_with_hidden_.size() ; ++i) {
    if (cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      col_files_[i] = nullptr;
      continue;
    }
    addColumnFile(i, flags, err_info);
    // check if all file open success
    if (err_info.errcode < 0) {
      return err_info.errcode;
    }
    // calculte block header size and block size
    col_block_header_size_[i] = entity_idx_->getBlockBitmapSize() + BLOCK_AGG_COUNT_SIZE
                                + cols_info_with_hidden_[i].size * BLOCK_AGG_NUM;
    col_block_header_size_[i] = (col_block_header_size_[i] + 63) / 64 * 64;

    col_block_size_[i] = col_block_header_size_[i] + cols_info_with_hidden_[i].size * entity_idx_->getBlockMaxRows();
    col_block_size_[i] = (col_block_size_[i] + 63) / 64 * 64;

    // vartype column will use string file to store real values.
    if ((cols_info_with_hidden_[i].type == VARSTRING || cols_info_with_hidden_[i].type == VARBINARY) && !m_str_file_) {
      m_str_file_ = new MMapStringFile(LATCH_ID_METRICS_STRING_FILE_MUTEX, RWLATCH_ID_METRICS_STRING_FILE_RWLOCK);
      std::string col_str_file_name = name_ + ".s";
      int err_code = m_str_file_->open(col_str_file_name, db_path_ + tbl_sub_path_ + col_str_file_name, flags);
      if (err_code < 0) {
        return err_code;
      }
      err_code = m_str_file_->reserve(4, 1024 * 1024);
      if (err_code < 0) {
        return err_code;
      }
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::open_(int char_code, const string& file_path, const std::string& db_path,
                          const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  if ((err_info.errcode = TsTableObject::open(file_path, db_path, tbl_sub_path, char_code, flags))
      < 0) {
    return err_info.errcode;
  }
  name_ = getURLObjectName(file_path);
  if (metaDataLen() >= (off_t) sizeof(TSTableFileMetadata)) {
    if (meta_data_->has_data) {
      if (initColumn(flags, err_info) < 0)
        return err_info.errcode;

      if (isTransient(meta_data_->struct_type))
        return 0;
    }
    setObjectReady();
    actual_writed_count_.store(meta_data_->num_node);
  } else {
    if (!(bt_file_.flags() & O_CREAT)) {
      err_info.errcode = KWECORR;
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::open(TsEntityIdxManager* entity_idx, BLOCK_ID segment_id, const string& file_path,
                           const std::string& db_path, const string& tbl_sub_path,
                           int flags, bool lazy_open, ErrorInfo& err_info) {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });
  if (getObjectStatus() == OBJ_READY) {
    return 0;
  }
  entity_idx_ = entity_idx;
  segment_id_ = segment_id;

  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  bt_file_.filePath() = file_path;
  bt_file_.realFilePath() = db_path + tbl_sub_path + file_path;

  // check if we should mount sqfs file
  string segment_dir = db_path + tbl_sub_path;
  string sqfs_path = getCompressedFilePath();
  is_compressed = IsExists(sqfs_path);
  bool is_mounted = isMounted(segment_dir);
  if (!(flags & O_CREAT) && lazy_open) {
    if (is_compressed && !is_mounted && IsExists(segment_dir)) {
      Remove(segment_dir);
    }
    return 0;
  }
  if (is_compressed && !mount(sqfs_path, segment_dir, err_info)) {
    LOG_ERROR("%s mount failed", sqfs_path.c_str());
    return err_info.errcode;
  }

  open_(magic(), file_path, db_path, tbl_sub_path, flags, err_info);
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode, tbl_sub_path + file_path);
    LOG_ERROR("%s open_ failed, err_msg: %s", sqfs_path.c_str(), err_info.errmsg.c_str());
    return err_info.errcode;
  }

  // check if compressed failed last time.
  if (!(flags & O_CREAT) && !is_compressed && getSegmentStatus() >= ImmuWithRawSegment) {
    setSegmentStatus(InActiveSegment);
  }

  // if reserve space failed while create segment, we can reserve again here.
  if (meta_data_ != nullptr && _reservedSize() == 0) {
    LOG_DEBUG("reservedSize of table[ %s ] is 0.", file_path.c_str());
    setObjectReady();
    err_info.errcode = reserve(entity_idx_->getReservedRows());
    if (err_info.errcode < 0) {
      err_info.setError(err_info.errcode, tbl_sub_path + file_path);
    }
    LOG_DEBUG("reserved size for table[%s],status: %s", file_path.c_str(), err_info.errmsg.c_str());
  }

  return err_info.errcode;
}

int MMapSegmentTable::init(TsEntityIdxManager* entity_idx, const vector<AttributeInfo>& schema, int encoding,
                           ErrorInfo& err_info) {
  entity_idx_ = entity_idx;
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);
  meta_data_->block_num_of_segment = 0;

  name_ = getURLObjectName(URL());

  for (size_t i = 0 ; i < schema.size() ; ++i) {
    cols_info_with_hidden_.push_back(schema[i]);
  }

  if ((meta_data_->record_size = setAttributeInfo(cols_info_with_hidden_)) < 0) {
    return err_info.errcode;
  }

  off_t col_off = 0;
  col_off = addColumnInfo(cols_info_with_hidden_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  meta_data_->cols_num = cols_info_with_hidden_.size();
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  cols_info_without_hidden_.clear();
  cols_idx_for_hidden_.clear();
  for (int i = 0; i < cols_info_with_hidden_.size(); ++i) {
    if (!cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      cols_info_without_hidden_.emplace_back(cols_info_with_hidden_[i]);
      cols_idx_for_hidden_.emplace_back(i);
    }
  }

  return err_info.errcode;
}

int MMapSegmentTable::reopen(bool lazy_open, ErrorInfo &err_info) {
  if (close(err_info) < 0) {
    LOG_ERROR("MMapSegmentTable reopen failed: MMapSegmentTable::close failed");
    return err_info.errcode;
  }

  return open(entity_idx_, segment_id_, bt_file_.filePath(), db_path_, tbl_sub_path_,
              MMAP_OPEN_NORECURSIVE, lazy_open, err_info);
}

int MMapSegmentTable::close(ErrorInfo& err_info) {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });
  if (getObjectStatus() != OBJ_READY) {
    return err_info.errcode;
  }

  setObjectStatus(OBJ_NOT_READY);

  int flags = bt_file_.flags();

  if (actual_writed_count_.load() != meta_data_->num_node) {
    LOG_WARN("MMapSegmentTable is writing while deleting.");
    meta_data_->num_node = actual_writed_count_.load();
  }

  SegmentStatus s_status = ActiveSegment;
  if (g_max_mount_cnt_ != 0 && g_cur_mount_cnt_ > g_max_mount_cnt_
      && meta_data_ != nullptr) {
    s_status = getSegmentStatus();
  }

  for (auto& col_file : col_files_) {
    delete col_file;
    col_file = nullptr;
  }
  if (m_str_file_) {
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  TsTableObject::close();

  if (s_status >= ImmuWithRawSegment) {
    string segment_dir = db_path_ + tbl_sub_path_;
    if (!isMounted(segment_dir)) {
      Remove(segment_dir);
    }
  }

  if (g_max_mount_cnt_ != 0 && g_cur_mount_cnt_ > g_max_mount_cnt_
      && s_status >= ImmuWithRawSegment) {
    if (!umount(db_path_, tbl_sub_path_, err_info)) {
      LOG_WARN("at MMapSegmentTable::close %s", err_info.errmsg.c_str());
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::setAttributeInfo(vector<AttributeInfo>& info) {
  int offset = 0;

  for (vector<AttributeInfo>::iterator it = info.begin() ; it != info.end() ; ++it) {
    if (it->length <= 0)
      it->length = 1;
    if ((it->size = getDataTypeSize(*it)) == -1)
      return -1;
    if (it->max_len == 0)
      it->max_len = it->size;
    it->offset = offset;
    offset += it->size;
    if (it->type == STRING) {
#if defined(USE_SMART_INDEX)
      if ((encoding & SMART_INDEX) && !actual_dim.empty())
        it->encoding = SMART_INDEX;
      else
#endif
      it->encoding = DICTIONARY;
    }
  }
  return offset;
}

int MMapSegmentTable::reserve(size_t n) {
  int err_code;
  if (meta_data_ == nullptr) {
    err_code = KWENOOBJ;
    return err_code;
  }
  if (n < _reservedSize())
    return 0;

  err_code = reserveBase(n);

  return err_code;
}

int MMapSegmentTable::reserveBase(size_t n) {
  if (meta_data_ == nullptr) {
    return KWENOOBJ;
  }
  if (!meta_data_->has_data) {
    return 0;
  }
  if (n < _reservedSize())
    return 0;
  int err_code;

  if (TSObject::startWrite() < 0) {
    return KWENOOBJ;
  }

  for (size_t i = 0 ; i < cols_info_with_hidden_.size() ; ++i) {
    if (cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    err_code = col_files_[i]->mremap(entity_idx_->getBlockMaxNum() * col_block_size_[i]);
    if (err_code < 0) {
      break;
    }
  }

  if (err_code >= 0) {
    _reservedSize() = n;
  }

  this->TSObject::stopWrite();
  return err_code;
}

int MMapSegmentTable::truncate() {
  if (TSObject::startWrite() < 0) {
    return KWENOOBJ;
  }

  int error_code = 0;
  int block_num = meta_data_->block_num_of_segment;
  for (size_t column = 0 ; column < cols_info_with_hidden_.size() ; ++column) {
    if (col_files_[column] == nullptr) {
      continue;
    }
    // Truncate the unused space in each column of data files
    size_t length = block_num * getDataBlockSize(column);
    error_code = col_files_[column]->resize(length);
    if (error_code < 0) {
      TSObject::stopWrite();
      return error_code;
    }
  }

  TSObject::stopWrite();
  return error_code;
}

int MMapSegmentTable::pushBackToColumn(MetricRowID start_row, size_t payload_col, kwdbts::Payload* payload,
                                       size_t start_in_payload, const BlockSpan& span, kwdbts::DedupInfo& dedup_info) {


  int error_code = 0;
  size_t segment_col = cols_idx_for_hidden_[payload_col];
  MetricRowID block_first_row = span.block_item->getRowID(1);
  if (cols_info_without_hidden_[payload_col].type != DATATYPE::VARSTRING &&
      cols_info_without_hidden_[payload_col].type != DATATYPE::VARBINARY) {
    push_back_payload(payload, start_row, segment_col, payload_col, start_in_payload, span.row_num);
  } else {
    // Variable length is written line by line and can be directly written into the .s file.
    // Before writing, it will check whether the current .s file needs to be resized.
    // The merge mode will determine whether to write the merged data based on the offset of the payload.
    // String file stores the actual value of the string, and stores the offset of the string file
    // in the data column file.
    error_code = push_back_var_payload(payload, start_row, segment_col, payload_col, start_in_payload, span.row_num);
    if (error_code < 0) return error_code;
  }

  // Aggregate result update, the structure includes 4 addresses, namely min, max, sum, count,
  // calculateAggAddr will write the obtained aggregation address to the AggDataAddresses struct.
  AggDataAddresses addresses{};
  calculateAggAddr(start_row.block_id, segment_col, addresses);
  bool is_block_first_row = isBlockFirstRow(start_row) || *reinterpret_cast<uint16_t*>(addresses.count) == 0;
  push_back_null_bitmap(payload, start_row, segment_col, payload_col, start_in_payload, span.row_num);
  void* src = payload->GetColumnAddr(start_in_payload, payload_col);
  if (hasValue(start_row, span.row_num, segment_col)) {
    if (cols_info_without_hidden_[payload_col].type != DATATYPE::VARSTRING &&
        cols_info_without_hidden_[payload_col].type != DATATYPE::VARBINARY) {
      AggCalculator aggCal(src, columnNullBitmapAddr(start_row.block_id, segment_col),
                           start_row.offset_row, DATATYPE(cols_info_without_hidden_[payload_col].type),
                           cols_info_without_hidden_[payload_col].size, span.row_num);
      if (aggCal.CalAllAgg(addresses.min, addresses.max, addresses.sum, addresses.count, is_block_first_row)) {
        span.block_item->is_overflow = true;
      }
    } else {
      std::shared_ptr<void> min_base =
          is_block_first_row ? nullptr : varColumnAggAddr(start_row, segment_col, Sumfunctype::MIN);
      std::shared_ptr<void> max_base =
          is_block_first_row ? nullptr : varColumnAggAddr(start_row, segment_col, Sumfunctype::MAX);
      MetricRowID begin_r = start_row;
      MetricRowID end_r = start_row + span.row_num;
      for (MetricRowID r = begin_r; r < end_r; ++r) {
        if (!isNullValue(r, segment_col)) {
          break;
        }
        ++begin_r;
      }
      for (MetricRowID r = end_r - 1; r > begin_r ; --r) {
        if (!isNullValue(r, segment_col)) {
          break;
        }
        --end_r;
      }
      void* mem = columnAddr(begin_r, segment_col);
      std::shared_ptr<void> var_mem = varColumnAddr(begin_r, end_r - 1, segment_col);
      VarColAggCalculator aggCal(mem, var_mem, columnNullBitmapAddr(start_row.block_id, segment_col),
                                 begin_r.offset_row,
                                 cols_info_without_hidden_[payload_col].size, end_r.offset_row - begin_r.offset_row);
      aggCal.CalAllAgg(addresses.min, addresses.max, min_base, max_base, addresses.count, is_block_first_row);
    }
  }

  // Update the maximum and minimum timestamp information of the current segment,
  // which will be used for subsequent compression and other operations
  if (payload_col == 0) {
    if (KTimestamp(addresses.max) > maxTimestamp() || maxTimestamp() == INVALID_TS) {
      maxTimestamp() = KTimestamp(addresses.max);
    }
    if (KTimestamp(addresses.min) > minTimestamp() || minTimestamp() == INVALID_TS) {
      minTimestamp() = KTimestamp(addresses.min);
    }
  }

  return error_code;
}


int MMapSegmentTable::PushPayload(uint32_t entity_id, MetricRowID start_row, kwdbts::Payload* payload,
                                size_t start_in_payload, const BlockSpan& span, kwdbts::DedupInfo& dedup_info) {
  int error_code = 0;
  if (cols_info_without_hidden_.size() != cols_idx_for_hidden_.size()) {
    LOG_ERROR("Schema mismatch: The number of columns does not match!");
    return -1;
  }
  // During writing, a determination is made as to whether the current payload contains out-of-order data.
  // If present, entity_item is marked as being out of order.
  // During subsequent queries, a judgment will be made. If the result set is unordered, a secondary HASH aggregation based on AGG SCAN is required.
  int payload_col = 1;
  for (size_t i = 1; i < cols_info_without_hidden_.size(); ++i) {
    if (payload_col >= payload->GetColNum()) {
      // The payload data column is fewer than the segment column, allowing for writing (in online scenarios of adding and deleting columns) and logging
      LOG_WARN("payload no column %d" , payload_col);
      continue;
    }
    // Memcpy by column. If the column is of variable length, write it row by row. After completion, update the bitmap and aggregation results.
    error_code = pushBackToColumn(start_row, i, payload, start_in_payload, span, dedup_info);
    if (error_code < 0) {
      return error_code;
    }
    payload_col++;
  }
  // set timestamp at last. timestamp not 0 used for checking data inserted fully.
  error_code = pushBackToColumn(start_row, 0, payload, start_in_payload, span, dedup_info);
  actual_writed_count_.fetch_add(span.row_num);

  return error_code;
}

void MMapSegmentTable::push_back_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                       size_t payload_column, size_t start_row, size_t num) {
  memcpy(columnAddr(row_id, segment_column), payload->GetColumnAddr(start_row, payload_column),
         num * cols_info_with_hidden_[segment_column].size);
  return;
}

int MMapSegmentTable::push_back_var_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                            size_t payload_column, size_t payload_start_row, size_t payload_num) {
  mutexLock();
  int err_code = 0;
  size_t loc = 0;
  uint16_t var_c_len = 0;
  char* var_addr_with_len = nullptr;
  for (int i = 0 ; i < payload_num ; i++) {
    if (!payload->IsNull(payload_column, payload_start_row)) {
      uint64_t var_offset = KUint64(payload->GetColumnAddr(payload_start_row, payload_column));
      if (var_offset >= UINT64_MAX / 2) {  // reuse string in stringfile.
        // In the merge mode, it is necessary to write the variable-length data after merging into the stringfile
        uint64_t var_addr_idx = var_offset - UINT64_MAX / 2;
        if (var_addr_idx >= payload->tmp_var_col_values_4_dedup_merge_.size()) {
          LOG_ERROR("cannot parse location %lu.", var_offset);
          return -1;
        }
        std::shared_ptr<void> var_addr = payload->tmp_var_col_values_4_dedup_merge_.at(var_addr_idx);
        var_addr_with_len = reinterpret_cast<char*>(var_addr.get());
        var_c_len = KUint16(var_addr_with_len);
        if (cols_info_with_hidden_[segment_column].type == DATATYPE::VARSTRING) {
          var_c_len -= 1;
        }
      } else {
        var_c_len = payload->GetVarColumnLen(payload_start_row, payload_column);
        var_addr_with_len = payload->GetVarColumnAddr(payload_start_row, payload_column);
      }
      // insert var string to string_file. and get string file location.
      {
        if (var_c_len + m_str_file_->size() >= m_str_file_->fileLen()) {
          // The default size of the string_file file is 4MB, and it automatically remaps and expands based on usage,
          // doubling within 1GB and expanding in increments of 50% if it exceeds 1GB.
          if (m_str_file_->fileLen() > 1024 * 1024 * 1024) {
            size_t new_size = m_str_file_->size() / 2;
            err_code = m_str_file_->reserve(m_str_file_->size(), 3 * new_size, 2);
          } else {
            err_code = m_str_file_->reserve(m_str_file_->size(), 2 * m_str_file_->fileLen(), 2);
          }
        }
        // When writing data, variable-length data is first written to the string_file file to obtain the offset,
        // and then written to the data block in the data column using memcpy.
        if (cols_info_with_hidden_[segment_column].type == DATATYPE::VARSTRING) {
          loc = m_str_file_->push_back(
              (void*) (var_addr_with_len + MMapStringFile::kStringLenLen), var_c_len);
        } else {
          loc = m_str_file_->push_back_binary(
              (void*) (var_addr_with_len + MMapStringFile::kStringLenLen), var_c_len);
        }

        if (loc == 0 || err_code < 0) {
          LOG_ERROR("StringFile reserve failed.");
          return err_code;
        }
      }
    }
    // set record tuple column addr with string file location.
    memcpy(columnAddr(row_id, segment_column), &loc, cols_info_with_hidden_[segment_column].size);
    row_id.offset_row++;
    payload_start_row++;
  }
  mutexUnlock();
  return err_code;
}

void MMapSegmentTable::push_back_null_bitmap(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                             size_t payload_column, size_t payload_start_row, size_t payload_num) {
  if (payload->NoNullMetric(payload_column)) {
    return;
  }
  for (int i = 0 ; i < payload_num ; i++) {
    // If the payload is empty, set the bitmap
    if (payload->IsNull(payload_column, payload_start_row)) {
      setNullBitmap(row_id, segment_column);
    }
    payload_start_row++;
    row_id.offset_row++;
  }
}

void MMapSegmentTable::sync(int flags) {
  meta_data_->num_node = actual_writed_count_.load();
  bt_file_.sync(flags);
  for (size_t i = 0 ; i < col_files_.size() ; ++i) {
    if (cols_info_with_hidden_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    if (col_files_[i]) {
      col_files_[i]->sync(flags);
    }
  }
  if (m_str_file_ != nullptr) {
    m_str_file_->sync(flags);
  }
}

int MMapSegmentTable::remove() {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });

  ErrorInfo err_info;

  status_ = OBJ_INVALID;
  if (m_str_file_) {
    m_str_file_->remove();
    delete m_str_file_;
    m_str_file_ = nullptr;
  }

  for (auto it = col_files_.begin() ; it != col_files_.end() ; ++it) {
    auto& file = *it;
    if (LIKELY(file != nullptr)) {
      file->remove();
      delete file;
      file = nullptr;
    }
  }

  TsTableObject::close();
  string sqfs_file_path = getCompressedFilePath();
  if (is_compressed) {
    // try umount sqfs file
    umount(db_path_, tbl_sub_path_, err_info);
    // try remove sqfs file
    Remove(sqfs_file_path);
  } else {
    Remove(db_path_ + tbl_sub_path_);
  }
  return err_info.errcode;
}

int MMapSegmentTable::addColumn(AttributeInfo& col_info,
                                ErrorInfo& err_info) {
  return err_info.setError(KWEPERM, "cannot support to add column " + quoteString(name()));
}

uint64_t MMapSegmentTable::dataLength() const {
  return 0;
}

string MMapSegmentTable::URL() const {
  return filePath();
}

const vector<AttributeInfo>& MMapSegmentTable::getSchemaInfo() const {
  return cols_info_with_hidden_;
}

int convertVarToNum(const std::string& str, DATATYPE new_type, char* data, int32_t old_len,
                    ErrorInfo& err_info) {
  std::size_t pos{};
  int res = 0;
  try {
    switch (new_type) {
      case DATATYPE::INT16 : {
        res = std::stoi(str, &pos);
        KInt16(data) = res;
        break;
      }
      case DATATYPE::INT32 : {
        auto res32 = std::stoi(str, &pos);
        KInt32(data) = res32;
        break;
      }
      case DATATYPE::INT64 : {
        auto res64 = std::stoll(str, &pos);
        KInt64(data) = res64;
        break;
      }
      case DATATYPE::FLOAT : {
        auto res_f = std::stof(str, &pos);
        KFloat32(data) = res_f;
        break;
      }
      case DATATYPE::DOUBLE : {
        auto res_d = std::stod(str, &pos);
        KDouble64(data) = res_d;
        break;
      }
      case DATATYPE::CHAR :
      case DATATYPE::BINARY : {
        memcpy(data, str.data(), old_len);
      }
      default:
        break;
    }
  }
  catch (std::invalid_argument const &ex) {
    return err_info.setError(KWEPERM, "Incorrect integer value '" + str + "'");
  }
  catch (std::out_of_range const &ex) {
    return err_info.setError(KWEPERM, "Out of range value '" + str + "'");
  }
  if (pos < str.size()) {
    return err_info.setError(KWEPERM, "Data truncated '" + str + "'");
  }
  if (new_type == DATATYPE::INT16) {
    if (res > INT16_MAX || res < INT16_MIN) {
      return err_info.setError(KWEPERM, "Out of range value '" + str + "'");
    }
  }
  return err_info.errcode;
}

void eraseZeroBeforeE(std::string& str) {
  while (true) {
    size_t e_pos = str.find('e');
    if (e_pos != std::string::npos) {
      if (str[e_pos - 1] == '0') {
        if (!str.empty()) {
          std::string str1 = str.substr(0, e_pos - 1);
          std::string str2 = str.substr(e_pos, str.length() - e_pos);
          str =  str1 + str2;
        }
      }
      else {
        break;
      }
    }
    else {
      break;
    }
  }
}

std::string floatToStr(float value) {
  std::ostringstream oss;
  int decimal_precision = 6;  // decimal part len
  int integer_part = 6;       // integer part len
  oss << std::fixed << std::setprecision(decimal_precision) << value;
  std::string str = oss.str();
  if (str[0] == '-') {
    integer_part = 7;  // '-' will use one len in integer.
  }

  while (true) {
    // delete end '0's if exists.
    if (str[str.length() - 1] == '0') {
      if (!str.empty())
        str.pop_back();
    }
    else {
      break;
    }
  }
  // if no decimal part, delete '.'
  if (str[str.length() - 1] == '.') {
    if (!str.empty())
      str.pop_back();
  }

  size_t dot_pos = str.find('.');
  if (dot_pos == std::string::npos) {
    // if no decimal part, we check integer len
    if (str.length() > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(16) << value;  // tmp use 16 len
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  } else {
    // check integer len
    if (dot_pos > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(16) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  }
  return str;
}


std::string doubleToStr(double value) {
  std::ostringstream oss;
  int decimal_precision = 16;  // decimal part len
  int integer_part = 6;        // integer part len
  int total = 17;              // value total len
  oss << std::fixed << std::setprecision(decimal_precision) << value;
  std::string str = oss.str();
  if (str[0] == '-') {
    integer_part = 7;  // '-' use occupy len of value
  }

  while (true) {
    // delete end '0's if exists.
    if (str[str.length() - 1] == '0') {
      if (!str.empty())
        str.pop_back();
    }
    else {
      break;
    }
  }
  // if no decimal part, delete '.'
  if (str[str.length() - 1] == '.') {
    if (!str.empty())
      str.pop_back();
  }

  size_t dot_pos = str.find('.');
  if (dot_pos == std::string::npos) {
    // if no decimal part, we check integer len
    if (str.length() > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(decimal_precision) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  } else {
    // check integer len
    if (dot_pos > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(decimal_precision) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    } else {
      if (str.length() > total + 1) {
        oss.clear();
        oss.str("");
        oss << std::fixed << std::setprecision(total - dot_pos) << value;
        str = oss.str();
      }
    }
  }

  return str;
}

std::shared_ptr<void> convertFixedToVar(DATATYPE old_type, DATATYPE new_type, char* data, ErrorInfo& err_info) {
  std::string res;
  char* var_data;
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
      res = floatToStr(KFloat32(data));
      break;
    }
    case DATATYPE::DOUBLE : {
      res = doubleToStr(KDouble64(data));
      break;
    }
    case DATATYPE::CHAR:
    case DATATYPE::BINARY: {
      auto char_len = strlen(data);
      if (new_type == DATATYPE::VARSTRING) {
        char_len += 1;
      }
      k_int16 buffer_len = char_len + MMapStringFile::kStringLenLen;
      var_data = static_cast<char*>(std::malloc(buffer_len));
      memset(var_data, 0, buffer_len);
      KInt16(var_data) = static_cast<k_int16>(char_len);
      memcpy(var_data + MMapStringFile::kStringLenLen, data, strlen(data));
      break;
    }
    default:
      err_info.setError(KWEPERM, "Incorrect integer value");
      break;
  }
  if (old_type == DATATYPE::INT16 || old_type == DATATYPE::INT32 || old_type == DATATYPE::INT64 ||
      old_type == DATATYPE::FLOAT || old_type == DATATYPE::DOUBLE) {
    auto act_len = res.size() + 1;
    var_data = static_cast<char*>(std::malloc(act_len + MMapStringFile::kStringLenLen));
    memset(var_data, 0, act_len + MMapStringFile::kStringLenLen);
    KUint16(var_data) = act_len;
    strcpy(var_data + MMapStringFile::kStringLenLen, res.data());
  }
  std::shared_ptr<void> ptr(var_data, free);
  return ptr;
}
