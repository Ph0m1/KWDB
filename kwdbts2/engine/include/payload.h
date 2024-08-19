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

#pragma once
#include <limits>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "kwdb_type.h"
#include "mmap/mmap_entity_idx.h"
#include "TSLockfreeOrderList.h"
#include "ts_common.h"
#include "libkwdbts2.h"
#include "st_wal_types.h"
#include "ts_table_object.h"

namespace kwdbts {

enum class DedupRule {
  KEEP = 0,      // not deduplicate
  OVERRIDE = 1,  // deduplicate by row
  REJECT = 2,    // reject duplicate rows
  DISCARD = 3,   // ignore duplicate rows
  MERGE = 4,     // duplicate by column
};


/**
 * @brief payload support function to read payload memory incoming from go
*/
class Payload {
 public:
  enum DataTagFlag {
    DATA_AND_TAG = 0,
    DATA_ONLY = 1,
    TAG_ONLY = 2,
  };
  /*  header part
  _____________________------_________________________________________________________________________
  |    16    |       2       |         4        |   4  |    8    |       4        |   4    |    1    |
  |----------|---------------|------------------|------|---------|----------------|--------|---------|
  |  txnID   | range groupID |  payloadVersion  | dbID |  tbID   |    TSVersion   | rowNum | rowType |
  */
  const static uint8_t txn_id_offset_ = 0;  // NOLINT
  const static uint8_t txn_id_size_ = 16;  // NOLINT

  const static uint8_t range_group_id_offset_ = 16;  // NOLINT
  const static uint8_t range_group_id_size_ = 2;  // NOLINT

  const static uint8_t payload_version_offset_ = 18;  // NOLINT
  const static uint8_t payload_version_size_ = 4;  // NOLINT

  const static uint8_t db_id_offset_ = 22;  // NOLINT
  const static uint8_t db_id_size_ = 4;  // NOLINT

  const static uint8_t table_id_offset_ = 26;  // NOLINT
  const static uint8_t table_id_size_ = 8;  // NOLINT

  const static uint8_t ts_version_offset_ = 34;  // NOLINT
  const static uint8_t ts_version_size_ = 4;  // NOLINT

  const static uint8_t row_num_offset_ = 38;  // NOLINT
  const static uint8_t row_num_size_ = 4;  // NOLINT

  const static uint8_t row_type_offset_ = 42;  // NOLINT
  const static uint8_t row_type_size_ = 1;  // NOLINT

  const static int header_size_ = row_type_offset_ + row_type_size_;  // NOLINT

  // TODO(jiadx): schema primary\tag\schema
  // data row first column is timestamp type
  Payload(const std::vector<AttributeInfo>& schema, TSSlice data) : schema_(schema), slice_(data) {
    start_row_ = 0;
    count_ = *reinterpret_cast<int32_t*> (slice_.data + row_num_offset_);
    flag_ = *reinterpret_cast<uint8_t*> (slice_.data + row_type_offset_);
    primary_len_ = KInt16(slice_.data + header_size_);
    primary_offset_ = header_size_ + 2;
    tag_len_ = KInt32(slice_.data + primary_offset_ + primary_len_);
    tag_offset_ = primary_offset_ + primary_len_ + 4;
    if (flag_ != Payload::TAG_ONLY) {
      data_len_ = KInt32(slice_.data + tag_offset_ + tag_len_);
      data_offset_ = tag_offset_ + tag_len_ + 4;
    }
    bitmap_len_ = (count_ + 7) / 8;

    col_offsets_ = new int32_t[schema_.size()];
    // calculate column offset
    int32_t col_len = data_offset_;
    for (int i = 0; i < schema_.size(); i++) {
      col_offsets_[i] = col_len;
      col_len += (bitmap_len_ + schema_[i].size * count_);
    }
  }
  ~Payload() {
    if (rec_helper_) delete rec_helper_;
    delete []col_offsets_;
  }

  // payload version
  uint32_t GetPayloadVersion() {
    return *reinterpret_cast<uint32_t*> (slice_.data + payload_version_offset_);
  }

  int64_t GetTableId() {
    return *reinterpret_cast<int64_t*> (slice_.data + table_id_offset_);
  }

  // table version
  uint32_t GetTsVersion() {
    return *reinterpret_cast<uint32_t*> (slice_.data + ts_version_offset_);
  }

  int32_t GetRowCount() {
    return count_;
  }

  int32_t GetStartRowId() {
    return start_row_;
  }

  int32_t GetDataLength() {
    return data_len_;
  }

  // primary tag value, multi-tags can be primary tag
  TSSlice GetPrimaryTag() {
    return TSSlice{slice_.data + primary_offset_, static_cast<size_t>(primary_len_)};
  }

  bool HasTagValue() {
    return *reinterpret_cast<bool*> (slice_.data + header_size_ - 1);
  }

  bool IsTagNull(int i) {
    return false;
  }

  TSSlice GetTagValue(int i) {
    return TSSlice();
  }

  int32_t GetColOffset(int col) {
    return col_offsets_[col] + bitmap_len_;
  }

  int32_t GetColNum() {
    return schema_.size();
  }

  int32_t GetNullBitMapOffset(int col) {
    return col_offsets_[col];
  }

  char* GetColumnAddr(int row, int col) {
    int col_len = schema_[col].size;
    return reinterpret_cast<char*> (slice_.data + GetColOffset(col) + col_len * row);
  }

  char* GetNullBitMapAddr(int col) {
    return reinterpret_cast<char*> (slice_.data + GetNullBitMapOffset(col));
  }

  bool IsNull(int col, int row) {
    char* null_bitmap = GetNullBitMapAddr(col);
    k_uint32 index = row >> 3;
    unsigned char bit_pos = (1 << (row & 7));
    return ((null_bitmap[index]) & bit_pos);
  }

  // check if certain column values all not null
  bool NoNullMetric(int col) {
    size_t null_size = (count_ + 7) / 8;

    for (int i = 0; i < null_size; i++) {
      if ((*(reinterpret_cast<char*>((intptr_t) GetNullBitMapAddr(col)) + i) | 0) != 0) {
        return false;
      }
    }
    return true;
  }

  // check if certain column value is null
  bool IsNullMetric(int col, int row) {
    size_t null_offset = (row + 7) / 8;
    int null_bit = *reinterpret_cast<int*>((intptr_t) GetNullBitMapAddr(col) + null_offset);
    return (null_bit & (1 << (8 - (row) % 8))) == 0;
  }

  // vartype column value
  char* GetVarColumnAddr(int row, int col) {
    uint64_t val_off = *reinterpret_cast<uint64_t *>(GetColumnAddr(row, col));
    return reinterpret_cast<char*>(GetNullBitMapAddr(col) + val_off);
  }

  uint16_t GetVarColumnLen(int row, int col) {
    if (IsNull(col, row)) return 0;
    uint64_t val_off = *reinterpret_cast<uint64_t *>(GetColumnAddr(row, col));
    return *reinterpret_cast<uint16_t *>(GetNullBitMapAddr(col) + val_off);
  }

  TSSlice GetColumnValue(int row, int col) {
    if (isBinaryType(schema_[col].type)) {
      // vartype column
      int32_t val_off = KInt32(GetColumnAddr(row, col));
      int32_t val_len = KInt32(slice_.data + val_off);
      return TSSlice{reinterpret_cast<char*> (slice_.data + val_off + 4), static_cast<size_t> (val_len)};
    } else {
      // fixed-len type column
      return TSSlice{GetColumnAddr(row, col), static_cast<size_t> (schema_[col].size)};
    }
  }

  // update payload rows lsn
  void SetLsn(TS_LSN lsn) {
    if (schema_[0].type != DATATYPE::TIMESTAMP64_LSN) return;

    for (int i = 0; i < GetRowCount(); i++) {
      TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(GetColumnAddr(i, 0));
      ts_lsn->lsn = lsn;
    }
  }

  // get lsn from first row.
  bool GetLsn(TS_LSN& lsn) {
    if (schema_[0].type != DATATYPE::TIMESTAMP64_LSN) return false;
    if (GetRowCount() <= 0) return false;
    TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(GetColumnAddr(0, 0));
    lsn = ts_lsn->lsn;
    return true;
  }

  void SetMergeValue(int col, MetricRowID row_id, const std::shared_ptr<void>& data) {
    if (col < var_merge_values_.size()) {
      var_merge_values_[col].insert({row_id, data});
    } else {
      std::map<MetricRowID, std::shared_ptr<void>> var_m;
      var_merge_values_.emplace_back(var_m);
      var_merge_values_[col].insert({row_id, data});
    }
  }

  std::shared_ptr<void> GetMergeValue(int col, MetricRowID row_id) {
    if (col >= var_merge_values_.size()) {
      return nullptr;
    }
    auto iter = var_merge_values_[col].find(row_id);
    if (iter != var_merge_values_[col].end()) {
      return iter->second;
    }
    return nullptr;
  }

  // Get the timestamp of the ith row
  KTimestamp GetTimestamp(int row) {
    return KTimestamp(GetColumnAddr(row, 0));
  }

  bool IsDisordered(int start_row, int count) {
    timestamp64 prev_ts = GetTimestamp(start_row);
    for (int i = 1; i < count; ++i) {
      timestamp64 cur_ts = GetTimestamp(start_row + i);
      if (cur_ts < prev_ts) {
        return true;
      }
      prev_ts = cur_ts;
    }
    return false;
  }

  inline const int32_t GetTagLen() { return tag_len_; }

  inline char* GetTagAddr() {
    return reinterpret_cast<char*> (slice_.data + tag_offset_);
  }

  inline int32_t GetTagOffset() {
    return tag_offset_;
  }

  inline char* GetPrimaryTagAddr() {
    return reinterpret_cast<char*> (slice_.data + primary_offset_);
  }

  inline uint8_t GetFlag() { return flag_; }

  ostream& PrintMetric(std::ostream& os) {
    if (!rec_helper_) {
      rec_helper_ = new RecordHelper();
      rec_helper_->setHelper(schema_, false);
    }

    for (int row = 0; row < GetRowCount(); row++) {
      for (int col = 0; col < schema_.size(); col++) {
        if (IsNull(col, row)) {
          os << s_NULL << ' ';
        } else if (schema_[col].type == VARSTRING) {
          os << std::string(GetVarColumnAddr(row, col) + MMapStringFile::kStringLenLen, GetVarColumnLen(row, col)) << ' ';
        } else {
          os << rec_helper_->columnToString(col, GetColumnAddr(row, col)) << ' ';
        }
      }
      os << "\n";
    }
    os << "\n";
    return os;
  }

 private:
  const vector<AttributeInfo>& schema_;
  TSSlice slice_;
  int32_t primary_offset_;
  int16_t primary_len_;
  int32_t tag_offset_;
  int32_t data_offset_;
  int32_t tag_len_;
  uint8_t flag_;  // 0: data+tag_val 1: primary_tag + data 2: primary_tag + tag
  // data offset
  int32_t data_len_;
  int32_t bitmap_len_;

  int32_t start_row_;
  int32_t count_;

  int32_t* col_offsets_;
  RecordHelper* rec_helper_{nullptr};
  std::vector<std::map<MetricRowID, std::shared_ptr<void>>> var_merge_values_;

 public:
  DedupRule dedup_rule_ = DedupRule::OVERRIDE;
  // while data deduplicate rule is merge, need change payload column value.
  std::vector<std::shared_ptr<void>> tmp_var_col_values_4_dedup_merge_;
};

struct DedupInfo {
  timestamp64 payload_max_ts = INT64_MIN;
  timestamp64 payload_min_ts = INT64_MAX;
  std::vector<timestamp64> check_ts;
  std::vector<MetricRowID> check_r;
  std::vector<MetricRowID> payload_r;
  bool need_dup = false;
  bool need_scan_table = false;
  std::unordered_map<timestamp64, std::vector<MetricRowID>> table_real_rows;  // duplicate row find from table entity.
  std::unordered_map<timestamp64, std::vector<size_t>> payload_rows;  // duplicate rows in payload.
};

}  //  namespace kwdbts
