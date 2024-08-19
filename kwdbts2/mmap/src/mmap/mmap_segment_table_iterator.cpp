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

#include "mmap/mmap_segment_table_iterator.h"

int convertFixedToFixed(DATATYPE old_type, DATATYPE new_type, char* src, char* dst, ErrorInfo& err_info) {
  switch (old_type) {
    case DATATYPE::INT16 : {
      switch (new_type) {
        case DATATYPE::INT32:
          return convertNumToNum<int16_t, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<int16_t, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int16_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int16_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::INT32 : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<int32_t, int16_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<int32_t, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int32_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int32_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::INT64 : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<int64_t, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<int64_t, int32_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int64_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int64_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::FLOAT : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<float, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<float, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<float, int64_t>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<float, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::DOUBLE : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<double, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<double, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<double, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<double, float>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      std::size_t pos{};
      int res = 0;
      switch (new_type) {
        case DATATYPE::INT16 : {
          res = std::stoi(std::string(src), &pos);
          KInt16(dst) = static_cast<int16_t>(res);
          break;
        }
        case DATATYPE::INT32 : {
          res = std::stoi(std::string(src), &pos);
          KInt32(dst) = res;
          break;
        }
        case DATATYPE::INT64: {
          auto res_ll = std::stoll(std::string(src), &pos);
          KInt64(dst) = res_ll;
          break;
        }
        case DATATYPE::FLOAT : {
          auto res_f = std::stof(std::string(src), &pos);
          KFloat32(dst) = res_f;
          break;
        }
        case DATATYPE::DOUBLE : {
          auto res_f = std::stod(std::string(src), &pos);
          KDouble64(dst) = res_f;
          break;
        }
        default:
          break;
      }
      break;
    }

    default:
      break;
  }
  return 0;
}

int convertFixedToStr(DATATYPE old_type, char* old_data, char* new_data, ErrorInfo& err_info) {
  std::string res;
  switch (old_type) {
    case DATATYPE::INT16 : {
      res = std::to_string(KInt16(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::FLOAT : {
      std::ostringstream oss;
      oss.clear();
      oss.precision(7);
      oss.setf(std::ios::fixed);
      oss << KFloat32(old_data);
      res = oss.str();
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::DOUBLE : {
      std::stringstream ss;
      ss.precision(15);
      ss.setf(std::ios::fixed);
      ss << KDouble64(old_data);
      res = ss.str();
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      strcpy(new_data, old_data);
      break;
    }
    default:
      err_info.setError(KWEPERM, "Fixed type invalid");
      break;
  }
  return 0;
}

KStatus ConvertToFixedLen(std::shared_ptr<MMapSegmentTable> segment_tbl, char* value, BlockItem* cur_block_item,
                          DATATYPE old_type, DATATYPE new_type, int32_t new_len,
                          size_t start_row, k_uint32 count, k_int32 ts_col, void* bitmap) {
  ErrorInfo err_info;
  if (old_type != VARSTRING && old_type != VARBINARY) {
    // fixed-len column type to fixed-len column type
    for (k_uint32 i = 0; i < count; ++i) {
      void* old_mem = segment_tbl->columnAddrByBlk(cur_block_item->block_id, start_row + i - 1, ts_col);
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, (char*) old_mem, value + i * new_len, err_info);
      } else {
        err_info.errcode = convertFixedToFixed(old_type, new_type, (char*) old_mem, value + i * new_len, err_info);
      }
      if (err_info.errcode < 0) {
        return KStatus::FAIL;
      }
    }
  } else {
    // vartype cloumn to fixed-len column type
    auto var_b = new VarColumnBatch(count, bitmap, start_row, segment_tbl);
    for (k_uint32 i = 0; i < count; ++i) {
      bool is_null;
      if (var_b->isNull(i, &is_null) != KStatus::SUCCESS) {
        delete var_b;
        var_b = nullptr;
        return KStatus::FAIL;
      }
      if (is_null) {
        continue;
      }
      std::shared_ptr<void> old_mem = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, start_row + i - 1, ts_col);
      std::string v_value((char*) old_mem.get() + MMapStringFile::kStringLenLen);
      convertVarToNum(v_value, new_type, value + i * new_len, KUint16(old_mem.get()), err_info);
    }
    delete var_b;
  }
  return KStatus::SUCCESS;
}

std::shared_ptr<void> ConvertVarLen(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* cur_block_item,
                                    DATATYPE old_type, DATATYPE new_type, size_t row_idx, k_int32 ts_col) {
  ErrorInfo err_info;
  std::shared_ptr<void> data = nullptr;
  if (old_type != VARSTRING && old_type != VARBINARY) {
    void* old_mem = segment_tbl->columnAddrByBlk(cur_block_item->block_id, row_idx, ts_col);
    data = convertFixedToVar(old_type, new_type, (char*) old_mem,err_info);
  } else {
    if (old_type == VARSTRING) {
      auto old_data = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, row_idx, ts_col);
      auto old_len = KUint16(old_data.get()) - 1;
      char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringFile::kStringLenLen));
      memset(var_data, 0, old_len + MMapStringFile::kStringLenLen);
      KUint16(var_data) = old_len;
      memcpy(var_data + MMapStringFile::kStringLenLen,
             (char*) old_data.get() + MMapStringFile::kStringLenLen, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    } else {
      auto old_data = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, row_idx, ts_col);
      auto old_len = KUint16(old_data.get());
      char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringFile::kStringLenLen + 1));
      memset(var_data, 0, old_len + MMapStringFile::kStringLenLen + 1);
      KUint16(var_data) = old_len + 1;
      memcpy(var_data + MMapStringFile::kStringLenLen,
             (char*) old_data.get() + MMapStringFile::kStringLenLen, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    }
  }
  return data;
}

KStatus MMapSegmentTableIterator::GetBatch(BlockItem* cur_block_item, size_t block_start_idx,
                                         ResultSet* res, k_uint32 count) {
  // add all column data to res
  auto schema_info = segment_table_->getSchemaInfo();
  ErrorInfo err_info;
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 ts_col = -1;
    if (i < ts_scan_cols_.size()) {
      ts_col = ts_scan_cols_[i];
    }
    Batch* b;
    if (ts_col >= 0 && segment_table_->isColExist(ts_col)) {
      void* bitmap_addr = segment_table_->getBlockHeader(cur_block_item->block_id, ts_col);
      if (attrs_[ts_col].type != VARSTRING && attrs_[ts_col].type != VARBINARY) {
        if (schema_info[ts_col].type != attrs_[ts_col].type) {
          // other column type to fixed-len column type
          char* value = static_cast<char*>(malloc(attrs_[ts_col].size * count));
          memset(value, 0, attrs_[ts_col].size * count);
          KStatus s = ConvertToFixedLen(segment_table_, value, cur_block_item, static_cast<DATATYPE>(schema_info[ts_col].type),
                                        static_cast<DATATYPE>(attrs_[ts_col].type), attrs_[ts_col].size,
                                        block_start_idx, count, ts_col, bitmap_addr);
          if (s != KStatus::SUCCESS) {
            free(value);
            return s;
          }
          b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_table_);
          b->is_new = true;
        } else {
          if (schema_info[ts_col].size != attrs_[ts_col].size) {
            // fixed-len column type with diff len
            char* value = static_cast<char*>(malloc(attrs_[ts_col].size * count));
            memset(value, 0, attrs_[ts_col].size * count);
            for (int idx = block_start_idx - 1; idx < count; idx++) {
              memcpy(value + idx * attrs_[ts_col].size,
                     segment_table_->columnAddrByBlk(cur_block_item->block_id, idx, ts_col),
                     schema_info[ts_col].size);
            }
            b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_table_);
            b->is_new = true;
          } else {
            b = new Batch(segment_table_->columnAddrByBlk(cur_block_item->block_id, block_start_idx - 1, ts_col),
                          count, bitmap_addr, block_start_idx, segment_table_);
          }
        }
      } else {
        b = new VarColumnBatch(count, bitmap_addr, block_start_idx, segment_table_);
        for (k_uint32 j = 0; j < count; ++j) {
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
            if (schema_info[ts_col].type != attrs_[ts_col].type) {
              // to vartype column
              data = ConvertVarLen(segment_table_, cur_block_item, static_cast<DATATYPE>(schema_info[ts_col].type),
                                   static_cast<DATATYPE>(attrs_[ts_col].type), block_start_idx + j - 1, ts_col);
            } else {
              data = segment_table_->varColumnAddrByBlk(cur_block_item->block_id, block_start_idx + j - 1, ts_col);
            }
          }
          b->push_back(data);
        }
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      b = new Batch(bitmap, count, bitmap, block_start_idx, segment_table_);
    }
    res->push_back(i, b);
  }
  return KStatus::SUCCESS;
}

KStatus MMapSegmentTableIterator::Next(BlockItem* cur_block_item, k_uint32* start_offset,
                                     ResultSet* res, k_uint32* count) {
  *count = 0;
  res->clear();
  bool has_first = false;
  size_t block_start_idx = 0;
  // scan all data in blockitem.
  while (*start_offset <= cur_block_item->publish_row_count) {
    bool is_deleted;
    if (cur_block_item->isDeleted(*start_offset, &is_deleted) < 0) {
      return KStatus::FAIL;
    }
    // if cur_blockdata_offset_  row not valid or not in ts_span or not in lsn, we should check next row continue
    timestamp64 cur_ts = KTimestamp(segment_table_->columnAddrByBlk(cur_block_item->block_id, *start_offset - 1, 0));
    if (is_deleted || !CheckIfTsInSpan(cur_ts)) {
      ++(*start_offset);
      if (has_first) {
        break;
      }
      continue;
    }
    if (!has_first) {
      has_first = true;
      block_start_idx = *start_offset;
    }
    ++(*count);
    ++(*start_offset);
  }

  if (has_first) {
    GetBatch(cur_block_item, block_start_idx, res, *count);
  }
  return SUCCESS;
}
