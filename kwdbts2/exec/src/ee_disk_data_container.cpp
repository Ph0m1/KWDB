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


#include "ee_disk_data_container.h"

#include <algorithm>

#include "ee_common.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

constexpr k_int64 OptimalDiskDataSize = 32768;  // 32k

KStatus DiskDataContainer::GenAttributeInfo(AttributeInfo* col_var,
                                            size_t chunk_size) {
  col_var->type = DATATYPE::BINARY;
  col_var->name[0] = '\0';
  col_var->offset = 0;
  col_var->size = chunk_size + 2 + STRING_WIDE;  // 2 bytes for row count in the chunk
  col_var->length = chunk_size + 2 + STRING_WIDE;
  col_var->max_len = chunk_size + 2 + STRING_WIDE;
  // col_var->attr_type = ATTR_TS_DATA;

  return KStatus::SUCCESS;
}

KStatus DiskDataContainer::Init() {
  std::vector<AttributeInfo> bt_schema;
  ErrorInfo err_info;
  AttributeInfo attr;
  GenAttributeInfo(&attr, ComputeBlockSize());
  bt_schema.push_back(attr);

  // create tmp table
  k_int32 encoding = ROW_TABLE | NULLBITMAP_TABLE;
  temp_table_ = CreateTempTable(bt_schema, ExecPool::GetInstance().db_path_,
                                encoding, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("create temp table error : %d, %s",
              err_info.errcode, err_info.errmsg.c_str());
    return FAIL;
  }
  cumulative_count_.clear();
  cumulative_count_.emplace_back(0);

  return SUCCESS;
}

void DiskDataContainer::Reset() {
  read_cache_chunk_index_ = 0;
  if (read_cache_chunk_ptr_ != nullptr) {
    read_cache_chunk_ptr_->~DataChunk();
  }
  read_cache_chunk_ptr_ = nullptr;

  if (write_cache_chunk_ptr_ != nullptr) {
    write_cache_chunk_ptr_->~DataChunk();
  }
  write_cache_chunk_ptr_ = nullptr;
  write_data_ptr_ = nullptr;

  if (temp_table_) {
    ErrorInfo err_info;
    DropTempTable(temp_table_, err_info);
    temp_table_ = nullptr;
  }
}

KStatus DiskDataContainer::Append(DataChunk* chunk) {
  k_uint32 begin_row = 0;
  k_uint32 end_row = chunk->Count();
  k_uint32 residual_rows = end_row - begin_row;
  while (residual_rows > 0) {
    bool isUpdated = false;
    if (UpdateWriteCacheChunk(isUpdated) != SUCCESS) {
      return FAIL;
    }

    k_uint32 free_rows = write_cache_chunk_ptr_->Capacity() - write_cache_chunk_ptr_->Count();
    k_uint32 append_rows = (free_rows > residual_rows) ? residual_rows : free_rows;
    KStatus ret = write_cache_chunk_ptr_->Append(chunk, begin_row, begin_row + append_rows);
    if (ret != SUCCESS) {
      return ret;
    }
    *(reinterpret_cast<k_uint16*>(write_data_ptr_)) += append_rows;
    count_ += append_rows;

    if (isUpdated) {
      cumulative_count_.emplace_back(cumulative_count_.back() + append_rows);
    } else {
      cumulative_count_.back() += append_rows;
    }

    k_uint32 chunk_num = cumulative_count_.size();
    if (chunk_num > 3) {
      if ((cumulative_count_[chunk_num - 2] - cumulative_count_[chunk_num - 3]) !=
          (cumulative_count_[chunk_num - 3] - cumulative_count_[chunk_num - 4])) {
        is_same_count_ = false;
      }
    }

    begin_row += append_rows;
    residual_rows = end_row - begin_row;
  }

  return SUCCESS;
}

KStatus DiskDataContainer::Append(std::queue<DataChunkPtr>& buffer) {
  KStatus ret = SUCCESS;
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    ret = Append(buf.get());
    if (ret != SUCCESS) {
      return ret;
    }
    buffer.pop();
  }
  return ret;
}

k_int32 DiskDataContainer::NextLine() {
  if (current_line_ + 1 >= count_) {
    return -1;
  }

  ++current_line_;
  if (!selection_.empty()) {
    return selection_[current_line_];  // for sort
  } else {
    return current_line_;
  }
}

bool DiskDataContainer::IsNull(k_uint32 row, k_uint32 col) {
  k_uint32 row_in_chunk = UpdateReadCacheChunk(row);
  return read_cache_chunk_ptr_->IsNull(row_in_chunk, col);
}

bool DiskDataContainer::IsNull(k_uint32 col) {
  k_int32 reqRow = selection_.empty() ? current_line_ : selection_[current_line_];
  k_uint32 row_in_chunk = UpdateReadCacheChunk(reqRow);
  return read_cache_chunk_ptr_->IsNull(row_in_chunk, col);
}

DatumPtr DiskDataContainer::GetData(k_uint32 row, k_uint32 col) {
  k_uint32 row_in_chunk = UpdateReadCacheChunk(row);
  return read_cache_chunk_ptr_->GetData(row_in_chunk, col);
}

DatumPtr DiskDataContainer::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  k_uint32 row_in_chunk = UpdateReadCacheChunk(row);
  return read_cache_chunk_ptr_->GetData(row_in_chunk, col, len);
}

DatumPtr DiskDataContainer::GetData(k_uint32 col) {
  k_int32 reqRow = selection_.empty() ? current_line_ : selection_[current_line_];
  k_uint32 row_in_chunk = UpdateReadCacheChunk(reqRow);
  return read_cache_chunk_ptr_->GetData(row_in_chunk, col);
}

k_uint32 DiskDataContainer::ComputeCapacity() {
  // Cautiousness: OptimalDiskDataSize * 8 < 7 * col_num
  // (capacity_ + 7)/8 * col_num + capacity_ * row_size_ <= OptimalDiskDataSize
  int capacity =
    (OptimalDiskDataSize * 8 - 7 * col_num_) / (col_num_ + 8 * static_cast<int>(row_size_));
  if (capacity < 2) {
    return 1;
  } else {
    return static_cast<k_uint32>(capacity);
  }
}

size_t DiskDataContainer::ComputeBlockSize() {
  return (capacity_ + 7) / 8 * col_num_ + capacity_ * row_size_;
}

k_uint32 DiskDataContainer::UpdateReadCacheChunk(k_uint32 row) {
  if (read_cache_chunk_index_ > 0 &&
      row >= cumulative_count_[read_cache_chunk_index_ - 1] &&
      row < cumulative_count_[read_cache_chunk_index_]) {
    return row - cumulative_count_[read_cache_chunk_index_ - 1];
  }

  k_uint32 chunk_index = 0;  // corresponding to cumulative_count_
  if (is_same_count_) {
    chunk_index = row / cumulative_count_[1] + 1;
  } else {
    auto begin = cumulative_count_.begin();
    auto end = cumulative_count_.end();
    auto it = std::lower_bound(begin, end, row);
    chunk_index = it - begin;
  }

  char* ptr = static_cast<char*>(temp_table_->getColumnAddr(chunk_index - 1, 0));
  read_cache_chunk_index_ = chunk_index;

  k_uint32 chunk_count = *reinterpret_cast<k_uint16*>(ptr);
  if (read_cache_chunk_ptr_ != nullptr) {
    read_cache_chunk_ptr_->ResetDataPtr(ptr + 2, chunk_count);
  } else {
    read_cache_chunk_ptr_ =
      new(read_cache_chunk_obj_)DataChunk(col_info_, col_num_, ptr + 2, chunk_count, capacity_);
    read_cache_chunk_ptr_->Initialize();
  }

  return row - cumulative_count_[read_cache_chunk_index_ - 1];
}

KStatus DiskDataContainer::UpdateWriteCacheChunk(bool& isUpdated) {
  if ((write_cache_chunk_ptr_ == nullptr) || write_cache_chunk_ptr_->isFull()) {
    k_uint32 curr_size = temp_table_->size();
    k_uint32 want_size = curr_size + 1;  // expected size
    ErrorInfo err_info;
    err_info.errcode = temp_table_->reserveBase(want_size + 1);
    if (err_info.errcode < 0) {
      LOG_ERROR("append temp table error : %d, %s", err_info.errcode, err_info.errmsg.c_str());
      return FAIL;
    }

    write_data_ptr_ = static_cast<char*>(temp_table_->getColumnAddr(curr_size, 0));
    temp_table_->resize(want_size);
    *(reinterpret_cast<k_uint16*>(write_data_ptr_)) = 0;
    if (write_cache_chunk_ptr_ != nullptr) {
      write_cache_chunk_ptr_->ResetDataPtr(write_data_ptr_ + 2, 0);
    } else {
      write_cache_chunk_ptr_ = new (write_cache_chunk_obj_)
          DataChunk(col_info_, col_num_, write_data_ptr_ + 2, 0, capacity_);
      write_cache_chunk_ptr_->Initialize();
    }
    isUpdated = true;
  } else {
    isUpdated = false;
  }

  return SUCCESS;
}

void DiskDataContainer::Sort() {
  // selection_ init
  selection_.resize(count_);
  for (int i = 0; i < count_; i++) {
    selection_[i] = i;
  }

  // iterator
  auto it_begin = selection_.begin();
  auto it_end = selection_.end();

  // sort
  OrderColumnCompare cmp(this, order_info_);
  std::stable_sort(it_begin, it_end, cmp);
}

}   // namespace kwdbts
