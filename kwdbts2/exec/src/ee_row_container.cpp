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

#include "ee_row_container.h"

#include <queue>
#include <chrono>
#include <cmath>

#include "ee_exec_pool.h"
#include "ee_aggregate_func.h"
#include "lg_api.h"
#include "big_table.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

// a, b represents the indexes of two rows of data in the container,
// respectively
bool OrderColumnCompare::operator()(k_uint32 a, k_uint32 b) {
  // ColunnInfo
  std::vector<ColumnInfo>& col_info = container_->GetColumnInfo();

  // compare
  for (int i = 0; i < order_info_.size(); i++) {
    int col_idx = order_info_[i].col_idx;

    // dispose Null
    bool is_a_null = container_->IsNull(a, col_idx);
    bool is_b_null = container_->IsNull(b, col_idx);

    // a,b is null
    if (is_a_null && is_b_null) {
      continue;
    }

    if (is_a_null && !is_b_null) {
      // a is null，b is not null
      return order_info_[i].direction == TSOrdering_Column::ASC ? true : false;
    } else if (!is_a_null && is_b_null) {
      // a is not null，b is null
      return order_info_[i].direction == TSOrdering_Column::ASC ? false : true;
    }

    // compare not null
    switch (col_info[col_idx].storage_type) {
      case roachpb::DataType::BOOL: {
        auto* a_data = reinterpret_cast<k_bool*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_bool*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::SMALLINT: {
        auto* a_data = reinterpret_cast<k_int16*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int16*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::INT: {
        auto* a_data = reinterpret_cast<k_int32*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int32*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        auto* a_data = reinterpret_cast<k_int64*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int64*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::FLOAT: {
        auto* a_data = reinterpret_cast<k_float32*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_float32*>(container_->GetData(b, col_idx));
        // float
        if (fabs(*a_data - *b_data) < std::numeric_limits<float>::epsilon()) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::DOUBLE: {
        auto* a_data = reinterpret_cast<k_double64*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_double64*>(container_->GetData(b, col_idx));
        // double
        if (fabs(*a_data - *b_data) < std::numeric_limits<double>::epsilon()) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::VARBINARY: {
        auto* a_data = reinterpret_cast<char*>(container_->GetData(a, col_idx));
        k_uint16 a_len;
        std::memcpy(&a_len, a_data, sizeof(k_uint16));
        std::string a_str = std::string{a_data + sizeof(k_uint16), a_len};

        auto* b_data = reinterpret_cast<char*>(container_->GetData(b, col_idx));
        k_uint16 b_len;
        std::memcpy(&b_len, b_data, sizeof(k_uint16));
        std::string b_str = std::string{b_data + sizeof(k_uint16), b_len};

        if (a_str.compare(b_str) == 0) {
          continue;
        }

        return order_info_[i].direction == TSOrdering_Column::ASC ?
               a_str.compare(b_str) < 0 : a_str.compare(b_str) > 0;
      }
      case roachpb::DataType::DECIMAL: {
        DatumPtr a_data = container_->GetData(a, col_idx);
        DatumPtr b_data = container_->GetData(b, col_idx);

        int cmp_result = AggregateFunc::CompareDecimal(a_data, b_data);
        if (cmp_result == 0) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
                cmp_result < 0 : cmp_result > 0;
      }
      default:
        return true;
    }
  }
  return false;
}

//////////////// MemRowContainer //////////////////////

KStatus MemRowContainer::Init() {
  if (Initialize() < 0) {
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus MemRowContainer::Append(std::queue<DataChunkPtr>& buffer) {
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    size_t batch_buf_length = buf->RowSize() * buf->Count();

    size_t offset = count_ * RowSize();
    memcpy(data_ + offset, buf->GetData(), batch_buf_length);
    count_ += buf->Count();
    buffer.pop();
  }
  return SUCCESS;
}

KStatus MemRowContainer::Append(DataChunkPtr& chunk) {
  for (int r = 0; r < chunk->Count(); r++) {
    this->CopyRow(count_, chunk->GetRow(r));
    this->AddCount();
  }
  return SUCCESS;
}

void MemRowContainer::Sort() {
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

k_int32 MemRowContainer::NextLine() {
  // current_sel_idx_ at the end of selection
  if (!selection_.empty()) {
    if (current_sel_idx_ + 1 >= selection_.size()) {
      return -1;
    }
    ++current_sel_idx_;
    return selection_[current_sel_idx_];
  }
  return -1;
}

k_uint32 MemRowContainer::Count() {
  return selection_.size();
}

DatumPtr MemRowContainer::GetData(k_uint32 col) {
  return DataChunk::GetData(selection_[current_sel_idx_], col);
}

bool MemRowContainer::IsNull(k_uint32 col) {
  return DataChunk::IsNull(selection_[current_sel_idx_], col);
}

////////////// DiskRowContainer //////////////////////
k_int32 DiskRowContainer::NextLine() {
  // current_sel_idx_ at the end of selection
  if (!selection_.empty()) {
    if (current_sel_idx_ + 1 >= selection_.size()) {
      return -1;
    }
    ++current_sel_idx_;
    return selection_[current_sel_idx_];
  }
  return -1;
}
k_uint32 DiskRowContainer::Count() {
  return selection_.size();
}
DatumRowPtr DiskRowContainer::GetRow(k_uint32 row) {
  return static_cast<DatumPtr>(temp_table_->getColumnAddr(row, 0));
}
void DiskRowContainer::Reset() {
  if (temp_table_) {
    ErrorInfo err_info;
    DropTempTable(temp_table_, err_info);
    temp_table_ = nullptr;
  }
}
DatumPtr DiskRowContainer::GetData(k_uint32 row, k_uint32 col) {
  return static_cast<DatumPtr>(temp_table_->getColumnAddr(row, col));
}

bool DiskRowContainer::IsNull(k_uint32 row, k_uint32 col) {
  return temp_table_->isNull(row, col);
}

DatumPtr DiskRowContainer::GetData(k_uint32 col) {
  return static_cast<DatumPtr>(temp_table_->getColumnAddr(selection_[current_sel_idx_], col));
}

bool DiskRowContainer::IsNull(k_uint32 col) {
  return temp_table_->isNull(selection_[current_sel_idx_], col);
}

KStatus DiskRowContainer::GenAttributeInfo(const ColumnInfo &col_info,
                                   AttributeInfo* col_var) {
  col_var->size = col_info.storage_len;
  switch (col_info.storage_type) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      col_var->type = DATATYPE::TIMESTAMP64;
      col_var->max_len = 3;
      break;
    case roachpb::SMALLINT:
      col_var->type = DATATYPE::INT16;
      break;
    case roachpb::INT:
      col_var->type = DATATYPE::INT32;
      break;
    case roachpb::BIGINT:
      col_var->type = DATATYPE::INT64;
      break;
    case roachpb::FLOAT:
      col_var->type = DATATYPE::FLOAT;
      break;
    case roachpb::DOUBLE:
      col_var->type = DATATYPE::DOUBLE;
      break;
    case roachpb::BOOL:
      col_var->type = DATATYPE::BYTE;
      break;
    case roachpb::DECIMAL:
      col_var->type = DATATYPE::BINARY;
      col_var->size = col_var->size + BOOL_WIDE;
      break;
    case roachpb::CHAR:
    case roachpb::VARCHAR: {
      col_var->type = DATATYPE::CHAR;
      col_var->size = col_var->size + STRING_WIDE;
      break;
    }
    case roachpb::BINARY:
    case roachpb::NCHAR:
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      col_var->type = DATATYPE::BINARY;
      col_var->size = col_var->size + STRING_WIDE;
      break;
    case roachpb::SDECHAR:
    case roachpb::SDEVARCHAR:
      col_var->type = DATATYPE::STRING;
      break;
    case roachpb::UNKNOWN:
    default: return KStatus::FAIL;  // throw err
      break;
  }

  col_var->length = col_var->size;
  col_var->max_len = col_var->size;
  col_var->col_flag = COL_TS_DATA;

  return KStatus::SUCCESS;
}

KStatus DiskRowContainer::Init() {
  std::vector<AttributeInfo> res_bt_schema;
  ErrorInfo err_info;
  k_uint32 offset = 0;
  for (k_uint32 i = 0; i < col_num_; i++) {
    AttributeInfo attr;
    GenAttributeInfo(col_info_[i], &attr);
    attr.offset = offset;
    offset = offset + attr.size;
    res_bt_schema.push_back(attr);
  }
  // create tmp table
  k_int32 encoding = ROW_TABLE | NULLBITMAP_TABLE;
  BigTable *tmp_bt = CreateTempTable(
      res_bt_schema, ExecPool::GetInstance().db_path_, encoding, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("create temp table error : %d, %s", err_info.errcode,
                   err_info.errmsg.c_str());
    return FAIL;
  }
  temp_table_ = tmp_bt;
  return SUCCESS;
}


KStatus DiskRowContainer::Append(std::queue<DataChunkPtr>& buffer) {
  KStatus ret = SUCCESS;
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    ret = Append(buf);
    if (ret != SUCCESS) {
      return ret;
    }
    buffer.pop();
  }
  return ret;
}

KStatus DiskRowContainer::Append(DataChunkPtr& chunk) {
  k_uint32 lines = chunk->Count();
  k_uint32 d_org_siz = temp_table_->size();
  k_uint32 d_sz = d_org_siz + lines;     // expected dest size
  ErrorInfo err_info;
  err_info.errcode = temp_table_->reserveBase(d_sz + 1);
  if (err_info.errcode < 0) {
    LOG_ERROR("append temp table error : %d, %s", err_info.errcode, err_info.errmsg.c_str());
    return FAIL;
  }
  size_t batch_buf_length = chunk->RowSize() * chunk->Count();
  memcpy(static_cast<char *>(temp_table_->getColumnAddr(d_org_siz, 0)), chunk->GetData(), batch_buf_length);
  temp_table_->resize(d_sz);
  count_ = temp_table_->size();
  return SUCCESS;
}

void DiskRowContainer::Sort() {
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
}  // namespace kwdbts
