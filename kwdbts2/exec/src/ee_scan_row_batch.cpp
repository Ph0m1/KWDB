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

#include "ee_scan_row_batch.h"

namespace kwdbts {
void* ScanRowBatch::GetData(k_uint32 col, k_uint32 offset, roachpb::KWDBKTSColumn::ColumnType ctype, roachpb::DataType dt) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return static_cast<char*>(tagdata_[col].tag_data);
  } else {
    if (dt == roachpb::VARCHAR || dt == roachpb::NVARCHAR || dt == roachpb::VARBINARY) {
      return res_.data[col][current_batch_no_]->getVarColData(
          current_batch_line_);
    } else {
      return static_cast<char*>(res_.data[col][current_batch_no_]->mem) +
             current_batch_line_ * offset;
    }
  }
}

k_uint16 ScanRowBatch::GetDataLen(k_uint32 col, k_uint32 offset, roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return tagdata_[col].size;
  } else {
    return res_.data[col][current_batch_no_]->getVarColDataLen(
            current_batch_line_);
  }
}

k_bool ScanRowBatch::IsOverflow(k_uint32 col,
                         roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype ||
      roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    return false;
  }
  return res_.data[col][current_batch_no_]->is_overflow;
}

void ScanRowBatch::Reset() {
  res_.clear();
  selection_.clear();
  is_filter_ = false;
  count_ = 0;
  effect_count_ = 0;
  current_line_ = 0;
  current_batch_line_ = 0;
  current_batch_no_ = 0;
}

/**
 * rows count
 */
k_uint32 ScanRowBatch::Count() {
  if (is_filter_) {
    return effect_count_;
  } else {
    return count_;
  }
}
/**
 *  nextline
 */
k_uint32 ScanRowBatch::NextLine() {
  if (is_filter_) {
    if (current_line_ + 1 >= effect_count_) {
      return -1;
    }
    current_line_++;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
    return current_line_;
  } else {
    if (current_line_ + 1 >= count_) {
      return -1;
    }
    if (res_.data.size() > 0) {
      if (current_batch_line_ < (res_.data[0][current_batch_no_]->count + 1)) {
        current_batch_line_++;
      } else {
        current_batch_no_++;
        current_batch_line_ = 0;
      }
    }
    current_line_++;
    return current_line_;
  }
}
/**
 *  ResetLine
 */
void ScanRowBatch::ResetLine() {
  if (effect_count_ > 0) {
    is_filter_ = true;
    current_line_ = 0;
    current_batch_no_ = selection_[current_line_].batch_;
    current_batch_line_ = selection_[current_line_].line_;
  } else {
    current_batch_no_ = 0;
    current_batch_line_ = 0;
    current_line_ = 0;
  }
}

bool ScanRowBatch::IsNull(k_uint32 col, roachpb::KWDBKTSColumn::ColumnType ctype) {
  if (roachpb::KWDBKTSColumn::TYPE_PTAG == ctype || roachpb::KWDBKTSColumn::TYPE_TAG == ctype) {
    if (ctype == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      return false;
    }
    if (tagdata_[col].is_null) {
      return true;
    }
    return false;
  } else {
    bool is_null = false;
    res_.data[col][current_batch_no_]->isNull(current_batch_line_, &is_null);
    return is_null;
  }
}
}  // namespace kwdbts
