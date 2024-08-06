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

#include <iomanip>
#include "kwdb_type.h"
#include "MMapSegmentTable.h"

using namespace kwdbts;

template<typename SrcType, typename DestType>
inline int convertNumToNum(char* src, char* dst)
{
  *reinterpret_cast<DestType*>(dst) = *reinterpret_cast<SrcType*>(src);
  return 0;
}

// convert column value from fixed-len type to other fixed-len type.
int convertFixedToFixed(DATATYPE old_type, DATATYPE new_type, char* src, char* dst, ErrorInfo& err_info);

// convert column value from fixed-len type to string type
int convertFixedToStr(DATATYPE old_type, char* old_data, char* new_data, ErrorInfo& err_info);

// convert column value from types to fixed len type.
KStatus ConvertToFixedLen(std::shared_ptr<MMapSegmentTable> segment_tbl, char* value, BlockItem* cur_block_item,
                          DATATYPE old_type, DATATYPE new_type, int32_t new_len,
                          size_t start_row, k_uint32 count, k_int32 ts_col, void* bitmap);

// convert column value from types to vartype.
std::shared_ptr<void> ConvertVarLen(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* cur_block_item,
                                    DATATYPE old_type, DATATYPE new_type, size_t row_idx, k_int32 ts_col);

/**
 * @brief iterator used for scan all data in segment.
 *        we will scan block by block, and return generated batch object 
*/
class MMapSegmentTableIterator {
 public:
  MMapSegmentTableIterator(std::shared_ptr<MMapSegmentTable> segment_table, const std::vector<KwTsSpan>& ts_spans,
                         const std::vector<k_uint32>& kw_scan_cols, const std::vector<k_uint32>& ts_scan_cols,
                         const vector<AttributeInfo>& attrs) : segment_table_(segment_table), ts_spans_(ts_spans),
                         kw_scan_cols_(kw_scan_cols), ts_scan_cols_(ts_scan_cols), attrs_(attrs) {}

  BLOCK_ID segment_id() {
    return segment_table_->segment_id();
  }

  // block_start_idx start from 1.
  KStatus GetBatch(BlockItem* cur_block_item, size_t block_start_idx, ResultSet* res, k_uint32 count);

  KStatus Next(BlockItem* cur_block_item, k_uint32* start_offset, ResultSet* res, k_uint32* count);

  // check if ts in iterator scan span.
  bool CheckIfTsInSpan(timestamp64 ts) {
    for (auto& ts_span : ts_spans_) {
      if (ts >= ts_span.begin && ts <= ts_span.end) {
        return true;
      }
    }
    return false;
  }

 private:
  std::shared_ptr<MMapSegmentTable> segment_table_;
  const std::vector<KwTsSpan>& ts_spans_; 
  const std::vector<k_uint32>& kw_scan_cols_;
  const std::vector<k_uint32>& ts_scan_cols_;
  const vector<AttributeInfo>& attrs_;
};
