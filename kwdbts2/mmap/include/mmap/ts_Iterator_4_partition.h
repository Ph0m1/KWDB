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

#pragma once

#include <iomanip>
#include "kwdb_type.h"
#include "ts_time_partition.h"
#include "mmap/mmap_segment_table_iterator.h"


using namespace kwdbts;

struct TsPartitonIteratorParams {
  uint64_t entity_group_id;
  uint32_t sub_group_id;
  vector<uint32_t> entity_ids;
  const std::vector<KwTsSpan>& ts_spans;
  const std::vector<k_uint32>& kw_scan_cols;
  const std::vector<k_uint32>& ts_scan_cols;
  const vector<AttributeInfo>& attrs;
};

/**
 * @brief iterator used for scan all data in segment.
 *        we will scan block by block, and return generated batch object 
*/
class TsPartitonIterator {
 public:
  TsPartitonIterator(TsTimePartition* p_table, const TsPartitonIteratorParams& params) :
        partition_(p_table), params_(params) {}
  
  ~TsPartitonIterator() {
    if (segment_iter_ != nullptr) {
      delete segment_iter_;
      segment_iter_ = nullptr;
    }
   }
  // if count =0 means all scan over. res.enityindex info mark which entity this res belong to.
  KStatus Next(ResultSet* res, k_uint32* count, bool full_block = false, TsBlockFullData* block_data = nullptr);

 private:
  KStatus fillblockItemData(BlockItem* block_item, TsBlockFullData* block_data, bool* ignored);
  
  KStatus blockItemNext(ResultSet* res, k_uint32* count);
  // return -1 means partition tables scan over.
  inline int nextBlockItem();
  // return -1 means entityList scan over.
  inline int nextEntity() {
    cur_entity_index_++;
    if (cur_entity_index_ >= params_.entity_ids.size()) {
      return -1;
    }
    block_item_queue_.clear();
    partition_->GetAllBlockItems(params_.entity_ids[cur_entity_index_], block_item_queue_);
    return 0;
  }

 private:
  TsTimePartition* partition_;
  TsPartitonIteratorParams params_;
  // save all BlockItem objects of current enitity in the partition being queried
  std::deque<BlockItem*> block_item_queue_;
  BlockItem* cur_block_item_{nullptr};
  // save the data offset within the BlockItem object being queried, used for traversal
  k_uint32 cur_blockdata_offset_ = 1;
  int cur_entity_index_{-1};
  MMapSegmentTableIterator* segment_iter_{nullptr};
};
