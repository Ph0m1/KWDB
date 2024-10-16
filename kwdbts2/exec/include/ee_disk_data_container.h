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

#include <memory>
#include <vector>
#include <queue>

#include "kwdb_type.h"
#include "data_type.h"
#include "big_table.h"
#include "ee_exec_pool.h"
#include "ee_data_container.h"
#include "ee_data_chunk.h"

namespace kwdbts {

class DiskDataContainer : public DataContainer {
 public:
  explicit DiskDataContainer(std::vector<ColumnInfo>& col_info)
      : col_info_(col_info) {
    row_size_ = DataChunk::ComputeRowSize(col_info);
    capacity_ = ComputeCapacity();
  }

  DiskDataContainer(std::vector<ColumnOrderInfo>& order_info, std::vector<ColumnInfo>& col_info)
      : order_info_(order_info), col_info_(col_info) {
    row_size_ = DataChunk::ComputeRowSize(col_info);
    capacity_ = ComputeCapacity();
  }

  ~DiskDataContainer() override { Reset(); }

  KStatus Init() override;

  void Sort() override;

  KStatus Append(DataChunk* chunk) override;

  KStatus Append(std::queue<DataChunkPtr>& buffer) override;

  bool IsNull(k_uint32 row, k_uint32 col) override;

  bool IsNull(k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len) override;

  DatumPtr GetData(k_uint32 col) override;

  std::vector<ColumnInfo>& GetColumnInfo() override { return col_info_; }

  k_int32 NextLine() override;

  k_uint32 Count() override { return count_; }

  size_t ComputeBlockSize();

 private:
  k_uint32 ComputeCapacity();

  KStatus GenAttributeInfo(AttributeInfo* col_var, size_t chunk_size);

  void Reset();

  // update cache chunk and calcate offset of row in the cache chunk.
  k_uint32 UpdateReadCacheChunk(k_uint32 row);

  KStatus UpdateWriteCacheChunk(bool& isUpdated);

 private:
  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;

  std::vector<ColumnInfo> col_info_;  // column info
  k_uint32 row_size_{0};
  k_uint32 capacity_{0};
  k_uint32 count_{0};  // total row number
  k_int32 current_line_{-1};  // current row

  // whether do all data chunks(except last chunk) contain the same number of rows?
  bool is_same_count_{true};
  std::vector<k_uint32> cumulative_count_;
  BigTable* temp_table_{nullptr};

  k_uint32 read_cache_chunk_index_{0};
  char read_cache_chunk_obj_[sizeof(DataChunk)]{0};
  DataChunk* read_cache_chunk_ptr_{nullptr};

  char write_cache_chunk_obj_[sizeof(DataChunk)]{0};
  DataChunk* write_cache_chunk_ptr_{nullptr};
  char* write_data_ptr_{nullptr};
};

}  //  namespace kwdbts
