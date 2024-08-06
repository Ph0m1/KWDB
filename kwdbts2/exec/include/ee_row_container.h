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

#include <memory>
#include <list>
#include <vector>
#include <queue>
#include <algorithm>
#include <sstream>
#include <utility>

#include "kwdb_type.h"
#include "ee_data_chunk.h"
// #include "rocksdb/db.h"
#include "ee_pb_plan.pb.h"

class BigTable;
struct AttributeInfo;
namespace kwdbts {

#define MAX_SORT_MEM_SIZE 16777216  // 16MB

struct ColumnOrderInfo {
  k_uint32 col_idx;
  TSOrdering_Column_Direction direction;
};

class RowContainer;

class OrderColumnCompare {
 public:
  OrderColumnCompare(RowContainer *container, std::vector<ColumnOrderInfo> order_info)
      : container_(container), order_info_(order_info) {}

  bool operator()(k_uint32 a, k_uint32 b);

 private:
  RowContainer *container_;
  std::vector<ColumnOrderInfo> order_info_;
};

/**
 * @brief MemRowContainer
 * @details A memory-sorting container based on DataChunk
 */
class MemRowContainer : public DataChunk, public std::enable_shared_from_this<MemRowContainer> {
 public:
  explicit MemRowContainer(std::vector<ColumnOrderInfo> order_info, std::vector<ColumnInfo> col_info, k_uint32 capacity)
      : DataChunk(col_info, capacity) {
    order_info_ = std::move(order_info);
  }

  ~MemRowContainer() override = default;

  std::shared_ptr<MemRowContainer> Ptr() {
    return shared_from_this();
  }

  // append ResultSet
  KStatus Append(DataChunkPtr& chunk) override;

  KStatus Append(std::queue<DataChunkPtr>& buffer) override;

  // data sort
  void Sort() override;

  k_int32 NextLine() override;

  k_uint32 Count() override;

  KStatus Init() override;
  DatumPtr GetData(k_uint32 col) override;
  bool IsNull(k_uint32 col) override;

 private:
  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;
  k_int32 current_sel_idx_{-1};
};

class DiskRowContainer : public RowContainer {
 public:
  DiskRowContainer(std::vector<ColumnOrderInfo> order_info, std::vector<ColumnInfo> col_info, k_uint32 capacity) {
    order_info_ = order_info;
    col_info_ = col_info;
    col_num_ = col_info.size();
    count_ = capacity;
  }

  KStatus Init() override;
  ~DiskRowContainer() override { Reset(); }

  KStatus Append(DataChunkPtr &chunk) override;
  KStatus Append(std::queue<DataChunkPtr>& buffer) override;
  void Sort() override;
  bool IsNull(k_uint32 row, k_uint32 col) override;
  DatumPtr GetData(k_uint32 row, k_uint32 col) override;
  bool IsNull(k_uint32 col) override;
  DatumPtr GetData(k_uint32 col) override;
  std::vector<ColumnInfo>& GetColumnInfo() override { return col_info_; }

  k_int32 NextLine() override;
  k_uint32 Count() override;
  DatumPtr GetRow(k_uint32 row) override;

 private:
  KStatus GenAttributeInfo(const ColumnInfo &col_info, AttributeInfo* col_var);
  void Reset();

 private:
  std::vector<ColumnOrderInfo> order_info_;
  std::vector<k_uint32> selection_;
  k_int32 current_sel_idx_{-1};
  std::vector<ColumnInfo> col_info_;  // col info
  k_uint32 col_num_{0};       // col num
  k_uint32 count_{0};
  BigTable *temp_table_{nullptr};
};

}   // namespace kwdbts
