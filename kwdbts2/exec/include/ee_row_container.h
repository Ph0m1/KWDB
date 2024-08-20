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

/**
 * @brief RowContainer
 * @details Sort container interface，derived classes include
 * MemRowContainer and DiskRowContainer
 */
class RowContainer : public IChunk {
 public:
  virtual ~RowContainer() {}

  /**
   * @brief Initialize row container. Now DiskRowContainer override it.
   */
  virtual KStatus Init() { return SUCCESS; }

  /**
   * @brief Sort all rows in the container
   */
  virtual void Sort() {}

  /**
   * @brief Read data in datachunk and append all rows into row container
   * @param[in] chunk datachunk unique_ptr
   */
  virtual KStatus Append(DataChunkPtr& chunk) { return SUCCESS; }

  /**
   * @brief Read all data from a datachunk queue and append into row container
   * @param[in] chunk_queue queue of datachunk unique_ptr
   */
  virtual KStatus Append(std::queue<DataChunkPtr>& buffer) { return SUCCESS; }

  /**
   * @brief Get data pointer at (row, 0)
   * @param[in] row
   */
  virtual DatumPtr GetRow(k_uint32 row) { return nullptr; }

  /* Column info getter */
  virtual std::vector<ColumnInfo>& GetColumnInfo() = 0;

  /**
   * @brief Move to next line
   * @return Return row index of next line when it's valid, otherwise return -1.
   */
  virtual k_int32 NextLine() { return -1; }

  /**
   * @brief return count
   */
  virtual k_uint32 Count() { return -1; }
  void SetMaxOutputRows(k_uint32 limit) { max_output_rows_ = limit; }

 public:
  k_uint32 max_output_rows_{UINT32_MAX};
};

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
class MemRowContainer : public RowContainer, public std::enable_shared_from_this<MemRowContainer> {
 public:
  explicit MemRowContainer(std::vector<ColumnOrderInfo> order_info, std::vector<ColumnInfo> col_info, k_uint32 capacity)
      : col_info_(col_info), capacity_(capacity) {
    order_info_ = std::move(order_info);
  }

  ~MemRowContainer();

  std::shared_ptr<MemRowContainer> Ptr() {
    return shared_from_this();
  }

  [[nodiscard]] inline k_uint32 RowSize() const { return row_size_; }
  /**
   * @brief increase the count
   */
  void AddCount() { ++count_; }

  /**
   * @brief Copy the row from another DataChunk (same column schema)
   * @param[in] row
   * @param[in] src
   */
  void CopyRow(k_uint32 row, void* src);

  // append ResultSet
  KStatus Append(DataChunkPtr& chunk) override;

  KStatus Append(std::queue<DataChunkPtr>& buffer) override;

  std::vector<ColumnInfo>& GetColumnInfo() override { return col_info_; }

  // data sort
  void Sort() override;

  k_int32 NextLine() override;

  k_uint32 Count() override;

  KStatus Init() override;

  bool IsNull(k_uint32 row, k_uint32 col) override;
  DatumPtr GetData(k_uint32 row, k_uint32 col) override;
  DatumPtr GetData(k_uint32 col) override;
  bool IsNull(k_uint32 col) override;

 private:
  /**
   * @return return -1 if memory allocation fails
   */
  int Initialize();

 private:
  std::vector<ColumnInfo> col_info_;        // column info
  k_uint32 capacity_{0};                    // data capacity
  k_bits32 col_num_{0};                     // the number of col
  k_uint32 bitmap_size_{0};                 // length of bitmap
  std::vector<k_uint32> col_offset_;        // column offset
  k_uint32 row_size_{0};                    // the total length of one row
  char* data_{nullptr};                     // Multiple rows of column data（not tag）
  k_uint32 count_{0};                       // total number
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
  BigTable *materialized_file_{nullptr};
};

}   // namespace kwdbts
