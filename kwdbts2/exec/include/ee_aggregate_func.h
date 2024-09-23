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

#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <limits>

#include "ee_combined_group_key.h"
#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "ee_common.h"
#include "ee_hash_table.h"

namespace kwdbts {

struct DistinctOpt {
  bool needDistinct;
  std::vector<roachpb::DataType>& col_types;
  std::vector<k_uint32>& col_lens;
  std::vector<k_uint32>& group_cols;
};

class AggregateFunc {
 public:
  AggregateFunc(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : col_idx_(col_idx), len_(len) {
    arg_idx_.push_back(arg_idx);
  }

  virtual ~AggregateFunc() {
    SafeDeletePointer(seen_);
  }

  /**
   * @brief agg update function used by Hash Agg OP.
   * @param dest the target location that the agg result is saving to.
   * @param bitmap the nullable bitmap of agg results.
   * @param chunk the input data chunk coming from previous OP.
   * @param line the processing line in input data chunk.
   */
  virtual void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) {}

  /**
   * @brief agg update function used by Ordered Agg OP (column-mode).
   * @param chunks the target data chunks that the agg results are saving to.
   * @param start_line_in_begin_chunk the begin line in the first target data chunk.
   * @param data_container the input data chunk coming from previous OP.
   * @param group_by_metadata the orderby information for the input records.
   * @param distinctOpt the distinct options.
   */
  virtual int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk,
                          IChunk* data_container, GroupByMetadata& group_by_metadata,
                          DistinctOpt& distinctOpt) { return 0; }

  /**
   * @brief agg update function used by Agg Scan OP.
   * @param chunks the target data chunks that the agg results are saving to.
   * @param start_line_in_begin_chunk the begin line in the first target data chunk.
   * @param data_container the input DataContainer coming from storage layer.
   * @param group_by_metadata the orderby information for the input records.
   * @param renders render definitions for input data.
   */
  virtual void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk,
                           RowBatch* row_batch, GroupByMetadata& group_by_metadata, Field** renders) {}

  /**
   * @brief agg update function used by Agg Scan OP to process input data chunk using batch mode.
   * @param chunk the input data chunk coming from previous OP.
   * @param ht the hash table to save the agg results.
   * @param bitmap_offset the bitmap of agg results.
   * @param distinctOpt the distinct options.
   */
  int AddOrUpdate(IChunk* chunk, LinearProbingHashTable* ht,
                  k_uint32 bitmap_offset, DistinctOpt& distinctOpt) {
    for (k_uint32 line = 0; line < chunk->Count(); ++line) {
      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(chunk, line, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      k_uint64 loc;
      if (ht->FindOrCreateGroups(chunk, line, distinctOpt.group_cols, &loc) < 0) {
        return -1;
      }
      auto agg_ptr = ht->GetAggResult(loc);

      addOrUpdate(agg_ptr, agg_ptr + bitmap_offset, chunk, line);
    }
    return 0;
  }

  void SetOffset(k_uint32 offset) {
    offset_ = offset;
  }

  inline k_uint32 GetOffset() const {
    return offset_;
  }

  inline k_uint32 GetLen() const {
    return len_;
  }

  static k_bool IsNull(const char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    return (bitmap[index] & mask) == 0;
  }

  // 0 indicates Null，1 indicates not Null
  static void SetNotNull(char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    bitmap[index] |= mask;
  }

  // 0 indicates Null，1 indicates not Null
  static void SetNull(char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    bitmap[index] &= ~mask;
  }

  static int CompareDecimal(DatumPtr src, DatumPtr dest) {
    k_bool src_is_double = *reinterpret_cast<k_bool*>(src);
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (!src_is_double && !dest_is_double) {
      k_int64 src_val = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
      if (src_val > dest_val)
        return 1;
      else if (src_val < dest_val)
        return -1;
    } else {
      k_double64 src_val, dest_val;
      if (src_is_double) {
        src_val = *reinterpret_cast<k_double64*>(src + sizeof(k_bool));
        std::memcpy(dest, &src_is_double, sizeof(k_bool));
      } else {
        k_int64 src_ival = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
        src_val = (k_double64) src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64) dest_ival;
      }

      if (src_val - dest_val > std::numeric_limits<double>::epsilon()) {
        return 1;
      } else if (dest_val - src_val > std::numeric_limits<double>::epsilon()) {
        return -1;
      }
    }

    return 0;
  }

  static void ConstructGroupKeys(IChunk* chunk, std::vector<k_uint32>& all_cols,
                                 std::vector<roachpb::DataType>& col_types, k_uint32 line,
                                 CombinedGroupKey& field_keys) {
    field_keys.Reserve(all_cols.size());
    for (auto idx : all_cols) {
      roachpb::DataType col_type = col_types[idx];

      bool is_null = chunk->IsNull(line, idx);
      if (is_null) {
        field_keys.AddGroupKey(nullptr, col_type);
        continue;
      }

      DatumPtr ptr = chunk->GetData(line, idx);
      field_keys.AddGroupKey(ptr, col_type);
    }
  }

  int isDistinct(IChunk* chunk, k_uint32 line,
                 std::vector<roachpb::DataType>& col_types,
                 std::vector<k_uint32>& col_lens,
                 std::vector<k_uint32>& group_cols,
                 k_bool* is_distinct);

  virtual roachpb::DataType GetStorageType() const { return roachpb::DataType::UNKNOWN; }

  virtual k_uint32 GetStorageLength() const { return 0; }

  static char* GetFieldDataPtr(Field* field, RowBatch* row_batch) {
    if (field->get_field_type() == Field::Type::FIELD_ITEM) {
      k_uint32 col_idx_in_rs = field->getColIdxInRs();
      k_uint32 storage_len = field->get_storage_length();
      roachpb::DataType storage_type = field->get_storage_type();
      roachpb::KWDBKTSColumn::ColumnType column_type = field->get_column_type();

      return static_cast<char*>(
          row_batch->GetData(col_idx_in_rs, 0 == field->get_num() ? storage_len + 8 : storage_len,
                             column_type, storage_type));
    }

    return field->get_ptr();
  }

 protected:
  LinearProbingHashTable* seen_{nullptr};

  // The output column index in the result data container.
  k_uint32 col_idx_;

  // The input column index for current aggregation function.
  std::vector<k_uint32> arg_idx_;

  // The offset of the aggregate result in the bucket
  k_uint32 offset_{};

  // The length of the aggregate result in the output data container.
  k_uint32 len_;
};

////////////////// AnyNotNullAggregate /////////////////////////

template<typename T>
class AnyNotNullAggregate : public AggregateFunc {
 public:
  AnyNotNullAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AnyNotNullAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // if the data's value is NULL，return directly
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      DatumPtr src = chunk->GetData(line, arg_idx_[0]);
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        auto dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        char* src_ptr = data_container->GetData(row, arg_idx);

        std::memcpy(dest_ptr, src_ptr, len_);
        current_data_chunk_->SetNotNull(target_row, col_idx_);
      }

      data_container->NextLine();
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }

        auto dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);

        if constexpr (std::is_same_v<T, std::string>) {
          k_uint16 len;
          if (IsVarStringType(storage_type)) {
            len = arg_field->ValStrLength(src_ptr);
          } else {
            std::string_view str = std::string_view{src_ptr};
            len = static_cast<k_int16>(str.length());
          }
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
        } else {
          std::memcpy(dest_ptr, src_ptr, len_);
        }
        current_data_chunk_->SetNotNull(target_row, col_idx_);
      }
      row_batch->NextLine();
    }
  }
};

////////////////////////// MaxAggregate //////////////////////////

template<typename T>
class MaxAggregate : public AggregateFunc {
 public:
  MaxAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~MaxAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // if the data's value is NULL，return directly
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // The aggregate row of the bucket is assigned for the first time and then
      // returned
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    if constexpr (std::is_same_v<T, std::string>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + offset_);
      auto dest_val = std::string_view(dest + offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) > 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      if (AggregateFunc::CompareDecimal(src, dest + offset_) > 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (src_val - dest_val > std::numeric_limits<T>::epsilon()) {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val > dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    T max_val;

    if (target_row >= 0) {
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      max_val = is_dest_null ?
                std::numeric_limits<T>::lowest() : *reinterpret_cast<T*>(dest_ptr);
    } else {
      max_val = std::numeric_limits<T>::lowest();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &max_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = std::numeric_limits<T>::lowest();
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val > std::numeric_limits<T>::epsilon()) {
            max_val = src_val;
          }
        } else {
          if (src_val > max_val) {
            max_val = src_val;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &max_val, len_);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                     GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr = nullptr;
    char* max_val = nullptr;

    if (target_row >= 0) {
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      max_val = is_dest_null ? nullptr : dest_ptr;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && max_val != dest_ptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &max_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = nullptr;
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        if (max_val == nullptr) {
          is_dest_null = false;
          max_val = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, max_val) > 0) {
            max_val = src_ptr;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null && max_val != dest_ptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &max_val, len_);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    bool is_dest_null = true;
    std::string_view max_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (is_dest_null) {
        max_val = "";
      } else {
        k_uint16 len;
        std::memcpy(&len, dest_ptr, STRING_WIDE);
        max_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE, len};
      }
    } else {
      max_val = "";
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_uint16 len = max_val.length();
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, max_val.data(), len + 1);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = "";
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        std::string_view src_val = std::string_view(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null) {
          is_dest_null = false;
          max_val = src_val;
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_uint16 len = max_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, max_val.data(), len + 1);
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, std::string>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    T max_val;

    if (target_row >= 0) {
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      max_val = is_dest_null ?
                std::numeric_limits<T>::lowest() : *reinterpret_cast<T*>(dest_ptr);
    } else {
      max_val = std::numeric_limits<T>::lowest();
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &max_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = std::numeric_limits<T>::lowest();
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        is_dest_null = false;
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val > std::numeric_limits<T>::epsilon()) {
            max_val = src_val;
          }
        } else {
          if (src_val > max_val) {
            max_val = src_val;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &max_val, len_);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr = nullptr;
    char* max_val = nullptr;

    if (target_row >= 0) {
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      max_val = is_dest_null ? nullptr : dest_ptr;
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && max_val != dest_ptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &max_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = nullptr;
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);

        if (max_val == nullptr) {
          is_dest_null = false;
          max_val = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, max_val) > 0) {
            max_val = src_ptr;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null && max_val != dest_ptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &max_val, len_);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    bool is_dest_null = true;
    std::string_view max_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (is_dest_null) {
        max_val = "";
      } else {
        k_uint16 len;
        std::memcpy(&len, dest_ptr, STRING_WIDE);
        max_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE, len};
      }
    } else {
      max_val = "";
    }

    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_uint16 len = max_val.length();
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, max_val.data(), len);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        max_val = "";
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);
        std::string_view src_val;

        if (IsVarStringType(storage_type)) {
          auto str_length = arg_field->ValStrLength(src_ptr);
          src_val = std::string_view{src_ptr, static_cast<k_uint32>(str_length)};
        } else {
          src_val = std::string_view{src_ptr};
        }

        if (is_dest_null) {
          is_dest_null = false;
          max_val = src_val;
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_uint16 len = max_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, max_val.data(), len);
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, std::string>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// MinAggregate //////////////////////////
template<typename T>
class MinAggregate : public AggregateFunc {
 public:
  MinAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~MinAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // The aggregate row of the bucket is assigned for the first time and then
      // returned
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    if constexpr (std::is_same_v<T, std::string>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + offset_);
      auto dest_val = std::string_view(dest + offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) < 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      if (AggregateFunc::CompareDecimal(src, dest + offset_) < 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (dest_val - src_val > std::numeric_limits<T>::epsilon()) {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val < dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    T min_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      min_val = is_dest_null ?
                std::numeric_limits<T>::max() : *reinterpret_cast<T*>(dest_ptr);
    } else {
      min_val = std::numeric_limits<T>::max();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &min_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = std::numeric_limits<T>::max();
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;

        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val - src_val > std::numeric_limits<T>::epsilon()) {
            min_val = src_val;
          }
        } else {
          if (src_val < min_val) {
            min_val = src_val;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &min_val, len_);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                     GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr = nullptr;
    char* min_val = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      min_val = is_dest_null ? nullptr : dest_ptr;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && min_val != dest_ptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &min_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = nullptr;
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);
        if (min_val == nullptr) {
          is_dest_null = false;
          min_val = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, min_val) < 0) {
            min_val = src_ptr;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null && min_val != dest_ptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &min_val, len_);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    std::string_view min_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (is_dest_null) {
        min_val = "";
      } else {
        k_uint16 len;
        std::memcpy(&len, dest_ptr, STRING_WIDE);
        min_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE, len};
      }
    } else {
      min_val = "";
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_uint16 len = min_val.length();
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, min_val.data(), len + 1);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = "";
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        std::string_view src_val = std::string_view(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null) {
          is_dest_null = false;
          min_val = src_val;
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_uint16 len = min_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, min_val.data(), len + 1);
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, std::string>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    T min_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      min_val = is_dest_null ?
                std::numeric_limits<T>::max() : *reinterpret_cast<T*>(dest_ptr);
    } else {
      min_val = std::numeric_limits<T>::max();
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &min_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = std::numeric_limits<T>::max();
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        is_dest_null = false;

        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val - src_val > std::numeric_limits<T>::epsilon()) {
            min_val = src_val;
          }
        } else {
          if (src_val < min_val) {
            min_val = src_val;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &min_val, len_);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr = nullptr;
    char* min_val = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      min_val = is_dest_null ? nullptr : dest_ptr;
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && min_val != dest_ptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &min_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = nullptr;
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);
        if (min_val == nullptr) {
          is_dest_null = false;
          min_val = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, min_val) < 0) {
            min_val = src_ptr;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null && min_val != dest_ptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &min_val, len_);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    char* dest_ptr;
    std::string_view min_val;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (is_dest_null) {
        min_val = "";
      } else {
        k_uint16 len;
        std::memcpy(&len, dest_ptr, STRING_WIDE);
        min_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE, len};
      }
    } else {
      min_val = "";
    }

    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_uint16 len = min_val.length();
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, min_val.data(), len + 1);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        min_val = "";
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);
        std::string_view src_val;

        if (IsVarStringType(storage_type)) {
          auto str_length = arg_field->ValStrLength(src_ptr);
          src_val = std::string_view{src_ptr, static_cast<k_uint32>(str_length)};
        } else {
          src_val = std::string_view{src_ptr};
        }

        if (is_dest_null) {
          is_dest_null = false;
          min_val = src_val;
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_uint16 len = min_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, min_val.data(), len + 1);
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, std::string>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// SumIntAggregate //////////////////////////

/**
 * SUM_INT aggregation expects int64 as input/output type
*/
class SumIntAggregate : public AggregateFunc {
 public:
  SumIntAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~SumIntAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      std::memcpy(dest + offset_, src, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    k_int64 src_val = *reinterpret_cast<k_int64*>(src);
    k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + offset_);
    k_int64 sum_int = src_val + dest_val;
    std::memcpy(dest + offset_, &sum_int, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      if (!is_dest_null) {
        sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val_i, sizeof(k_int64));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_i = 0;
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr);

        sum_val_i += src_val;
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val_i, sizeof(k_int64));
    }
    return 0;
  }
};

////////////////////////// SumAggregate //////////////////////////

/**
 * SUM aggregation input/output type summary:
 *
 *    INPUT TYPE            OUTPUT TYPE
 *    int16/int32/int64     decimal
 *    decimal               decimal
 *    float/double          double
*/
template<typename T_SRC, typename T_DEST>
class SumAggregate : public AggregateFunc {
 public:
  SumAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~SumAggregate() override = default;

  void handleDouble(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
      T_DEST dest_val = src_val;
      std::memcpy(dest, &dest_val, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
    T_DEST dest_val = *reinterpret_cast<T_DEST*>(dest);
    T_DEST sum = src_val + dest_val;
    std::memcpy(dest, &sum, len_);
  }

  void handleDecimal(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      std::memcpy(dest, src, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    // double flag
    k_bool src_is_double = *reinterpret_cast<k_bool*>(src);
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (!src_is_double && !dest_is_double) {
      // Integer + Integer
      k_int64 src_val = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));

      if ((dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val) ||
          (dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val)) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64) dest_val + (k_double64) src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_double64));
      } else {
        k_int64 sum = dest_val + src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      }
    } else {
      k_double64 src_val, dest_val;
      if (src_is_double) {
        src_val = *reinterpret_cast<k_double64*>(src + sizeof(k_bool));
        std::memcpy(dest, &src_is_double, sizeof(k_bool));
      } else {
        k_int64 src_ival = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
        src_val = (k_double64) src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64) dest_ival;
      }

      k_double64 sum = src_val + dest_val;
      std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
    }
  }

  void handleInteger(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      k_bool is_double = false;
      std::memcpy(dest, &is_double, sizeof(k_bool));

      T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
      auto dest_val = (k_int64) src_val;
      std::memcpy(dest + sizeof(k_bool), &dest_val, sizeof(k_int64));

      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    T_SRC src_val = *reinterpret_cast<T_SRC*>(src);

    // double flag
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (dest_is_double) {
      k_double64 dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
      k_double64 sum = dest_val + (k_double64) src_val;
      std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_double64));
    } else {
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
      if ((dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val) ||
          (dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val)) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64) dest_val + (k_double64) src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      } else {
        k_int64 sum = dest_val + src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      handleDouble(src, dest + offset_, is_dest_null, bitmap);
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      // input type: decimal
      handleDecimal(src, dest + offset_, is_dest_null, bitmap);
    } else {
      // input type: int16/int32/int64
      handleInteger(src, dest + offset_, is_dest_null, bitmap);
    }
  }

  int handleDouble(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                   GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum_val = 0.0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val = 0.0;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);

        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);
        sum_val += src_val;
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val, len_);
    }
    return 0;
  }

  int handleInteger(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);

        if (dest_is_double) {
          sum_val_d += src_val;
        } else {
          if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
              (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
            dest_is_double = true;
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
          } else {
            sum_val_i += src_val;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
    return 0;
  }

  int handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double = false;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        k_bool src_is_double = *reinterpret_cast<k_bool*>(src_ptr);


        if (src_is_double) {
          k_double64 src_val = *reinterpret_cast<k_double64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += src_val;
          } else {
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
            dest_is_double = true;
          }
        } else {
          k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += (k_double64) src_val;
          } else {
            if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
                (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
              dest_is_double = true;
              sum_val_d = static_cast<k_double64>(sum_val_i);
              sum_val_d += (k_double64) src_val;
            } else {
              sum_val_i += src_val;
            }
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
    return 0;
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      if (handleDouble(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      if (handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    } else {
      // input type: int16/int32/int64
      if (handleInteger(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    }
    return 0;
  }

  void handleDouble(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum_val = 0.0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val = 0.0;
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        is_dest_null = false;
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);

        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);
        sum_val += src_val;
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val, len_);
    }
  }

  void handleInteger(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        is_dest_null = false;
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);
        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);

        if (dest_is_double) {
          sum_val_d += src_val;
        } else {
          if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
              (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
            dest_is_double = true;
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
          } else {
            sum_val_i += src_val;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double = false;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        is_dest_null = false;
        char* src_ptr = GetFieldDataPtr(arg_field, row_batch);
        k_bool src_is_double = *reinterpret_cast<k_bool*>(src_ptr);


        if (src_is_double) {
          k_double64 src_val = *reinterpret_cast<k_double64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += src_val;
          } else {
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
            dest_is_double = true;
          }
        } else {
          k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += (k_double64) src_val;
          } else {
            if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
                (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
              dest_is_double = true;
              sum_val_d = static_cast<k_double64>(sum_val_i);
              sum_val_d += (k_double64) src_val;
            } else {
              sum_val_i += src_val;
            }
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      handleDouble(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      // input type: int16/int32/int64
      handleInteger(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// CountAggregate //////////////////////////

/*
    The return type of CountAggregate is BIGINT
*/
class CountAggregate : public AggregateFunc {
 public:
  CountAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~CountAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      // first assign
      k_int64 count = 0;
      std::memcpy(dest + offset_, &count, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_int64 val = *reinterpret_cast<k_int64*>(dest + offset_);
    ++val;
    std::memcpy(dest + offset_, &val, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        ++val;
      }

      data_container->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        ++val;
      }

      row_batch->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
  }
};

////////////////////////// CountRowAggregate //////////////////////////

/*
    The return type of CountRowAggregate is BIGINT
*/
class CountRowAggregate : public AggregateFunc {
 public:
  CountRowAggregate(k_uint32 col_idx, k_uint32 len) : AggregateFunc(col_idx, 0, len) {
  }

  ~CountRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      // first assign
      k_int64 count = 0;
      std::memcpy(dest + offset_, &count, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    k_int64 val = *reinterpret_cast<k_int64*>(dest + offset_);
    ++val;
    std::memcpy(dest + offset_, &val, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      ++val;

      data_container->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      ++val;

      row_batch->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
  }
};

////////////////////////// AVGRowAggregate //////////////////////////
template<typename T>
class AVGRowAggregate : public AggregateFunc {
 public:
  AVGRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AVGRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // first assign
      k_double64 sum = 0.0f;
      std::memcpy(dest + offset_, &sum, sizeof(k_double64));
      k_int64 count = 0;
      std::memcpy(dest + offset_ + sizeof(k_double64), &count, sizeof(k_int64));
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    T src_val = *reinterpret_cast<T*>(src);
    k_double64 dest_val = *reinterpret_cast<k_double64*>(dest + offset_);
    k_double64 sum_val = src_val + dest_val;
    std::memcpy(dest + offset_, &sum_val, sizeof(k_double64));

    k_int64 count_val = *reinterpret_cast<k_int64*>(dest + offset_ + sizeof(k_double64));
    ++count_val;
    std::memcpy(dest + offset_ + sizeof(k_double64), &count_val, sizeof(k_int64));
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum = 0.0f;
    k_int64 count = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum = *reinterpret_cast<k_double64*>(dest_ptr);
        count = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_double64));
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum = 0.0f;
        count = 0;
      }

      char* src_ptr = data_container->GetData(row, arg_idx);

      T src_val = *reinterpret_cast<T*>(src_ptr);
      sum += src_val;
      ++count;

      data_container->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
    }
    return 0;
  }
};

////////////////////////// STDDEVRowAggregate //////////////////////////

class STDDEVRowAggregate : public AggregateFunc {
 public:
  STDDEVRowAggregate(k_uint32 col_idx, k_uint32 len) : AggregateFunc(col_idx, 0, len) {
  }

  ~STDDEVRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // do nothing temporarily.
  }
};

////////////////////////// LastAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class LastAggregate : public AggregateFunc {
 public:
  LastAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx,
                k_int32 point_idx, k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len),
        ts_idx_(ts_idx),
        point_idx_(point_idx) {}

  ~LastAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    DatumPtr dest_ptr = dest + offset_;
    k_int64 point_ts = INT64_MAX;
    if (point_idx_ != -1) {  // for last point
      DatumPtr point_ptr = chunk->GetData(line, point_idx_);
      point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
    }

    if (is_dest_null) {
      if (ts > point_ts && point_ts != INT64_MAX) {
        return;
      }
      // first assign
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));
    if (ts > last_ts &&
        (point_ts == INT64_MAX || (ts <= point_ts && point_ts != INT64_MAX))) {
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    k_uint16 str_length = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          k_int64 point_ts = INT64_MAX;
          if (point_idx_ != -1) {  // for last point
            DatumPtr point_ptr = input_chunk->GetData(row, point_idx_);
            point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
          }
          if (!(last_ts_ > point_ts && point_ts != INT64_MAX)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
        str_length = 0;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        k_int64 point_ts = INT64_MAX;
        if (point_idx_ != -1) {  // for last point
          DatumPtr point_ptr = input_chunk->GetData(row, point_idx_);
          point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
        }
        if (ts > last_ts_ && (point_ts == INT64_MAX ||
                              (ts <= point_ts && point_ts != INT64_MAX))) {
          last_ts_ = ts;
          last_line_ptr = input_chunk->GetData(row, arg_idx);
        }
      }
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    k_uint16 str_length = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto* point_field = renders[point_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          k_int64 point_ts = INT64_MAX;
          if (point_idx_ != -1) {  // for last point
            DatumPtr point_ptr = GetFieldDataPtr(point_field, row_batch);
            point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
          }
          if (!(last_ts_ > point_ts && point_ts != INT64_MAX)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            if (IS_STRING_FAMILY) {
              std::memcpy(dest_ptr, &str_length, STRING_WIDE);
              std::memcpy(dest_ptr + STRING_WIDE, last_line_ptr, str_length);
            } else {
              std::memcpy(dest_ptr, last_line_ptr, len_);
            }
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
        str_length = 0;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        k_int64 point_ts = INT64_MAX;
        if (point_idx_ != -1) {  // for last point
          DatumPtr point_ptr = GetFieldDataPtr(point_field, row_batch);;
          point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
        }

        if (ts > last_ts_ && (point_ts == INT64_MAX ||
                              (ts <= point_ts && point_ts != INT64_MAX))) {
          last_ts_ = ts;
          last_line_ptr = GetFieldDataPtr(arg_field, row_batch);

          if (IS_STRING_FAMILY) {
            if (IsVarStringType(storage_type)) {
              str_length = arg_field->ValStrLength(last_line_ptr);
            } else {
              std::string_view str = std::string_view{last_line_ptr};
              str_length = static_cast<k_int16>(str.length());
            }
          }
        }
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);

      if (IS_STRING_FAMILY) {
        std::memcpy(dest_ptr, &str_length, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, last_line_ptr, str_length);
      } else {
        std::memcpy(dest_ptr, last_line_ptr, len_);
      }
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
  k_int32 point_idx_ = -1;
};

////////////////////////// LastRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class LastRowAggregate : public AggregateFunc {
 public:
  LastRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));

    if (ts > last_ts) {
      k_bool is_data_null = chunk->IsNull(line, arg_idx_[0]);
      if (is_data_null) {
        SetNull(bitmap, col_idx_);
      } else {
        DatumPtr src = chunk->GetData(line, arg_idx_[0]);
        std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
        SetNotNull(bitmap, col_idx_);
      }
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;
    k_uint16 str_length = 0;
    k_bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, last_line_ptr, len_);
        } else {
          if (is_dest_null && target_row >= 0) {
            current_data_chunk_->SetNull(target_row, col_idx_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
        str_length = 0;
        is_dest_null = true;
      }

      char* ts_src_ptr = data_container->GetData(row, ts_idx_);
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        if (!data_container->IsNull(row, arg_idx)) {
          is_dest_null = false;
          last_line_ptr = data_container->GetData(row, arg_idx);
        } else {
          is_dest_null = true;
          last_line_ptr = nullptr;
        }
      }

      data_container->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    } else {
      if (is_dest_null && target_row >= 0) {
        current_data_chunk_->SetNull(target_row, col_idx_);
      }
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;
    k_uint16 str_length = 0;
    k_bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);

          if (IS_STRING_FAMILY) {
            std::memcpy(dest_ptr, &str_length, STRING_WIDE);
            std::memcpy(dest_ptr + STRING_WIDE, last_line_ptr, str_length);
          } else {
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        } else {
          if (is_dest_null && target_row >= 0) {
            current_data_chunk_->SetNull(target_row, col_idx_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
        str_length = 0;
        is_dest_null = true;
      }

      char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        if (!(arg_field->isNullable() && arg_field->is_nullable())) {
          is_dest_null = false;
          last_line_ptr = GetFieldDataPtr(arg_field, row_batch);
          if (IS_STRING_FAMILY) {
            if (IsVarStringType(storage_type)) {
              str_length = arg_field->ValStrLength(last_line_ptr);
            } else {
              std::string_view str = std::string_view{last_line_ptr};
              str_length = static_cast<k_int16>(str.length());
            }
          }
        } else {
          is_dest_null = true;
          last_line_ptr = nullptr;
        }
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);

      if (IS_STRING_FAMILY) {
        std::memcpy(dest_ptr, &str_length, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, last_line_ptr, str_length);
      } else {
        std::memcpy(dest_ptr, last_line_ptr, len_);
      }
    } else {
      if (is_dest_null && target_row >= 0) {
        current_data_chunk_->SetNull(target_row, col_idx_);
      }
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
};

////////////////////////// LastTSAggregate //////////////////////////

class LastTSAggregate : public AggregateFunc {
 public:
  LastTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx,
                  k_int32 point_Idx, k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len),
        ts_idx_(ts_idx),
        point_idx_(point_Idx) {}

  ~LastTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;
    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    k_int64 point_ts = INT64_MAX;
    if (point_idx_ != -1) {  // for last point
      DatumPtr point_ptr = chunk->GetData(line, point_idx_);
      point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
    }
    if (is_dest_null) {
      if (ts > point_ts && point_ts != INT64_MAX) {
        return;
      }
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto last_ts =
        *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));
    if (ts > last_ts &&
        (point_ts == INT64_MAX || (ts <= point_ts && point_ts != INT64_MAX))) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      k_int64 point_ts = INT64_MAX;
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          if (point_idx_ != -1) {  // for last point
            DatumPtr point_ptr = input_chunk->GetData(row, point_idx_);
            point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
          }

          if (!(last_ts_ > point_ts && point_ts != INT64_MAX)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (point_idx_ != -1) {  // for last point
          DatumPtr point_ptr = input_chunk->GetData(row, point_idx_);
          point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
        }
        if ((last_line_ptr == nullptr || ts > last_ts_) &&
            (point_ts == INT64_MAX ||
             (ts <= point_ts && point_ts != INT64_MAX))) {
          last_ts_ = ts;
          last_line_ptr = ts_src_ptr;
        }
      }
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto* point_field = renders[point_idx_];
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      k_int64 point_ts = INT64_MAX;
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          if (point_idx_ != -1) {  // for last point
            DatumPtr point_ptr = GetFieldDataPtr(ts_field, row_batch);
            point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
          }
          if (!(last_ts_ > point_ts && point_ts != INT64_MAX)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);
        if (point_idx_ != -1) {  // for last point
          DatumPtr point_ptr = GetFieldDataPtr(point_field, row_batch);
          point_ts = *reinterpret_cast<KTimestamp*>(point_ptr);
        }
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if ((last_line_ptr == nullptr || ts > last_ts_) &&
            (point_ts == INT64_MAX ||
             (ts <= point_ts && point_ts != INT64_MAX))) {
          last_ts_ = ts;
          last_line_ptr = ts_src_ptr;
        }
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
  k_int32 point_idx_ = -1;
};

////////////////////////// LastRowTSAggregate //////////////////////////

class LastRowTSAggregate : public AggregateFunc {
 public:
  LastRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));

    if (ts > last_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, last_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        last_line_ptr = ts_src_ptr;
      }
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }

    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, last_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (last_line_ptr == nullptr || ts > last_ts_) {
        last_ts_ = ts;
        last_line_ptr = ts_src_ptr;
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
};

////////////////////////// FirstAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class FirstAggregate : public AggregateFunc {
 public:
  FirstAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    if (is_dest_null) {
      //  first assign
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));
    if (ts < first_ts) {
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;
    k_uint16 str_length = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
        str_length = 0;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = input_chunk->GetData(row, arg_idx);
        }
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;
    k_uint16 str_length = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);

          if (IS_STRING_FAMILY) {
            std::memcpy(dest_ptr, &str_length, STRING_WIDE);
            std::memcpy(dest_ptr + STRING_WIDE, first_line_ptr, str_length);
          } else {
            std::memcpy(dest_ptr, first_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
        str_length = 0;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = GetFieldDataPtr(arg_field, row_batch);

          if (IS_STRING_FAMILY) {
            if (IsVarStringType(storage_type)) {
              str_length = arg_field->ValStrLength(first_line_ptr);
            } else {
              std::string_view str = std::string_view{first_line_ptr};
              str_length = static_cast<k_int16>(str.length());
            }
          }
        }
      }

      row_batch->NextLine();
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);

      if (IS_STRING_FAMILY) {
        std::memcpy(dest_ptr, &str_length, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, first_line_ptr, str_length);
      } else {
        std::memcpy(dest_ptr, first_line_ptr, len_);
      }
    }
  }


 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

////////////////////////// FirstRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class FirstRowAggregate : public AggregateFunc {
 public:
  FirstRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));

    if (ts < first_ts) {
      k_bool is_data_null = chunk->IsNull(line, arg_idx_[0]);
      if (is_data_null) {
        SetNull(bitmap, col_idx_);
      } else {
        DatumPtr src = chunk->GetData(line, arg_idx_[0]);
        std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
        SetNotNull(bitmap, col_idx_);
      }
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;
    k_uint16 str_length = 0;
    k_bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        } else {
          if (is_dest_null && target_row >= 0) {
            current_data_chunk_->SetNull(target_row, col_idx_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
        str_length = 0;
        is_dest_null = true;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        if (!input_chunk->IsNull(row, arg_idx)) {
          is_dest_null = false;
          first_line_ptr = input_chunk->GetData(row, arg_idx);
        } else {
          is_dest_null = true;
          first_line_ptr = nullptr;
        }
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    } else {
      if (is_dest_null && target_row >= 0) {
        current_data_chunk_->SetNull(target_row, col_idx_);
      }
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;
    k_uint16 str_length = 0;
    k_bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);

          if (IS_STRING_FAMILY) {
            std::memcpy(dest_ptr, &str_length, STRING_WIDE);
            std::memcpy(dest_ptr + STRING_WIDE, first_line_ptr, str_length);
          } else {
            std::memcpy(dest_ptr, first_line_ptr, len_);
          }
        } else {
          if (is_dest_null && target_row >= 0) {
            current_data_chunk_->SetNull(target_row, col_idx_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
        str_length = 0;
        is_dest_null = true;
      }

      char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        if (!(arg_field->isNullable() && arg_field->is_nullable())) {
          is_dest_null = false;
          first_line_ptr = GetFieldDataPtr(arg_field, row_batch);
          if (IS_STRING_FAMILY) {
            if (IsVarStringType(storage_type)) {
              str_length = arg_field->ValStrLength(first_line_ptr);
            } else {
              std::string_view str = std::string_view{first_line_ptr};
              str_length = static_cast<k_int16>(str.length());
            }
          }
        } else {
          is_dest_null = true;
          first_line_ptr = nullptr;
        }
      }

      row_batch->NextLine();
    }


    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);

      if (IS_STRING_FAMILY) {
        std::memcpy(dest_ptr, &str_length, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, first_line_ptr, str_length);
      } else {
        std::memcpy(dest_ptr, first_line_ptr, len_);
      }
    } else {
      if (is_dest_null && target_row >= 0) {
        current_data_chunk_->SetNull(target_row, col_idx_);
      }
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

////////////////////////// FirstTSAggregate //////////////////////////
class FirstTSAggregate : public AggregateFunc {
 public:
  FirstTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    if (is_dest_null) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));
    if (ts < first_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (first_line_ptr == nullptr || ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = ts_src_ptr;
        }
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      if (!(arg_field->isNullable() && arg_field->is_nullable())) {
        char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (first_line_ptr == nullptr || ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = ts_src_ptr;
        }
      }

      row_batch->NextLine();
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

////////////////////////// FirstRowTSAggregate //////////////////////////

class FirstRowTSAggregate : public AggregateFunc {
 public:
  FirstRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));

    if (ts < first_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        first_line_ptr = ts_src_ptr;
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      char* ts_src_ptr = GetFieldDataPtr(ts_field, row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (first_line_ptr == nullptr || ts < first_ts_) {
        first_ts_ = ts;
        first_line_ptr = ts_src_ptr;
      }

      row_batch->NextLine();
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

}  // namespace kwdbts
