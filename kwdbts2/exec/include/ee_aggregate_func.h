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
#include <unordered_set>
#include <limits>

#include "ee_combined_group_key.h"
#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "ee_common.h"

namespace kwdbts {

struct DistinctOpt {
  bool needDistinct;
  std::vector<roachpb::DataType>& data_types_;
  std::vector<k_uint32>& group_cols_;
};

class AggregateFunc {
 public:
  AggregateFunc(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : col_idx_(col_idx), len_(len) {
    arg_idx_.push_back(arg_idx);
  }

  virtual ~AggregateFunc() = default;

  virtual void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) = 0;

  void AddOrUpdate(DataChunkPtr& chunk, std::vector<DatumRowPtr>& dest_ptrs,
                   k_uint32 bitmap_offset, DistinctOpt& distinctOpt) {
    for (k_uint32 line = 0; line < chunk->Count(); ++line) {
      // Distinct Agg
      if (distinctOpt.needDistinct) {
        // group cols + agg cols Whether it already exists
        std::vector<k_uint32> all_cols(distinctOpt.group_cols_);
        all_cols.insert(all_cols.end(), arg_idx_.begin(), arg_idx_.end());

        if (!isDistinct(chunk, line, distinctOpt.data_types_, all_cols)) {
          continue;
        }
      }

      addOrUpdate(dest_ptrs[line], dest_ptrs[line] + bitmap_offset, chunk, line);
    }
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
        src_val = (k_double64)src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64)dest_ival;
      }

      if (src_val - dest_val > std::numeric_limits<double>::epsilon()) {
        return 1;
      } else if (dest_val - src_val > std::numeric_limits<double>::epsilon()) {
        return -1;
      }
    }

    return 0;
  }

  static void ConstructGroupKeys(DataChunkPtr& chunk, std::vector<k_uint32>& all_cols,
                                 std::vector<roachpb::DataType>& data_types, k_uint32 line,
                                 CombinedGroupKey& field_keys) {
    field_keys.Reserve(all_cols.size());
    for (auto idx : all_cols) {
      DatumPtr ptr = chunk->GetData(line, idx);
      bool is_null = chunk->IsNull(line, idx);

      roachpb::DataType col_type = data_types[idx];
      if (is_null) {
        field_keys.AddGroupKey(std::monostate(), col_type);
        continue;
      }
      switch (col_type) {
        case roachpb::DataType::BOOL: {
          k_bool val = *reinterpret_cast<k_bool*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::SMALLINT: {
          k_int16 val = *reinterpret_cast<k_int16*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::INT: {
          k_int32 val = *reinterpret_cast<k_int32*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          k_int64 val = *reinterpret_cast<k_int64*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::FLOAT: {
          k_float32 val = *reinterpret_cast<k_float32*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::DOUBLE: {
          k_double64 val = *reinterpret_cast<k_double64*>(ptr);
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          k_uint16 len = *reinterpret_cast<k_uint16*>(ptr);
          std::string val = std::string{ptr + sizeof(k_uint16), len};
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        case roachpb::DataType::DECIMAL: {
          k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
          if (is_double) {
            k_double64 val = *reinterpret_cast<k_double64*>(ptr + sizeof(k_bool));
            field_keys.AddGroupKey(val, roachpb::DataType::DOUBLE);
          } else {
            k_int64 val = *reinterpret_cast<k_int64*>(ptr + sizeof(k_bool));
            field_keys.AddGroupKey(val, roachpb::DataType::BIGINT);
          }
          break;
        }
        default:
          break;
      }
    }
  }

  k_bool isDistinct(DataChunkPtr& chunk, k_uint32 line, std::vector<roachpb::DataType>& data_types,
                    std::vector<k_uint32>& group_cols);

  virtual roachpb::DataType GetStorageType() const { return roachpb::DataType::UNKNOWN; }

  virtual k_uint32 GetStorageLength() const { return 0; }

 protected:
  std::unordered_set<CombinedGroupKey, GroupKeyHasher> seen;
  k_uint32 col_idx_;
  std::vector<k_uint32> arg_idx_;

  k_uint32 offset_{};  // The offset of the aggregate result in the bucket
  k_uint32 len_;       // The length of the aggregate result in the bucket
};

////////////////// AnyNotNullAggregate /////////////////////////

template<typename T>
class AnyNotNullAggregate : public AggregateFunc {
 public:
  AnyNotNullAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AnyNotNullAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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
};

////////////////////////// MaxAggregate //////////////////////////

template<typename T>
class MaxAggregate : public AggregateFunc {
 public:
  MaxAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~MaxAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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
      if constexpr(std::is_floating_point<T>::value) {
        if (src_val - dest_val > std::numeric_limits<T>::epsilon())  {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val > dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
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

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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
      if constexpr(std::is_floating_point<T>::value) {
        if (dest_val - src_val > std::numeric_limits<T>::epsilon())  {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val < dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
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

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

      if (dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val ||
          dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64)dest_val + (k_double64)src_val;
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
        src_val = (k_double64)src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64)dest_ival;
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
      k_int64 dest_val = (k_int64)src_val;
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
      k_double64 sum = dest_val + (k_double64)src_val;
      std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_double64));
    } else {
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
      if (dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val ||
          dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64)dest_val + (k_double64)src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      } else {
        k_int64 sum = dest_val + src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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
};

////////////////////////// AVGRowAggregate //////////////////////////
template<typename T>
class AVGRowAggregate : public AggregateFunc {
 public:
  AVGRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AVGRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

    k_int64 count_val = *reinterpret_cast<k_int64*>(dest + offset_+ sizeof(k_double64));
    ++count_val;
    std::memcpy(dest + offset_ + sizeof(k_double64), &count_val, sizeof(k_int64));
  }
};

////////////////////////// STDDEVRowAggregate //////////////////////////

class STDDEVRowAggregate : public AggregateFunc {
 public:
  STDDEVRowAggregate(k_uint32 col_idx, k_uint32 len) : AggregateFunc(col_idx, 0, len) {
  }

  ~STDDEVRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
    // do nothing temporarily.
  }
};

////////////////////////// LastAggregate //////////////////////////

class LastAggregate : public AggregateFunc {
 public:
  LastAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }


  ~LastAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    if (is_dest_null) {
      // first assign
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));
    if (ts > last_ts) {
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

 private:
  k_uint32 ts_idx_;
};

////////////////////////// LastRowAggregate //////////////////////////

class LastRowAggregate : public AggregateFunc {
 public:
  LastRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

////////////////////////// LastTSAggregate //////////////////////////
class LastTSAggregate : public AggregateFunc {
 public:
  LastTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));
    if (ts > last_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

 private:
  k_uint32 ts_idx_;
};

////////////////////////// LastRowTSAggregate //////////////////////////

class LastRowTSAggregate : public AggregateFunc {
 public:
  LastRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

////////////////////////// FirstAggregate //////////////////////////

class FirstAggregate : public AggregateFunc {
 public:
  FirstAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

////////////////////////// FirstRowAggregate //////////////////////////

class FirstRowAggregate : public AggregateFunc {
 public:
  FirstRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

////////////////////////// FirstTSAggregate //////////////////////////
class FirstTSAggregate : public AggregateFunc {
 public:
  FirstTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

////////////////////////// FirstRowTSAggregate //////////////////////////

class FirstRowTSAggregate : public AggregateFunc {
 public:
  FirstRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, DataChunkPtr& chunk, k_uint32 line) override {
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

 private:
  k_uint32 ts_idx_;
};

}  // namespace kwdbts
