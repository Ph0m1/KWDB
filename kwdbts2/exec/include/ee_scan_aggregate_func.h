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
#include <set>
#include <unordered_set>
#include <map>
#include <limits>

#include "ee_combined_group_key.h"
#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

struct DataSource {
  char* src_ptr{nullptr};
  bool is_null{false};
  k_int16 str_length{-1};
};

class ScanAggregateFunc {
 public:
  ScanAggregateFunc(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : col_idx_(col_idx), len_(len) {
    arg_idx_.push_back(arg_idx);
  }

  virtual ~ScanAggregateFunc() = default;

  virtual void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                            std::vector<std::vector<DataSource>>& source,
                            k_uint32 start_line, k_uint32 end_line) = 0;

  inline k_uint32 getArgColIdx() {
    return arg_idx_[0];
  }

  inline bool hasDistinct() const {
    return distinct_;
  }

  void setDistinct(const std::set<k_uint32>& group_cols, k_int32 time_bucket_bol, k_uint64 time_bucket_interval) {
    distinct_ = true;
    time_bucket_col_ = time_bucket_bol;
    time_bucket_interval_ = time_bucket_interval;
    distinct_cols_.insert(distinct_cols_.end(), group_cols.begin(), group_cols.end());
    distinct_cols_.insert(distinct_cols_.end(), arg_idx_.begin(), arg_idx_.end());
  }

  static k_bool IsNull(const char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    return (bitmap[index] & mask) == 0;
  }

  [[nodiscard]] k_bool isDistinct(Field** renders, std::vector<roachpb::DataType>& data_types) {
    if (hasDistinct()) {
      CombinedGroupKey field_keys;
      ConstructGroupKeys(renders, distinct_cols_, field_keys, data_types);

      // found the same key
      if (seen.find(field_keys) != seen.end()) {
        return false;
      }
      seen.emplace(field_keys);
    }
    return true;
  }

  static KTimestampTz construct(Field* filed, k_uint64 time_bucket_interval) {
    auto time_bucket_field = dynamic_cast<FieldFuncTimeBucket*>(filed);
    if (time_bucket_field != nullptr) {
      KTimestampTz original_timestamp = time_bucket_field->getOriginalTimestamp();
      return original_timestamp - original_timestamp % time_bucket_interval;
    }
    return 0;
  }

  void ConstructGroupKeys(Field** renders, std::vector<k_uint32>& all_cols,
                          CombinedGroupKey& field_keys, std::vector<roachpb::DataType>& data_types) {
    for (auto idx : all_cols) {
      Field* field = renders[idx];
      bool is_null = field->is_nullable();
      auto ptr = field->get_ptr();

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
          // TODO(liuwei): need distinguish k_uint64、k_int64
          k_int64 val;
          if (idx == time_bucket_col_) {
            val = construct(field, time_bucket_interval_);
          } else {
            val = *reinterpret_cast<k_int64*>(ptr);
          }

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
        case roachpb::DataType::BINARY:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::VARBINARY: {
          std::string val = std::string{ptr};
          field_keys.AddGroupKey(val, col_type);
          break;
        }
        default:
          break;
      }
    }
  }

 protected:
  std::unordered_set<CombinedGroupKey, GroupKeyHasher> seen;
  k_uint32 col_idx_;
  std::vector<k_uint32> arg_idx_;

  k_uint32 len_;  // The length of the aggregate result in the data source

  bool distinct_{false};
  std::vector<k_uint32> distinct_cols_;
  k_int32 time_bucket_col_{-1};
  k_uint64 time_bucket_interval_{0};
};

////////////////// AnyNotNullAggregate /////////////////////////

template<typename T>
class AnyNotNullScanAggregate : public ScanAggregateFunc {
 public:
  AnyNotNullScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : ScanAggregateFunc(col_idx, arg_idx, len) {
  }

  ~AnyNotNullScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    if (!is_dest_null) {
      return;
    }

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }

      char* src_ptr = source[arg_idx][i].src_ptr;
      if constexpr (std::is_same_v<T, std::string>) {
        k_int16 len = source[arg_idx][i].str_length;
        if (len < 0) {
          std::string_view str = std::string_view{src_ptr};
          len = static_cast<k_int16>(str.length());
        }
        std::memcpy(dest_ptr, &len, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
      } else {
        std::memcpy(dest_ptr, src_ptr, len_);
      }

      dest->SetNotNull(line, col_idx_);
      break;
    }
  }
};

////////////////////////// MaxAggregate //////////////////////////

template<typename T>
class MaxScanAggregate : public ScanAggregateFunc {
 public:
  MaxScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : ScanAggregateFunc(col_idx, arg_idx, len) {
  }

  ~MaxScanAggregate() override = default;

  void handleNumber(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    T max_val = is_dest_null ?
      std::numeric_limits<T>::lowest() : *reinterpret_cast<T*>(dest_ptr);

    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      T src_val = *reinterpret_cast<T*>(arg_source[i].src_ptr);
      if constexpr(std::is_floating_point<T>::value) {
        if (src_val - max_val > std::numeric_limits<T>::epsilon())  {
          max_val = src_val;
        }
      } else {
        if (src_val > max_val) {
          max_val = src_val;
        }
      }

      if (is_dest_null) {
        is_dest_null = false;
      }
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);
      std::memcpy(dest_ptr, &max_val, len_);
    }
  }

  void handleString(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    std::string_view max_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE};
    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      char* src_ptr = arg_source[i].src_ptr;
      k_int16 len = arg_source[i].str_length;
      std::string_view src_val = len < 0 ?
        std::string_view{src_ptr} : std::string_view{src_ptr, static_cast<k_uint32>(len)};

      if (is_dest_null) {
        max_val = src_val;
        is_dest_null = false;
      } else if (src_val.compare(max_val) > 0) {
        max_val = src_val;
      }
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);
      k_uint16 len = max_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, max_val.data(), len);
    }
  }

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    if constexpr (std::is_same_v<T, std::string>) {
      handleString(dest, line, source[arg_idx], start_line, end_line);
    } else {
      handleNumber(dest, line, source[arg_idx], start_line, end_line);
    }
  }
};

////////////////////////// MinAggregate //////////////////////////
template<typename T>
class MinScanAggregate : public ScanAggregateFunc {
 public:
  MinScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : ScanAggregateFunc(col_idx, arg_idx, len) {
  }

  ~MinScanAggregate() override = default;

  void handleNumber(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    T min_val = is_dest_null ?
      std::numeric_limits<T>::max() : *reinterpret_cast<T*>(dest_ptr);

    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      T src_val = *reinterpret_cast<T*>(arg_source[i].src_ptr);
      if constexpr(std::is_floating_point<T>::value) {
        if (min_val - src_val > std::numeric_limits<T>::epsilon())  {
          min_val = src_val;
        }
      } else {
        if (src_val < min_val) {
          min_val = src_val;
        }
      }

      if (is_dest_null) {
        is_dest_null = false;
      }
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);
      std::memcpy(dest_ptr, &min_val, len_);
    }
  }

  void handleString(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    std::string_view min_val = is_dest_null ? "" : std::string_view{dest_ptr + STRING_WIDE};

    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      char* src_ptr = arg_source[i].src_ptr;
      k_int16 len = arg_source[i].str_length;
      std::string_view src_val = len < 0 ?
        std::string_view{src_ptr} : std::string_view{src_ptr, static_cast<k_uint32>(len)};

      if (is_dest_null) {
        min_val = src_val;
        is_dest_null = false;
      } else if (src_val.compare(min_val) < 0) {
        min_val = src_val;
      }
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);
      k_uint16 len = min_val.length();
      std::memcpy(dest_ptr, &len, STRING_WIDE);
      std::memcpy(dest_ptr + STRING_WIDE, min_val.data(), len);
    }
  }

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    if constexpr (std::is_same_v<T, std::string>) {
      handleString(dest, line, source[arg_idx], start_line, end_line);
    } else {
      handleNumber(dest, line, source[arg_idx], start_line, end_line);
    }
  }
};

////////////////////////// SumAggregate //////////////////////////

template<typename T_SRC, typename T_DEST>
class SumScanAggregate : public ScanAggregateFunc {
 public:
  SumScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : ScanAggregateFunc(col_idx, arg_idx, len) {
  }

  ~SumScanAggregate() override = default;

  void handleDouble(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);
    k_double64 sum_val = 0.0;

    if (!is_dest_null) {
      sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
    }

    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      T_SRC src_val = *reinterpret_cast<T_SRC*>(arg_source[i].src_ptr);
      sum_val += src_val;
      is_dest_null = false;
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);
      std::memcpy(dest_ptr, &sum_val, sizeof(k_double64));
    }
  }

  void handleInteger(DataChunkPtr& dest, k_uint32 line, std::vector<DataSource>& arg_source,
                      k_uint32 start_line, k_uint32 end_line) {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;

    if (!is_dest_null) {
      if (dest_is_double) {
        sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
      } else {
        sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
      }
    }

    for (int i = start_line; i < end_line; ++i) {
      if (arg_source[i].is_null) {
        continue;
      }

      T_SRC src_val = *reinterpret_cast<T_SRC*>(arg_source[i].src_ptr);

      if (dest_is_double) {
        sum_val_d += src_val;
      } else {
        sum_val_i += src_val;
        if (sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val ||
              sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val) {
          dest_is_double = true;
          sum_val_d = static_cast<k_double64>(sum_val_i);
        }
      }
      is_dest_null = false;
    }

    if (!is_dest_null) {
      dest->SetNotNull(line, col_idx_);

      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      handleDouble(dest, line, source[arg_idx], start_line, end_line);
    } else {
      // input type: int16/int32/int64
      handleInteger(dest, line, source[arg_idx], start_line, end_line);
    }
  }
};

////////////////////////// CountAggregate //////////////////////////

/*
    CountAggregate's return type is BIGINT
*/
class CountScanAggregate : public ScanAggregateFunc {
 public:
  CountScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : ScanAggregateFunc(col_idx, arg_idx, len) {
  }

  ~CountScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int64 val = 0;
    if (!is_dest_null) {
      val = *reinterpret_cast<k_int64*>(dest_ptr);
    }

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }
      ++val;
    }

    if (is_dest_null) {
      dest->SetNotNull(line, col_idx_);
    }
    std::memcpy(dest_ptr, &val, len_);
  }
};

////////////////////////// CountRowAggregate //////////////////////////

/*
    CountRowAggregate return type BIGINT
*/
class CountRowScanAggregate : public ScanAggregateFunc {
 public:
  CountRowScanAggregate(k_uint32 col_idx, k_uint32 len) : ScanAggregateFunc(col_idx, 0, len) {
  }

  ~CountRowScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int64 val = 0;
    if (!is_dest_null) {
      val = *reinterpret_cast<k_int64*>(dest_ptr);
    }

    for (int i = start_line; i < end_line; ++i) {
      ++val;
    }

    if (is_dest_null) {
      dest->SetNotNull(line, col_idx_);
    }
    std::memcpy(dest_ptr, &val, len_);
  }
};

////////////////////////// AVGRowAggregate //////////////////////////

class AVGRowScanAggregate : public ScanAggregateFunc {
 public:
  AVGRowScanAggregate(k_uint32 col_idx, k_uint32 len) : ScanAggregateFunc(col_idx, 0, len) {
  }

  ~AVGRowScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    // not implemented
  }
};

////////////////////////// STDDEVRowAggregate //////////////////////////

class STDDEVRowScanAggregate : public ScanAggregateFunc {
 public:
  STDDEVRowScanAggregate(k_uint32 col_idx, k_uint32 len) : ScanAggregateFunc(col_idx, 0, len) {
  }

  ~STDDEVRowScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    // not implemented
  }
};

////////////////////////// LastAggregate //////////////////////////

template<bool IS_STRING_FAMILY>
class LastScanAggregate : public ScanAggregateFunc {
 public:
  LastScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 last_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MIN;
    }
    KTimestamp last_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts > last_ts) {
        last_ts = ts;
        last_line = i;
      }
    }

    // write last result to dest
    if (last_line >= 0) {
      timestamps_[dest_ptr] = last_ts;

      char* src_ptr = source[arg_idx][last_line].src_ptr;
      if (IS_STRING_FAMILY) {
        k_int16 len = source[arg_idx][last_line].str_length;
        if (len < 0) {
          std::string_view str = std::string_view{src_ptr};
          len = static_cast<k_int16>(str.length());
        }
        std::memcpy(dest_ptr, &len, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
      } else {
        std::memcpy(dest_ptr, src_ptr, len_);
      }

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp>
      timestamps_;  // Different buckets correspond to different timestamps
  k_uint32 ts_idx_;
};

////////////////////////// LastTSAggregate //////////////////////////

class LastTSScanAggregate : public ScanAggregateFunc {
 public:
  LastTSScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastTSScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 last_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MIN;
    }
    KTimestamp last_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts > last_ts) {
        last_ts = ts;
        last_line = i;
      }
    }

    // write last result to dest
    if (last_line >= 0) {
      timestamps_[dest_ptr] = last_ts;
      std::memcpy(dest_ptr, source[ts_idx_][last_line].src_ptr, len_);

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp> timestamps_;
  k_uint32 ts_idx_;
};

////////////////////////// LastRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY>
class LastRowScanAggregate : public ScanAggregateFunc {
 public:
  LastRowScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 last_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MIN;
    }
    KTimestamp last_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts > last_ts) {
        last_ts = ts;
        last_line = i;
      }
    }

    if (last_line >= 0) {
      timestamps_[dest_ptr] = last_ts;

      if (source[arg_idx][last_line].is_null) {
        dest->SetNull(line, col_idx_);
      } else {
        char* src_ptr = source[arg_idx][last_line].src_ptr;
        if (IS_STRING_FAMILY) {
          k_int16 len = source[arg_idx][last_line].str_length;
          if (len < 0) {
            std::string_view str = std::string_view{src_ptr};
            len = static_cast<k_int16>(str.length());
          }
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
        } else {
          std::memcpy(dest_ptr, src_ptr, len_);
        }

        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp> timestamps_;
  k_uint32 ts_idx_;
};

////////////////////////// LastRowTSAggregate //////////////////////////

class LastRowTSScanAggregate : public ScanAggregateFunc {
 public:
  LastRowTSScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowTSScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 last_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MIN;
    }
    KTimestamp last_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts > last_ts) {
        last_ts = ts;
        last_line = i;
      }
    }

    if (last_line >= 0) {
      timestamps_[dest_ptr] = last_ts;
      std::memcpy(dest_ptr, source[ts_idx_][last_line].src_ptr, len_);

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  k_uint32 ts_idx_;
  map<DatumRowPtr, KTimestamp> timestamps_;
};

////////////////////////// FirstAggregate //////////////////////////

template<bool IS_STRING_FAMILY>
class FirstScanAggregate : public ScanAggregateFunc {
 public:
  FirstScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 first_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MAX;
    }
    KTimestamp first_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts < first_ts) {
        first_ts = ts;
        first_line = i;
      }
    }

    if (first_line >= 0) {
      timestamps_[dest_ptr] = first_ts;

      char* src_ptr = source[arg_idx][first_line].src_ptr;
      if (IS_STRING_FAMILY) {
        k_int16 len = source[arg_idx][first_line].str_length;
        if (len < 0) {
          std::string_view str = std::string_view{src_ptr};
          len = static_cast<k_int16>(str.length());
        }
        std::memcpy(dest_ptr, &len, STRING_WIDE);
        std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
      } else {
        std::memcpy(dest_ptr, src_ptr, len_);
      }

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp> timestamps_;
  k_uint32 ts_idx_;
};


////////////////////////// FirstTSAggregate //////////////////////////

class FirstTSScanAggregate : public ScanAggregateFunc {
 public:
  FirstTSScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstTSScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 first_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MAX;
    }
    KTimestamp first_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      if (source[arg_idx][i].is_null) {
        continue;
      }

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts < first_ts) {
        first_ts = ts;
        first_line = i;
      }
    }

    if (first_line >= 0) {
      timestamps_[dest_ptr] = first_ts;
      std::memcpy(dest_ptr, source[ts_idx_][first_line].src_ptr, len_);

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp> timestamps_;
  k_uint32 ts_idx_;
};

////////////////////////// FirstRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY>
class FirstRowScanAggregate : public ScanAggregateFunc {
 public:
  FirstRowScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 first_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MAX;
    }
    KTimestamp first_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts < first_ts) {
        first_ts = ts;
        first_line = i;
      }
    }

    if (first_line >= 0) {
      timestamps_[dest_ptr] = first_ts;

      if (source[arg_idx][first_line].is_null) {
        dest->SetNull(line, col_idx_);
      } else {
        char* src_ptr = source[arg_idx][first_line].src_ptr;
        if (IS_STRING_FAMILY) {
          k_int16 len = source[arg_idx][first_line].str_length;
          if (len < 0) {
            std::string_view str = std::string_view{src_ptr};
            len = static_cast<k_int16>(str.length());
          }
          std::memcpy(dest_ptr, &len, STRING_WIDE);
          std::memcpy(dest_ptr + STRING_WIDE, src_ptr, len);
        } else {
          std::memcpy(dest_ptr, src_ptr, len_);
        }

        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  map<DatumRowPtr, KTimestamp> timestamps_;
  k_uint32 ts_idx_;
};

////////////////////////// LastRowTSAggregate //////////////////////////

class FirstRowTSScanAggregate : public ScanAggregateFunc {
 public:
  FirstRowTSScanAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      ScanAggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowTSScanAggregate() override = default;

  void addOrUpdate(DataChunkPtr& dest, k_uint32 line,
                    std::vector<std::vector<DataSource>>& source,
                    k_uint32 start_line, k_uint32 end_line) override {
    k_uint32 arg_idx = arg_idx_[0];
    auto dest_ptr = dest->GetData(line, col_idx_);
    k_bool is_dest_null = dest->IsNull(line, col_idx_);

    k_int32 first_line = -1;

    if (timestamps_.find(dest_ptr) == timestamps_.end()) {
      timestamps_[dest_ptr] = INT64_MAX;
    }
    KTimestamp first_ts = timestamps_[dest_ptr];

    for (int i = start_line; i < end_line; ++i) {
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(source[ts_idx_][i].src_ptr);
      if (ts < first_ts) {
        first_ts = ts;
        first_line = i;
      }
    }

    if (first_line >= 0) {
      timestamps_[dest_ptr] = first_ts;
      std::memcpy(dest_ptr, source[ts_idx_][first_line].src_ptr, len_);

      if (is_dest_null) {
        dest->SetNotNull(line, col_idx_);
      }
    }
  }

 private:
  k_uint32 ts_idx_;
  map<DatumRowPtr, KTimestamp> timestamps_;
};

}  // namespace kwdbts
