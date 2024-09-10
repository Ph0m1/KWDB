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
#include <vector>
#include <string>
#include <variant>

#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"
#include "ee_data_chunk.h"

#ifndef UINT64_C
#define UINT64_C(c) c ## ULL
#endif

namespace kwdbts {

/*
    hash_value is called without qualification, so that overloads can be found
via ADL.

This is an extension to TR1
*/
// template<class T>
// inline void hash_combine(std::size_t& seed, const T& v) {
//   std::hash<T> hasher;
//   seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
// }


// FIXME: fix me if a better combined hash function is found
template<class T>
inline void hash_combine(std::size_t& h, const T& v) {
  std::hash<T> hasher;
  h = (hasher(v) * UINT64_C(0xbf58476d1ce4e5b9)) ^ h;
}

/*
    The GroupKey combination of the aggregate function is used as the key value
   of the hash table in the aggregation operator implementation
*/
class CombinedGroupKey {
  using GroupKeyValue = std::variant<std::monostate, k_bool, k_int16, k_int32, k_int64, k_float32, k_double64, std::string>;

 public:
  CombinedGroupKey() = default;

  CombinedGroupKey(const CombinedGroupKey& other) {
    group_key_values_ = other.group_key_values_;
    group_key_types_ = other.group_key_types_;
    group_key_size_ = other.group_key_size_;
  }

  void Reserve(k_uint32 sz) {
    group_key_values_.reserve(sz);
    group_key_types_.reserve(sz);
    group_key_size_ = 0;
  }

  // add key for group by
  void AddGroupKey(GroupKeyValue value, roachpb::DataType type) {
    group_key_values_.push_back(value);
    group_key_types_.push_back(type);
    group_key_size_++;
  }

  void AddGroupKey(DatumPtr ptr, roachpb::DataType type);

  bool is_null(k_uint32 i) const { return group_key_values_[i].index() == 0; }

  // overload ==
  bool operator==(const CombinedGroupKey& other) const;

  std::string to_string();

  /**
   * @brief whether the row of data chunk belongs to a new group
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols index of group columns
   * @param[in] col_types data type of all columns
   */
  bool IsNewGroup(const DataChunkPtr& chunk, k_uint32 row,
                  const std::vector<k_uint32>& group_cols,
                  const std::vector<roachpb::DataType>& col_types);

  // the real data，Otherwise, the data pointed to by the pointer may be
  // released
  std::vector<GroupKeyValue> group_key_values_;
  std::vector<roachpb::DataType> group_key_types_;
  k_int32 group_key_size_{-1};
};

// calculat hash value
struct GroupKeyHasher {
  std::size_t operator()(const CombinedGroupKey& gkc) const {
    std::size_t h = 13;
    for (int i = 0; i < gkc.group_key_size_; i++) {
      if (gkc.is_null(i)) {
        continue;
      }

      // calculate the hash value of each column
      switch (gkc.group_key_types_[i]) {
        case roachpb::DataType::BOOL: {
          if (const k_bool* pval = get_if<k_bool>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::SMALLINT: {
          if (const k_int16* pval = get_if<k_int16>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::INT: {
          if (const k_int32* pval = get_if<k_int32>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          if (const k_int64* pval = get_if<k_int64>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::FLOAT: {
          if (const k_float32* pval = get_if<k_float32>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::DOUBLE: {
          if (const k_double64* pval = get_if<k_double64>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          if (const std::string* pval = get_if<std::string>(&gkc.group_key_values_[i])) {
            hash_combine(h, *pval);
          }
          break;
        }
        default:
          // Handle unsupported data types here
          break;
      }
    }
    // printf("hash value: %lu\n", h);
    return h;
  }
};

}  // namespace kwdbts
