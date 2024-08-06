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

#include "ee_combined_group_key.h"
#include <numeric>
#include <limits>
#include <cmath>

namespace kwdbts {

/*
    to_string
*/
std::string CombinedGroupKey::to_string() {
  std::vector<std::string> vec;
  for (int i = 0; i < group_key_size_; i++) {
    if (is_null(i)) {
      vec.push_back("NULL");
    } else {
      switch (group_key_types_[i]) {
        case roachpb::DataType::BOOL:
          if (const k_bool* pval = get_if<k_bool>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::SMALLINT:
          if (const k_int16* pval = get_if<k_int16>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::INT:
          if (const k_int32* pval = get_if<k_int32>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT:
          if (const k_int64* pval = get_if<k_int64>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::FLOAT:
          if (const k_float32* pval = get_if<k_float32>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::DOUBLE:
          if (const k_double64* pval = get_if<k_double64>(&group_key_values_[i])) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        case roachpb::DataType::CHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY:
          if (const std::string* pval = get_if<std::string>(&group_key_values_[i])) {
            vec.push_back(*pval);
          }
          break;
        default:
          break;
      }
    }
  }
  std::string result = std::accumulate(vec.begin(), vec.end(), std::string(),
                                       [](string& ss, string& s) {
                                         return ss.empty() ? s : ss + "," + s;
                                       });
  return result;
}

bool CombinedGroupKey::operator==(const CombinedGroupKey& other) const {
  if (group_key_size_ != other.group_key_size_) {
    return false;
  }

  // compare the value of each col
  for (int i = 0; i < group_key_size_; i++) {
    if (group_key_types_[i] != other.group_key_types_[i]) {
      return false;
    }

    if (is_null(i) != other.is_null(i)) {
      // is_null indicates is inconsistent
      return false;
    } else if (is_null(i) && other.is_null(i)) {
      // all null
      continue;
    }

    switch (group_key_types_[i]) {
      case roachpb::DataType::BOOL: {
        const k_bool* left = get_if<k_bool>(&group_key_values_[i]);
        const k_bool* right = get_if<k_bool>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr || *left != *right) {
          return false;
        }
        break;
      }
      case roachpb::DataType::SMALLINT: {
        const k_int16* left = get_if<k_int16>(&group_key_values_[i]);
        const k_int16* right = get_if<k_int16>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr || *left != *right) {
          return false;
        }
        break;
      }
      case roachpb::DataType::INT: {
        const k_int32* left = get_if<k_int32>(&group_key_values_[i]);
        const k_int32* right = get_if<k_int32>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr || *left != *right) {
          return false;
        }
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::DATE: {
        const k_int64* left = get_if<k_int64>(&group_key_values_[i]);
        const k_int64* right = get_if<k_int64>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr || *left != *right) {
          return false;
        }
        break;
      }
      case roachpb::DataType::BIGINT: {
        const k_int64* left = get_if<k_int64>(&group_key_values_[i]);
        if (left == nullptr) {
          return false;
        }

        if (other.group_key_types_[i] == roachpb::DataType::BIGINT) {
          const k_int64* right = get_if<k_int64>(&other.group_key_values_[i]);
          if (right == nullptr || *left != *right) {
            return false;
          }
        } else {
          // DECIMAL type comparison. It is actually double in other object.
          const k_double64* right = get_if<k_double64>(&other.group_key_values_[i]);
          if (right == nullptr) {
            return false;
          }

          if (fabs((k_double64)(*left) - *right) > std::numeric_limits<double>::epsilon()) {
            return false;
          }
        }
        break;
      }
      case roachpb::DataType::FLOAT: {
        const k_float32* left = get_if<k_float32>(&group_key_values_[i]);
        const k_float32* right = get_if<k_float32>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr) {
          return false;
        }

        // float compare
        if (fabs(*left - *right) > std::numeric_limits<float>::epsilon()) {
          return false;
        }
        break;
      }
      case roachpb::DataType::DOUBLE: {
        const k_double64* left = get_if<k_double64>(&group_key_values_[i]);
        if (left == nullptr) {
          return false;
        }

        k_double64 rvalue = 0.0f;
        if (other.group_key_types_[i] == roachpb::DataType::DOUBLE) {
          const k_double64* right = get_if<k_double64>(&other.group_key_values_[i]);
          if (right == nullptr) {
            return false;
          }
          rvalue = *right;
        } else if (other.group_key_types_[i] == roachpb::DataType::BIGINT) {
          // DECIMAL type comparison. It is actually int64 in other object.
          const k_int64* right = get_if<k_int64>(&other.group_key_values_[i]);
          if (right == nullptr) {
            return false;
          }
          rvalue = (k_double64)(*right);
        }

        if (fabs(*left - rvalue) > std::numeric_limits<double>::epsilon()) {
          return false;
        }
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::VARBINARY: {
        const std::string* left = get_if<std::string>(&group_key_values_[i]);
        const std::string* right = get_if<std::string>(&other.group_key_values_[i]);
        if (left == nullptr || right == nullptr || *left != *right) {
          return false;
        }
        break;
      }
      default:
        break;
    }
  }
  return true;
}

}  // namespace kwdbts
