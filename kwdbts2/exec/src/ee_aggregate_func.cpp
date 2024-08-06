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

#include "ee_aggregate_func.h"

namespace kwdbts {

k_bool AggregateFunc::isDistinct(DataChunkPtr& chunk, k_uint32 line, std::vector<roachpb::DataType>& data_types,
                                 std::vector<k_uint32>& group_cols) {
  // group col + agg col
  std::vector<k_uint32> all_cols(group_cols);
  all_cols.insert(all_cols.end(), arg_idx_.begin(), arg_idx_.end());

  // CominbedGroupKey
  CombinedGroupKey field_keys;
  AggregateFunc::ConstructGroupKeys(chunk, all_cols, data_types, line, field_keys);

  // find the same key
  if (seen.find(field_keys) != seen.end()) {
    return false;
  }

  seen.emplace(field_keys);

  return true;
}

}   // namespace kwdbts
