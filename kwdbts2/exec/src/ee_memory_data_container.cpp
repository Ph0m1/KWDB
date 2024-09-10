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


#include "ee_memory_data_container.h"

namespace kwdbts {

//////////////// MemRowContainer //////////////////////

KStatus MemRowContainer::Init() {
  if (data_chunk_.Initialize() < 0) {
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

void MemRowContainer::Sort() {
  // selection_ init
  selection_.resize(data_chunk_.Count());
  for (int i = 0; i < data_chunk_.Count(); i++) {
    selection_[i] = i;
  }

  // iterator
  auto it_begin = selection_.begin();
  auto it_end = selection_.end();

  // sort
  OrderColumnCompare cmp(this, order_info_);
  std::stable_sort(it_begin, it_end, cmp);
}

k_int32 MemRowContainer::NextLine() {
  // current_sel_idx_ at the end of selection
  if (!selection_.empty()) {
    if (current_sel_idx_ + 1 >= selection_.size()) {
      return -1;
    }
    ++current_sel_idx_;
    return selection_[current_sel_idx_];
  }
  return -1;
}

}  // namespace kwdbts
