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

#include "test_util.h"
#include "ts_table_snapshot.h"

using namespace kwdbts;  // NOLINT

class TestTsTableSnapshot : public ::testing::Test {
 public:
};


bool checkBlockItemEQ(const BlockItem& b1, const BlockItem& b2) {
  return (
    b1.alloc_row_count == b2.alloc_row_count &&
    b1.block_id == b2.block_id &&
    b1.entity_id == b2.entity_id &&
    b1.publish_row_count == b2.publish_row_count
  );
}

TEST_F(TestTsTableSnapshot, BlockBasedPartOneBlock) {
  std::string pstring = "this is primary key";
  TSSlice pkey = {const_cast<char*>(pstring.data()), pstring.length()};
  uint32_t blk_rows = 3456;
  std::list<TsBlockFullData*> res_list;
  TsBlockFullData cur_block;
  cur_block.rows = blk_rows;
  cur_block.block_item.alloc_row_count = 3;
  cur_block.block_item.publish_row_count = 4;
  cur_block.block_item.block_id = 6;
  cur_block.block_item.entity_id = 7;
  cur_block.block_item.read_only = true;

  size_t block_size = 234;
  char* block_mem = reinterpret_cast<char*>(malloc(block_size));
  memset(block_mem, 77, block_size);
  cur_block.col_block_addr.push_back({block_mem, block_size});

  res_list.push_back(new TsBlockFullData(cur_block));
  SnapshotPayloadDataBlockPart part(pkey, blk_rows, &res_list);
  TSSlice data = part.GenData();

  auto parsed_part = SnapshotPayloadDataBlockPart::ParseData(data);

  ASSERT_EQ(1, parsed_part.res_list->size());
  ASSERT_TRUE(checkBlockItemEQ(parsed_part.res_list->front()->block_item, cur_block.block_item));
  ASSERT_EQ(res_list.front()->col_block_addr.size(), 1);
  ASSERT_EQ(res_list.front()->col_block_addr.front().len, block_size);
  ASSERT_TRUE(memcmp(res_list.front()->col_block_addr.front().data, block_mem, block_size) == 0);

  free(data.data);
  delete res_list.front();
  free(block_mem);
}

TEST_F(TestTsTableSnapshot, BlockBasedPartManyBlocks) {
  std::string pstring = "this is primary key";
  TSSlice pkey = {const_cast<char*>(pstring.data()), pstring.length()};
  uint32_t blk_rows = 3456;
  uint32_t blkitem_num_in_data = 30;
  uint32_t col_nums = 30;
  std::list<TsBlockFullData*> res_list;
  size_t block_size = 234;
  uint8_t block_filled_num = 6;
  for (size_t i = 0; i < blkitem_num_in_data; i++){
    TsBlockFullData cur_block;
    cur_block.rows = blk_rows + i;
    cur_block.block_item.alloc_row_count = 3 + i;
    cur_block.block_item.publish_row_count = 4 + i;
    cur_block.block_item.block_id = 6 + i;
    cur_block.block_item.entity_id = 7 + i;
    cur_block.block_item.read_only = true;
    for (size_t j = 0; j < col_nums; j++) {
      char* block_mem = reinterpret_cast<char*>(malloc(block_size + j));
      memset(block_mem, block_filled_num + j, block_size + j);
      cur_block.col_block_addr.push_back({block_mem, block_size + j});
    }
    res_list.push_back(new TsBlockFullData(cur_block));
  }

  SnapshotPayloadDataBlockPart part(pkey, blk_rows, &res_list);
  TSSlice data = part.GenData();
  auto parsed_part = SnapshotPayloadDataBlockPart::ParseData(data);

  ASSERT_EQ(blkitem_num_in_data, parsed_part.res_list->size());
  
  auto res_iter = res_list.begin();
  for (auto& blkitem : *parsed_part.res_list) {
    ASSERT_EQ(blkitem->col_block_addr.size(), col_nums);
    ASSERT_TRUE(checkBlockItemEQ(blkitem->block_item, (*res_iter)->block_item));
    for (int j = 0; j < blkitem->col_block_addr.size(); j++) {
      ASSERT_TRUE(memcmp(blkitem->col_block_addr[j].data, (*res_iter)->col_block_addr[j].data, blkitem->col_block_addr[j].len) == 0);  
    }
    res_iter++;
  }

  for (auto& blkitem : res_list) {
    for (auto& block : blkitem->col_block_addr) {
      free(block.data);
    }
    delete blkitem;
  }
  free(data.data);
}

TEST_F(TestTsTableSnapshot, BlockBasedPartManyBlockWithVarValues) {
  std::string pstring = "this is primary key";
  TSSlice pkey = {const_cast<char*>(pstring.data()), pstring.length()};
  uint32_t blk_rows = 3456;
  uint32_t blkitem_num_in_data = 34;
  uint32_t col_nums = 32;
  uint32_t var_nums_per_blockitem = 165;
  std::list<TsBlockFullData*> res_list;
  size_t block_size = 234;
  uint8_t block_filled_num = 6;
  for (size_t i = 0; i < blkitem_num_in_data; i++) {
    TsBlockFullData cur_block;
    cur_block.rows = blk_rows + i;
    cur_block.block_item.alloc_row_count = 3 + i;
    cur_block.block_item.publish_row_count = 4 + i;
    cur_block.block_item.block_id = 6 + i;
    cur_block.block_item.entity_id = 7 + i;
    cur_block.block_item.read_only = true;
    for (size_t j = 0; j < col_nums; j++) {
      char* block_mem = reinterpret_cast<char*>(malloc(block_size + j));
      memset(block_mem, block_filled_num + j, block_size + j);
      cur_block.col_block_addr.push_back({block_mem, block_size + j});
    }
    for (size_t j = 0; j < var_nums_per_blockitem + i; j++) {
      std::shared_ptr<void> var_data(malloc(block_size + 30 + j + 2), free);
      memset(var_data.get(), block_filled_num + 10 + j, block_size + 30 + j + 2);
      KUint16(var_data.get()) = block_size + 30 + j;
      cur_block.var_col_values.push_back(var_data);
    }
    
    res_list.push_back(new TsBlockFullData(cur_block));
  }

  SnapshotPayloadDataBlockPart part(pkey, blk_rows, &res_list);
  TSSlice data = part.GenData();
  auto parsed_part = SnapshotPayloadDataBlockPart::ParseData(data);

  ASSERT_EQ(blkitem_num_in_data, parsed_part.res_list->size());

  auto res_iter = res_list.begin();
  for (auto& blkitem : *parsed_part.res_list) {
    ASSERT_EQ(blkitem->col_block_addr.size(), col_nums);
    ASSERT_TRUE(checkBlockItemEQ(blkitem->block_item, (*res_iter)->block_item));
    for (int j = 0; j < blkitem->col_block_addr.size(); j++) {
      ASSERT_TRUE(memcmp(blkitem->col_block_addr[j].data, (*res_iter)->col_block_addr[j].data, blkitem->col_block_addr[j].len) == 0);  
    }
    ASSERT_EQ(blkitem->var_col_values.size(), (*res_iter)->var_col_values.size());
    auto org_iter = (*res_iter)->var_col_values.begin();
    auto new_var_iter = blkitem->var_col_values.begin();
    for (; new_var_iter != blkitem->var_col_values.end(); org_iter++, new_var_iter++) {
      ASSERT_EQ(KUint16(org_iter->get()), KUint16(new_var_iter->get()));
      ASSERT_TRUE(memcmp(org_iter->get(), new_var_iter->get(), KUint16(new_var_iter->get()) + 2) == 0);  
    }
    res_iter++;
  }

  for (auto& blkitem : res_list) {
    for (auto& block : blkitem->col_block_addr) {
      free(block.data);
    }
    blkitem->var_col_values.clear();
    delete blkitem;
  }
  free(data.data);
}
