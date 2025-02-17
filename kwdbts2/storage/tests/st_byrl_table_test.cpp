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

#include "st_byrl_table.h"

#include "engine.h"
#include "../../engine/tests/test_util.h"

using namespace kwdbts;  // NOLINT

const std::string kDbPath = "./test_ts_wal_table"; // NOLINT
const std::string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";   // NOLINT
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestRaftLoggedTable : public TestBigTableInstance {
 public:
  EngineOptions opt_;
  roachpb::CreateTsTable meta_;
  RaftLoggedTsTable* table_;
  KTableKey table_id_ = 10086;
  uint64_t range_group_id_ = 100;
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  std::vector<RangeGroup> ranges_;

  TestRaftLoggedTable() {
    ctx_ = &context_;
    setenv("KW_HOME", kDbPath.c_str(), 1);
    system(("rm -rf " + kDbPath + "/*").c_str());
    InitServerKWDBContext(ctx_);

    opt_.db_path = kDbPath;
    opt_.wal_level = 1;
    opt_.wal_buffer_size = 4;

    ConstructRoachpbTable(&meta_, "testTable", table_id_);
    ranges_.push_back({range_group_id_, 0});
    table_ = new RaftLoggedTsTable(ctx_, kDbPath, table_id_);
    Create(&meta_, ranges_);
  }

  void Create(roachpb::CreateTsTable* meta, const std::vector<RangeGroup>& range_groups) {
    KStatus s;

    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> metric_schema;
    for (int i = 0; i < meta->k_column_size(); i++) {
      const auto& col = meta->k_column(i);
      struct AttributeInfo col_var;
      s = TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      EXPECT_EQ(s, KStatus::SUCCESS);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.push_back(TagInfo{col.column_id(), col_var.type,
                                    static_cast<uint32_t>(col_var.length), 0,
                                    static_cast<uint32_t>(col_var.length),
                                    static_cast<TagType>(col_var.col_flag)});
      } else {
        metric_schema.push_back(std::move(col_var));
      }
    }

    s = table_->Create(ctx_, metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsEntityGroup> table_range;
    for (auto range_group : range_groups) {
      s = table_->CreateEntityGroup(ctx_, range_group, tag_schema, &table_range);
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
  }

  ~TestRaftLoggedTable() override {
    table_->DropAll(ctx_, false);
    delete table_;
    table_ = nullptr;
  }
};

// PutData
TEST_F(TestRaftLoggedTable, CreateCheckpoint) {
  EXPECT_EQ(table_->CreateCheckpoint(ctx_), KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, 10, p_len, start_ts, &meta_);
  TSSlice payload{data_value, p_len};
  uint16_t inc_entity_cnt;
  uint32_t inc_unordered_cnt;
  DedupResult dedup_result_{0, 0, 0, TSSlice {nullptr, 0}};
  EXPECT_EQ(table_->PutData(
              ctx_,
              range_group_id_,
              &payload, 1, 0,
              &inc_entity_cnt,
              &inc_unordered_cnt,
              &dedup_result_,
              DedupRule::OVERRIDE), KStatus::SUCCESS);
  EXPECT_EQ(table_->CreateCheckpoint(ctx_), KStatus::SUCCESS);

  delete[] data_value;
}
