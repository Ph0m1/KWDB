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

#include <st_wal_table.h>
#include "engine.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT big table dir
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{101, 0};

class TestEngineWAL : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestEngineWAL() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    // delete ts_engine_;
    // ts_engine_ = nullptr;
    setenv("KW_HOME", kDbPath.c_str(), 1);
    // clear data dir
    system(("rm -rf " + kDbPath + "/*").c_str());

  }

  ~TestEngineWAL() {
    // CLOSE engine
    delete ts_engine_;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  k_uint32 GetTableRows(KTableKey table_id, const std::vector<RangeGroup>& rgs, KwTsSpan ts_span, uint16_t entity_num = 1) {
    k_uint32 total_rows = 0;
    std::shared_ptr<TsTable> table;
    ts_engine_->GetTsTable(ctx_, table_id, table);

    for (auto& rg : rgs) {
      std::shared_ptr<TsEntityGroup> entity_group;
      KStatus s = table->GetEntityGroup(ctx_, rg.range_group_id, &entity_group);
      std::vector<k_uint32> scan_cols = {0, 1, 2};
      std::vector<Sumfunctype> scan_agg_types;
      k_uint32 entity_id = 1;
      SubGroupID group_id = 1;
      while (entity_id <= entity_num) {
        TsIterator* iter1;
        entity_group->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, scan_cols, scan_cols, scan_agg_types, &iter1, entity_group);
        total_rows += GetIterRows(iter1, scan_cols.size());
        entity_id++;
        delete iter1;
      }
    }

    return total_rows;
  }

  k_uint32 GetIterRows(TsIterator* iter1, k_uint32 scancol_num) {
    ResultSet res{scancol_num};
    k_uint32 ret_cnt;
    k_uint32 total_rows = 0;
    bool is_finished = false;
    do {
      KStatus s = iter1->Next(&res, &ret_cnt, &is_finished);
      if (s != KStatus::SUCCESS) {
        return 0;
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    return total_rows;
  }

  std::string getPrimaryTag(roachpb::CreateTsTable meta, TSSlice payload) {
    vector<AttributeInfo> schema;
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i==0);
      if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
        schema.push_back(std::move(col_var));
      }
    }
    Payload pd(schema, payload);
    std::string primary_tag(pd.GetPrimaryTag().data, pd.GetPrimaryTag().len);

    return primary_tag;
  }

  // header info
  int row_num_ = 5;
};

TEST_F(TestEngineWAL, create) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, CreateSomeTables) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  int table_num = 10;
  KTableKey cur_table_id = 123456789;
  std::vector<RangeGroup> ranges{kTestRange};
  for (size_t i = 0; i < table_num; i++) {
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id + i);
    s = ts_engine_->CreateTsTable(ctx_, cur_table_id + i, &meta, ranges);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  std::shared_ptr<TsTable> ts_table;
  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, ts_table);
    EXPECT_EQ(s, KStatus::SUCCESS);
    EXPECT_TRUE(ts_table->IsExist());
    EXPECT_EQ(ts_table->GetTableId(), cur_table_id + i);

    s = ts_engine_->DropTsTable(ctx_, cur_table_id + i);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }
  for (size_t i = 0; i < table_num; i++) {
    s = ts_engine_->GetTsTable(ctx_, cur_table_id + i, ts_table);
    EXPECT_EQ(s, KStatus::FAIL);
    EXPECT_TRUE(ts_table == nullptr);
  }

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, insert) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, mtr_id, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, insertRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, mtr_id, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), row_num_);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {start_ts, start_ts + 1000}), 0);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, checkpoint) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // tags_table.release();

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteData) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, 1000, &meta);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  // delete
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t count = 0;
  KwTsSpan span{1, ts + 10,};
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {span}, &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num_);  // delete 12 rows

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  // after delete
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {ts, ts + 10000}), row_num_ - 2);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteDataRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp ts = TestBigTableInstance::iot_interval_ * 3 * 1000;  // millisecond
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, ts, &meta);
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, 1000, &meta);
  TSSlice payload{data_value, p_len};
  TSSlice payload2{data_value2, p_len2};
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  // delete
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t count = 0;
  KwTsSpan span{1, ts + 10,};
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {span}, &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2 + row_num_);
  // after delete
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ - 2);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, ts + 10000}), row_num_ * 2);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteEntities) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // delete entities
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t del_cnt;
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}), 0);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, DeleteEntitiesRollback) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

  // insert
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // delete entities
  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  uint64_t del_cnt;
  auto pt = getPrimaryTag(meta, payload);
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrRollback(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}), row_num_);

  delete[] data_value;
  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, updateTag) {
  opts_.wal_level = 2;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts1 = 3600;
  int cnt = 20;
  char* data_value = nullptr;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 0, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload1, 1, 0, &dedup_result), KStatus::SUCCESS);
    delete[] data_value;
  }
  // update
  data_value = nullptr;
  for (int i = 0; i < cnt; i++) {
    k_uint32 p_len = 0;
    data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts1 + i * 100, &meta, 10, 1, false);
    TSSlice payload1{data_value, p_len};
    ASSERT_EQ(ts_engine_->PutEntity(ctx_, cur_table_id, kTestRange.range_group_id, &payload1, 1, 0), KStatus::SUCCESS);
    delete[] data_value;
  }

  // tagiterator
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  std::vector<EntityResultIndex> entity_id_list;
  std::vector<k_uint32> scan_tags = {1, 2};
  TagIterator *iter;
  ASSERT_EQ(ts_table->GetTagIterator(ctx_, scan_tags, &iter, 1), KStatus::SUCCESS);

  ResultSet res{(k_uint32) scan_tags.size()};
  k_uint32 fetch_total_count = 0;
  k_uint64 ptag = 0;
  k_uint32 count = 0;
  k_uint32 all_idx = 0;
  k_uint32 tagval = 0;
  do {
    ASSERT_EQ(iter->Next(&entity_id_list, &res, &count), KStatus::SUCCESS);
    if (count == 0) {
      break;
    }
    // check entity_id
    for (int idx = 0; idx < entity_id_list.size(); idx++) {
      ASSERT_EQ(entity_id_list[idx].mem != nullptr, true);
      memcpy(&ptag, entity_id_list[idx].mem, sizeof(ptag));
      ASSERT_EQ(ptag, start_ts1+(idx+fetch_total_count)*100);
    }
    // check tag_column
    for (int tag_idx = 0; tag_idx < scan_tags.size(); tag_idx++) {
      for (int i = 0; i < res.data[tag_idx][0]->count; i++) {
        if (tag_idx == 0) {
          memcpy(&tagval, res.data[tag_idx][0]->mem + i * (sizeof(k_uint32) + k_per_null_bitmap_size) + k_per_null_bitmap_size, sizeof(tagval));
          ASSERT_EQ(tagval, start_ts1 + 1 + (i+fetch_total_count) * 100);
        } else {
          // var
          char* rec_ptr = static_cast<char*>(res.data[tag_idx][0]->getVarColData(i));
          ASSERT_EQ(rec_ptr, std::to_string(start_ts1 + 1 + (i)*100));
        }
      }
    }
    fetch_total_count += count;
    entity_id_list.clear();
    res.clear();
  }while(count);
  ASSERT_EQ(fetch_total_count, cnt);
  iter->Close();
  delete iter;
}

TEST_F(TestEngineWAL, EngineApi) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len};
  auto pt = getPrimaryTag(meta, payload);
  auto pt2 = getPrimaryTag(meta, payload2);
  vector<TSSlice> payloads;
  payloads.push_back(payload);
  payloads.push_back(payload2);

  TS_LSN mtr_id = 0;
  uint64_t range_id = 1;
  uint64_t index = 1;
  s = ts_engine_->TSMtrBegin(ctx_, cur_table_id, kTestRange.range_group_id, range_id, index, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // insert
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, payloads.data(), 2, mtr_id, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ * 2);

  uint64_t count = 0;
  KwTsSpan span{1, start_ts + 10,};

  s = ts_engine_->DeleteData(ctx_, cur_table_id, kTestRange.range_group_id, pt, {span}, &count, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_EQ(count, 2);
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ * 2 - 2);

  // delete entities
  uint64_t del_cnt;
  s = ts_engine_->DeleteEntities(ctx_, cur_table_id, kTestRange.range_group_id, {pt2}, &del_cnt, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);
  EXPECT_EQ(del_cnt, row_num_);

  s = ts_engine_->TSMtrCommit(ctx_, cur_table_id, kTestRange.range_group_id, mtr_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->FlushBuffer(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  // check result
  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_ - 2);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {1, start_ts + 10000}, 2), row_num_);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
  delete[] data_value;
  delete[] data_value2;
}

TEST_F(TestEngineWAL, ShiftWalLevel) {
  opts_.wal_level = 0;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id);

  std::vector<RangeGroup> ranges{kTestRange};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 1000, &meta);
  TSSlice payload2{data_value2, p_len};
  char* data_value3 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 2000, &meta);
  TSSlice payload3{data_value3, p_len};
  char* data_value4 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts + 3000, &meta);
  TSSlice payload4{data_value4, p_len};

  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  MakeDirectory(kDbPath + "/test_db_");
  MakeDirectory(kDbPath + "/test111");
  MakeDirectory(kDbPath + "/_111");

  opts_.wal_level = 1;

  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable meta2;
  KTableKey cur_table_id2 = 12345;
  ConstructRoachpbTable(&meta2, "testTable", cur_table_id2);
  RangeGroup test_range2{201, 0};
  std::vector<RangeGroup> ranges2{test_range2};

  s = ts_engine_->CreateTsTable(ctx_, cur_table_id2, &meta2, ranges2);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->PutData(ctx_, cur_table_id2, test_range2.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  opts_.wal_level = 2;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  std::vector<KTableKey> table_id_list;
  ts_engine_->GetTableIDList(ctx_, table_id_list);
  EXPECT_EQ(table_id_list.size(), 2);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_ * 2);
  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload3, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  opts_.wal_level = 0;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}), row_num_ * 3);
  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}), row_num_);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload4, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  s = ts_engine_->PutData(ctx_, cur_table_id2, test_range2.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->CreateCheckpoint(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id2, ranges2, {0, start_ts + 10000}, 2), row_num_ * 2);

  TSEngine* ts_engine2;
  opts_.wal_level = 1;
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine2);
  EXPECT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(GetTableRows(cur_table_id, ranges, {0, start_ts + 10000}, 3), row_num_ * 4);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine2);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine2 = nullptr;

  delete[] data_value;
  delete[] data_value2;
  delete[] data_value3;
  delete[] data_value4;
}

TEST_F(TestEngineWAL, TsxAlterColumn) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value;

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);
  int column_size = meta.k_column_size();

  // add column
  roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMP);
  column->set_storage_len(8);
  column->set_column_id(column_size);
  column->set_name("column" + std::to_string(meta.k_column_size()));
  string err_msg;
  size_t col_size = column->ByteSizeLong();
  auto* buffer = new char[col_size];
  column->SerializeToArray(buffer, col_size);

  TSSlice column_slice{buffer, col_size};
  ASSERT_EQ(column->ParseFromArray(column_slice.data, column_slice.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer;
  // add tag
  roachpb::KWDBKTSColumn* column1 = meta.mutable_k_column()->Add();
  column1->set_col_type(roachpb::KWDBKTSColumn_ColumnType::
                       KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1->set_storage_type(roachpb::DataType::TIMESTAMP);
  column1->set_storage_len(8);
  column1->set_column_id(column_size + 1);
  column1->set_name("column" + std::to_string(column_size + 1));
  size_t col_size1 = column1->ByteSizeLong();
  auto* buffer1 = new char[col_size1];
  column1->SerializeToArray(buffer1, col_size1);

  TSSlice column_slice1{buffer1, col_size1};
  ASSERT_EQ(column1->ParseFromArray(column_slice1.data, column_slice1.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice1, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer1;

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  meta.mutable_k_column()->DeleteSubrange(2, 1);
  size_t col_size2 = column2.ByteSizeLong();
  auto* buffer2 = new char[col_size2];
  column2.SerializeToArray(buffer2, col_size2);
  TSSlice column_slice2{buffer2, col_size2};
  ASSERT_EQ(column2.ParseFromArray(column_slice2.data, column_slice2.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer2;

  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  meta.mutable_k_column()->DeleteSubrange(5, 1);
  size_t col_size3 = column3.ByteSizeLong();
  auto* buffer3 = new char[col_size3];
  column3.SerializeToArray(buffer3, col_size3);
  TSSlice column_slice3{buffer3, col_size3};
  ASSERT_EQ(column3.ParseFromArray(column_slice3.data, column_slice3.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer3;
  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAddDropColumnRollback) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len2};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0 , &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // add column
  roachpb::KWDBKTSColumn column;
  column.set_storage_type(roachpb::DataType::TIMESTAMP);
  column.set_storage_len(8);
  column.set_column_id(meta.k_column_size());
  column.set_name("column" + std::to_string(meta.k_column_size()));
  string err_msg;
  size_t col_size = column.ByteSizeLong();
  auto* buffer = new char[col_size];
  column.SerializeToArray(buffer, col_size);

  TSSlice column_slice{buffer, col_size};
  ASSERT_EQ(column.ParseFromArray(column_slice.data, column_slice.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer;
  // add tag
  roachpb::KWDBKTSColumn column1;
  column1.set_col_type(roachpb::KWDBKTSColumn_ColumnType::
                        KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1.set_storage_type(roachpb::DataType::TIMESTAMP);
  column1.set_storage_len(8);
  column1.set_column_id(meta.k_column_size() + 1);
  column1.set_name("column" + std::to_string(meta.k_column_size() + 1));
  size_t col_size1 = column1.ByteSizeLong();
  auto* buffer1 = new char[col_size1];
  column1.SerializeToArray(buffer1, col_size1);
  TSSlice column_slice1{buffer1, col_size1};
  ASSERT_EQ(column1.ParseFromArray(column_slice1.data, column_slice1.len), true);
  s = ts_engine_->AddColumn(ctx_, cur_table_id, trans_id.data(), column_slice1, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer1;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  size_t col_size2 = column2.ByteSizeLong();
  auto* buffer2 = new char[col_size2];
  column2.SerializeToArray(buffer2, col_size2);

  TSSlice column_slice2{buffer2, col_size2};
  ASSERT_EQ(column2.ParseFromArray(column_slice2.data, column_slice2.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice2, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer2;
  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  size_t col_size3 = column3.ByteSizeLong();
  auto* buffer3 = new char[col_size3];
  column3.SerializeToArray(buffer3, col_size3);

  TSSlice column_slice3{buffer3, col_size3};
  ASSERT_EQ(column3.ParseFromArray(column_slice3.data, column_slice3.len), true);
  s = ts_engine_->DropColumn(ctx_, cur_table_id, trans_id.data(), column_slice3, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer3;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAddDropColumnRecover) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;

  auto wal2 = new WALMgr(opts_.db_path, 0, 0, &opts_);
  wal2->Init(ctx_);
  auto tsm = TSxMgr(wal2);

  string trans_id = "0000000000000001";
  uint64_t mtr_id = 0;
  s = tsm.TSxBegin(ctx_, trans_id.data());
  EXPECT_EQ(s, KStatus::SUCCESS);
  mtr_id = tsm.getMtrID(trans_id);
  int columns_size = meta.k_column_size();
  // add column
  roachpb::KWDBKTSColumn column;
  column.set_storage_type(roachpb::DataType::TIMESTAMP);
  column.set_storage_len(8);
  column.set_column_id(columns_size);
  column.set_name("column" + std::to_string(meta.k_column_size()));

  size_t column_size = column.ByteSizeLong();
  auto* buffer = KNEW char[column_size];
  column.SerializeToArray(buffer, column_size);
  TSSlice column_slice{buffer, column_size};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, WALAlterType::ADD_COLUMN, column_slice);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer;

  // add tag
  roachpb::KWDBKTSColumn column1;
  column1.set_col_type(roachpb::KWDBKTSColumn_ColumnType::
                       KWDBKTSColumn_ColumnType_TYPE_TAG);
  column1.set_storage_type(roachpb::DataType::TIMESTAMP);
  column1.set_storage_len(8);
  column1.set_column_id(meta.k_column_size() + 1);
  column1.set_name("column" + std::to_string(meta.k_column_size() + 1));

  size_t column_size1 = column1.ByteSizeLong();
  auto* buffer1 = KNEW char[column_size1];
  column1.SerializeToArray(buffer1, column_size1);
  TSSlice column_slice1{buffer1, column_size1};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, WALAlterType::ADD_COLUMN, column_slice1);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer1;

  s = tsm.TSxCommit(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  trans_id = "0000000000000002";
  mtr_id = tsm.getMtrID(trans_id);
  s = tsm.TSxBegin(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // drop column
  roachpb::KWDBKTSColumn column2 = meta.mutable_k_column()->Get(2);
  size_t column_size2 = column2.ByteSizeLong();
  auto* buffer2 = KNEW char[column_size2];
  column2.SerializeToArray(buffer2, column_size2);
  TSSlice column_slice2{buffer2, column_size2};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, WALAlterType::DROP_COLUMN, column_slice2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer2;

  // drop tag
  roachpb::KWDBKTSColumn column3 = meta.mutable_k_column()->Get(5);
  size_t column_size3 = column3.ByteSizeLong();
  auto* buffer3 = KNEW char[column_size3];
  column3.SerializeToArray(buffer3, column_size3);
  TSSlice column_slice3{buffer3, column_size3};
  s = wal2->WriteDDLAlterWAL(ctx_, mtr_id, cur_table_id, WALAlterType::DROP_COLUMN, column_slice3);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer3;

  s = tsm.TSxCommit(ctx_, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = wal2->Flush(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete wal2;

  // recover
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  k_uint32 p_len2 = 0;
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len2};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;


  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWAL, TsxAlterColumnRollback) {
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);

  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  k_uint32 p_len2 = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta, 10, 0, false);
  TSSlice payload{data_value, p_len};
  char* data_value2 = GenSomePayloadData(ctx_, row_num_, p_len2, start_ts + 1000, &meta, 10, 0, false);
  TSSlice payload2{data_value2, p_len2};
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload, 1, 0 , &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  // alter column
  roachpb::KWDBKTSColumn column_old = meta.mutable_k_column()->Get(2);
  roachpb::KWDBKTSColumn column_new = column_old;
  column_new.set_storage_type(roachpb::DataType::BIGINT);
  column_new.set_storage_len(8);
  string err_msg;

  size_t col_size_new = column_new.ByteSizeLong();
  auto* buffer_new = new char[col_size_new];
  column_new.SerializeToArray(buffer_new, col_size_new);
  TSSlice column_slice_new{buffer_new, col_size_new};
  ASSERT_EQ(column_new.ParseFromArray(column_slice_new.data, column_slice_new.len), true);

  size_t col_size_old = column_old.ByteSizeLong();
  auto* buffer_old = new char[col_size_old];
  column_old.SerializeToArray(buffer_old, col_size_old);
  TSSlice column_slice_old{buffer_old, col_size_old};
  ASSERT_EQ(column_old.ParseFromArray(column_slice_old.data, column_slice_old.len), true);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), column_slice_new, column_slice_old, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer_new;
  delete[] buffer_old;
  // alter tag
  roachpb::KWDBKTSColumn column_old1 = meta.mutable_k_column()->Get(5);
  roachpb::KWDBKTSColumn column_new1 = column_old1;
  column_new1.set_storage_type(roachpb::DataType::BIGINT);
  column_new1.set_storage_len(8);

  size_t col_size_new1 = column_new1.ByteSizeLong();
  auto* buffer_new1 = new char[col_size_new1];
  column_new1.SerializeToArray(buffer_new1, col_size_new1);
  TSSlice column_slice_new1{buffer_new1, col_size_new1};
  ASSERT_EQ(column_new1.ParseFromArray(column_slice_new1.data, column_slice_new1.len), true);

  size_t col_size_old1 = column_old1.ByteSizeLong();
  auto* buffer_old1 = new char[col_size_old1];
  column_old1.SerializeToArray(buffer_old1, col_size_old1);
  TSSlice column_slice_old1{buffer_old1, col_size_old1};
  ASSERT_EQ(column_old1.ParseFromArray(column_slice_old1.data, column_slice_old1.len), true);
  s = ts_engine_->AlterColumnType(ctx_, cur_table_id, trans_id.data(), column_slice_new1, column_slice_old1, err_msg);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] buffer_new1;
  delete[] buffer_old1;
  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->PutData(ctx_, cur_table_id, kTestRange.range_group_id, &payload2, 1, 0, &dedup_result);
  ASSERT_EQ(s, KStatus::SUCCESS);

  delete[] data_value;
  delete[] data_value2;

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

/*TEST_F(TestEngineWAL, TsxAlterPartitionInterval) {
  opts_.db_path = db_path;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, db_path, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  roachpb::CreateTsTable meta;
  KTableKey cur_table_id = 123456789;
  ConstructRoachpbTable(&meta, "testTable", cur_table_id);
  std::vector<RangeGroup> ranges{kTestRange};
  s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table), KStatus::SUCCESS);
  ASSERT_EQ(ts_table->GetPartitionInterval(), BigObjectConfig::iot_interval);

  string trans_id = "0000000000000001";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->AlterPartitionInterval(ctx_, cur_table_id, trans_id.data(), 10000);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxCommit(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(ts_table->GetPartitionInterval(), 10000);

  trans_id = "0000000000000002";
  s = ts_engine_->TSxBegin(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->AlterPartitionInterval(ctx_, cur_table_id, trans_id.data(), 20000);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_engine_->TSxRollback(ctx_, cur_table_id, trans_id.data());
  ASSERT_EQ(s, KStatus::SUCCESS);

  ASSERT_EQ(ts_table->GetPartitionInterval(), 10000);

  s = ts_engine_->DropTsTable(ctx_, cur_table_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}*/
