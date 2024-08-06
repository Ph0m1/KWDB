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

#include "engine.h"
#include "test_util.h"

using namespace kwdbts;  // NOLINT

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

RangeGroup kTestRange{101, 0};

class TestTsSnapshotTable : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestTsSnapshotTable() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    opts_.wal_level = 0;
    opts_.db_path = kDbPath;

    system(("rm -rf " + kDbPath + "/*").c_str());
    EXPECT_EQ(TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_), KStatus::SUCCESS);
  }

  ~TestTsSnapshotTable() {
    // CLOSE engine
    // ts_engine_->Close();
    delete ts_engine_;
    ts_engine_ = nullptr;
    kwdbts::KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  int row_num_ = 5;
};

// Test TsSnapshotTable
TEST_F(TestTsSnapshotTable, CreateSnapshot) {
  roachpb::CreateTsTable meta;

  KTableKey cur_table_id = 1007;
  ConstructRoachpbTable(&meta, "testTableAndColumnNameTooLong_testTableAndColumnNameTooLong", cur_table_id, 86400, 12);

  std::vector<RangeGroup> ranges{kTestRange};

  KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
  ASSERT_EQ(s, KStatus::SUCCESS);
  const KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  k_uint32 p_len = 0;
  char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);

// INSERT INTO benchmark.host_template VALUES ('host_0','x86', 510,56),('host_23','x86', 780,62);
  std::shared_ptr<TsTable> ts_table;
  s = ts_engine_->GetTsTable(ctx_, cur_table_id, ts_table);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  s = ts_table->GetEntityGroup(ctx_, kTestRange.range_group_id, &tbl_range);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  s = tbl_range->PutData(ctx_, payload);
  ASSERT_EQ(s, KStatus::SUCCESS);

  roachpb::CreateTsTable get_meta;
  s = ts_engine_->GetMetaData(ctx_, cur_table_id, kTestRange, &get_meta);
  ASSERT_EQ(s, KStatus::SUCCESS);
  std::vector<AttributeInfo> data_schema;
  ts_table->GetDataSchema(ctx_, &data_schema);
  Payload pl(data_schema, payload);
  CheckgenSomePayloadData(ctx_, &pl, start_ts, &meta);
  delete[] data_value;

  k_uint32 entity_id = 1;
  KwTsSpan ts_span = {start_ts, start_ts + 5 * 10};
  std::vector<k_uint32> scan_cols = {0, 1, 2};
  std::vector<Sumfunctype> scan_agg_types;
  TsIterator* iter1;
  SubGroupID group_id = 1;
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, scan_cols, scan_cols, scan_agg_types, &iter1, tbl_range),
            KStatus::SUCCESS);
  ResultSet res{(k_uint32) scan_cols.size()};
  k_uint32 count;
  bool is_finished = false;
  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, row_num_);
  ASSERT_EQ(KTimestamp(res.data[0][0]->mem), start_ts);

  ASSERT_EQ(iter1->Next(&res, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter1;


  MMapTagColumnTable* entity_tag_bt = tbl_range->GetSubEntityGroupTagbt();
  size_t rownum = 1;
  uint64_t begin_hash = UINT_MAX;
  uint64_t end_hash = 0;

  for(; rownum <= entity_tag_bt->size(); rownum++) {
    if(!entity_tag_bt->isValidRow(rownum)) {
      continue;
    }
    uint32_t tag_hash = TsTable::GetConsistentHashId((char*) (entity_tag_bt->record(rownum)),
                                                     entity_tag_bt->primaryTagSize());
    if (tag_hash < begin_hash) {
      begin_hash = tag_hash;
    }
    if (tag_hash > end_hash) {
      end_hash = tag_hash;
    }
  }

  uint64_t snapshot_id = 0;
  s = ts_table->CreateSnapshot(ctx_, kTestRange.range_group_id, begin_hash, end_hash, &snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  TSSlice data;
  size_t total = 0;
  size_t limit = 999999;
  s = ts_table->GetSnapshotData(ctx_, kTestRange.range_group_id, snapshot_id, 0, limit, &data, &total);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto it = ts_table->snapshot_manage_pool_.begin();
  std::shared_ptr<TsTableSnapshot> snapshot = it->second;
  TsEntityGroup* snapshot_gp = snapshot->GetSnapshotEntityGroup();
  int tag_bt_data_num = snapshot_gp->GetSubEntityGroupTagbt()->size();
  ASSERT_EQ(tag_bt_data_num, 1);

  EntityGroupTagIterator *tag_iter = nullptr;
  std::vector<uint32_t> scan_tags = {0};
  s = snapshot_gp->GetTagIterator(ctx_, scan_tags, &tag_iter);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(tag_iter, nullptr);

  std::vector<EntityResultIndex> entity_id_list;
  ResultSet log_res{(k_uint32) scan_cols.size()};
  k_uint32 log_count = 0;
  ASSERT_EQ(tag_iter->Next(&entity_id_list, &log_res, &log_count), KStatus::SUCCESS);
  ASSERT_EQ(log_count, 1);
  log_res.clear();
  tag_iter->Close();
  delete tag_iter;

  TsIterator* iter2;
  group_id = 1;
  entity_id = 1;
  ASSERT_EQ(snapshot_gp->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, scan_cols, scan_cols, scan_agg_types, &iter2, tbl_range),
            KStatus::SUCCESS);
  ResultSet res2{(k_uint32) scan_cols.size()};
  is_finished = false;
  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 5);
  ASSERT_EQ(KTimestamp(res2.data[0][0]->mem), start_ts);
  ASSERT_EQ(KInt16(res2.data[1][0]->mem), 11);

  ASSERT_EQ(iter2->Next(&res2, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter2;

  s = ts_table->DropSnapshot(ctx_, kTestRange.range_group_id, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  s = ts_table->WriteSnapshotData(ctx_, kTestRange.range_group_id, snapshot_id, 0, data, true);
  ASSERT_EQ(s, KStatus::SUCCESS);
  delete[] data.data;

  s = ts_table->EnableSnapshot(ctx_, kTestRange.range_group_id, snapshot_id);
  ASSERT_EQ(s, KStatus::SUCCESS);

  int tag_bt_data_num_1 = tbl_range->GetSubEntityGroupTagbt()->size();
  ASSERT_EQ(tag_bt_data_num_1, 2);

  TsIterator* iter3;
  group_id = 2;
  entity_id = 1;
  std::vector<k_uint32> scan_cols_data;
  for (size_t i = 0; i < data_schema.size(); i++) {
    scan_cols_data.push_back(i);
  }
  
  ASSERT_EQ(tbl_range->GetIterator(ctx_, group_id, {entity_id}, {ts_span}, scan_cols_data, scan_cols_data, scan_agg_types, &iter3, tbl_range),
            KStatus::SUCCESS);
  ResultSet res3{(k_uint32) scan_cols_data.size()};
  is_finished = false;
  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 5);
  // ASSERT_EQ(KTimestamp(res3.data[0][0]->mem), start_ts);
  // ASSERT_EQ(KInt16(res3.data[1][0]->mem), 11);
  // ASSERT_EQ(KInt16(res3.data[2][0]->mem), 2222);
  CheckBatchData(ctx_, res3, start_ts, &meta);

  ASSERT_EQ(iter3->Next(&res3, &count, &is_finished), KStatus::SUCCESS);
  ASSERT_EQ(count, 0);
  delete iter3;

  EntityGroupTagIterator *tag_iter2 = nullptr;
  std::vector<uint32_t> scan_tags2 = {1};
  s = tbl_range->GetTagIterator(ctx_, scan_tags2, &tag_iter2);
  ASSERT_EQ(s, KStatus::SUCCESS);
  ASSERT_NE(tag_iter, nullptr);

  std::vector<EntityResultIndex> entity_id_list2;
  ResultSet log_res2{(k_uint32) scan_tags2.size()};
  k_uint32 log_count2 = 0;
  tag_iter2->Next(&entity_id_list2, &log_res2, &log_count2);
  ASSERT_EQ(log_count2, 2);
  log_res2.clear();
  tag_iter2->Close();
  delete tag_iter2;

  s = ts_table->DropAll(ctx_);
  ASSERT_EQ(s, KStatus::SUCCESS);
}


