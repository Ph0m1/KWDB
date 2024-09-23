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
#ifndef KWDBTS2_EXEC_TESTS_EE_METADATA_DATA_TEST_H_
#define KWDBTS2_EXEC_TESTS_EE_METADATA_DATA_TEST_H_

#include <memory>
#include <string>
#include <vector>

#include "ee_test_util.h"
#include "engine.h"
#include "libkwdbts2.h"
namespace kwdbts {

#define SafeDelete(p)             \
  if (nullptr != (p)) delete (p); \
  (p) = nullptr;

RangeGroup test_range{101, 0};
void CreateTable(kwdbContext_p ctx, TSEngine* ts_engine, KTableId table_id) {
  roachpb::CreateTsTable meta;
  ConstructRoachpbTable(&meta, "test_table", table_id);

  vector<RangeGroup> ranges{test_range};
  // TSEngine* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  ASSERT_EQ(ts_engine->CreateTsTable(ctx, table_id, &meta, ranges),
            KStatus::SUCCESS);
  int row_num = 5;
  k_uint32 p_len = 0;
  KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
  char* data_value = GenSomePayloadData(ctx, row_num, p_len, start_ts, &meta);

  std::shared_ptr<TsTable> ts_table;
  ASSERT_EQ(ts_engine->GetTsTable(ctx, table_id, ts_table), KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> tbl_range;
  ASSERT_EQ(
      ts_table->GetEntityGroup(ctx, test_range.range_group_id, &tbl_range),
      KStatus::SUCCESS);
  TSSlice payload{data_value, p_len};
  ASSERT_EQ(tbl_range->PutData(ctx, payload), KStatus::SUCCESS);
  delete []data_value;
}

void CreateTestTsEngine(kwdbContext_p ctx, string db_path, KTableKey table_id) {
  EngineOptions opts;
  opts.wal_level = 0;
  opts.db_path = db_path;
  TSEngine* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  TSEngineImpl::OpenTSEngine(ctx, db_path, opts, &ts_engine);
  CreateTable(ctx, ts_engine, table_id);
  ctx->ts_engine = ts_engine;
}

void DropTsTable(kwdbContext_p ctx, KTableKey table_id) {
  TSEngine* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  ts_engine->DropTsTable(ctx, table_id);
}

void CloseTestTsEngine(kwdbContext_p ctx) {
  TSEngine* ts_engine = static_cast<TSEngine*>(ctx->ts_engine);
  TSEngineImpl::CloseTSEngine(ctx, ts_engine);
}
}  // namespace kwdbts
#endif  // KWDBTS2_EXEC_TESTS_EE_METADATA_DATA_TEST_H_
