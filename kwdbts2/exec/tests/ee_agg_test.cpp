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

#include "gtest/gtest.h"
#include "ee_dml_exec.h"
#include "ee_op_test_base.h"
#include "ee_op_operator_utils.h"
#include "ee_op_spec_utils.h"

namespace kwdbts {

const int row_num_per_payload = 1;
const int insert_batch = 1;
const int test_table_id = 800;

class TestAggOp : public OperatorTestBase {
 public:
  TestAggOp() : OperatorTestBase(test_table_id) {

  }

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    roachpb::CreateTsTable meta;
    TSBSSchema::constructTableMetadata(meta, "test_table", table_id_);

    CreateTable(meta);
    InsertRecords(meta);
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }

  void CreateTable(roachpb::CreateTsTable& meta) {
    ASSERT_EQ(engine_->CreateTsTable(ctx_, table_id_, &meta, {test_range}),
              KStatus::SUCCESS);
  }

  void InsertRecords(roachpb::CreateTsTable& meta) {
    for (int round = 0; round < insert_batch; round++) {
      k_uint32 p_len = 0;
//      KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
//          std::chrono::system_clock::now().time_since_epoch())
//          .count();
      KTimestamp start_ts = 0;
      auto data_value = TSBSSchema::genPayloadData(ctx_, row_num_per_payload, p_len, start_ts, meta);
      TSSlice payload{data_value.get(), p_len};
      DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
      engine_->PutData(ctx_, table_id_, test_range.range_group_id, &payload, 1, 0, &dedup_result);
    }
  }
};

// select hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
// from benchmark.cpu group by hostname  order by min_usage;
TEST_F(TestAggOp, TestDmlExecAgg) {
  TSFlowSpec flow;
  SpecAgg agg_spec(table_id_);
  agg_spec.PrepareFlowSpec(flow);

  size_t size = flow.ByteSizeLong();

  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));
  auto message = make_unique<char[]>(size);
  flow.SerializeToArray(message.get(), size);

  auto* info = reinterpret_cast<QueryInfo*>(req.get());
  auto* info2 = reinterpret_cast<QueryInfo*>(resp.get());
  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = size;
  info->id = 3;
  info->unique_id = 34716;
  info->handle = nullptr;
  info->value = message.get();
  info->ret = 0;
  info->time_zone = 0;
  info->relBatchData = nullptr;
  info->relRowCount = 0;

  KStatus status = DmlExec::ExecQuery(ctx_, info, info2);
  ASSERT_EQ(status, KStatus::SUCCESS);

  auto* result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));
  ASSERT_EQ(result->ret, SUCCESS);

  // next
  info->tp = EnMqType::MQ_TYPE_DML_NEXT;

  do {
    ASSERT_EQ(DmlExec::ExecQuery(ctx_, info, info2), KStatus::SUCCESS);
    result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));

    if (result->value) {
      free(result->value);
      result->value = nullptr;
    }
  } while (result->code != -1);

  ASSERT_EQ(result->ret, SUCCESS);

  info->handle = result->handle;
  info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
  DmlExec::ExecQuery(ctx_, info, info2);
}

}  // namespace kwdbts
