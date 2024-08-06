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

#include <memory>
#include <vector>

#include "ee_base_op.h"
#include "ee_statistic_scan_flow_spec.h"
#include "ee_scan_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
namespace kwdbts {
class Handler;
class TABLE;
class TSPostProcessSpec;
class TSStatisticReaderSpec;

class TableStatisticScanOperator : public BaseOperator {
 public:
  TableStatisticScanOperator(TSStatisticReaderSpec *spec,
                             TSPostProcessSpec *post, TABLE *table, BaseOperator *input, int32_t processor_id);
  TableStatisticScanOperator(const TableStatisticScanOperator&, BaseOperator* input, int32_t processor_id);
  virtual ~TableStatisticScanOperator();
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode PreInit(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  KStatus Close(kwdbContext_p ctx) override;
  EEIteratorErrCode Reset(kwdbContext_p ctx) override;
  [[nodiscard]] BaseOperator* GetInput() const { return input_; }
  BaseOperator* Clone() override;

 protected:
  EEIteratorErrCode InitHandler(kwdbContext_p ctx);
  EEIteratorErrCode InitScanRowBatch(kwdbContext_p ctx);

 protected:
  TSPostProcessSpec *post_{nullptr};
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  std::vector<KwTsSpan> ts_kwspans_;
  StatisticSpecResolve param_;

 protected:
  BaseOperator *input_{nullptr};  // input iterator

 protected:
  Handler *handle_{nullptr};
  std::shared_ptr<ScanRowBatch> data_handle_{nullptr};
};
}  // namespace kwdbts
