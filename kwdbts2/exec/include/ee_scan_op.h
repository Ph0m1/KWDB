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

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ee_base_op.h"
#include "ee_scan_row_batch.h"
#include "ee_flow_param.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "ee_global.h"

namespace kwdbts {

class ScanRowBatch;

class ReaderSpec;

class TABLE;

class KWDBPostProcessSpec;

class Handler;

/**
 * @brief   scan operator
 *
 * @author  liguoliang
 */
class TableScanOperator : public BaseOperator {
 public:
  TableScanOperator(TSReaderSpec* spec, TSPostProcessSpec* post, TABLE* table, BaseOperator* input, int32_t processor_id);

  TableScanOperator(const TableScanOperator&, BaseOperator* input, int32_t processor_id);

  ~TableScanOperator() override;

  EEIteratorErrCode PreInit(kwdbContext_p ctx) override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  [[nodiscard]] BaseOperator* GetInput() const { return input_; }

  BaseOperator* Clone() override;

  RowBatchPtr GetRowBatch(kwdbContext_p ctx) override;

  k_uint32 GetTotalReadRow() {return total_read_row_;}

 protected:
  EEIteratorErrCode InitHandler(kwdbContext_p ctx);
  EEIteratorErrCode InitScanRowBatch(kwdbContext_p ctx, ScanRowBatchPtr *row_batch);
  k_bool ResolveOffset();

 protected:
  TSPostProcessSpec* post_{nullptr};
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  std::vector<KwTsSpan> ts_kwspans_;
  k_uint32 limit_{0};
  k_uint32 offset_{0};
  ReaderPostResolve param_;
  Field* filter_{nullptr};

  BaseOperator* input_{nullptr};  // input iterator

 protected:
  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
  k_uint32 total_read_row_{0};

  k_bool is_done_{false};
};

}  // namespace kwdbts
