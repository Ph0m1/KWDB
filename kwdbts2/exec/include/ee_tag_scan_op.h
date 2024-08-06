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
#include "ee_flow_param.h"
#include "ee_tag_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"

namespace kwdbts {

/**
 * @brief   scan oper
 *
 * @author  liguoliang
 */
class TagScanOperator : public BaseOperator {
 public:
  TagScanOperator(TSTagReaderSpec* spec, TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  ~TagScanOperator() override;

  EEIteratorErrCode PreInit(kwdbContext_p ctx) override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  RowBatchPtr GetRowBatch(kwdbContext_p ctx) override;

  KStatus GetEntities(kwdbContext_p ctx,
                      std::vector<EntityResultIndex>* entities,
                      k_uint32* start_tag_index, TagRowBatchPtr* row_batch_ptr);

 protected:
  k_bool ResolveOffset();

 protected:
  TSTagReaderSpec* spec_{nullptr};
  TSPostProcessSpec* post_{nullptr};
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  k_uint32 examined_rows_{0};   // valid row count
  k_uint32 total_read_row_{0};  // total count
  char* data_{nullptr};
  k_uint32 count_{0};
  ReaderPostResolve param_;
  Field* filter_{nullptr};
  TagRowBatchPtr tagdata_handle_{nullptr};
  Handler* handler_{nullptr};

 private:
  mutable std::mutex tag_lock_;
  bool is_pre_init_{false};
  bool is_init_{false};
  EEIteratorErrCode pre_init_code_;
  EEIteratorErrCode init_code_;
  bool tag_index_once_{true};
  bool is_first_entity_{true};
};
}  // namespace kwdbts
