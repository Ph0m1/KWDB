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
#include <unordered_map>
#include <utility>
#include <vector>

#include "ee_row_batch.h"
#include "ee_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_rel_hash_index.h"
#include "ee_rel_batch_container.h"

namespace kwdbts {

class RelTagRowBatch;
typedef std::shared_ptr<RelTagRowBatch> RelTagRowBatchPtr;

// wrap HashTagScan result as tag row batch for multiple model processing
class RelTagRowBatch : public TagRowBatch {
 public:
  RelTagRowBatch() {}
  ~RelTagRowBatch() {}

  /**
   * read count
   */
  k_uint32 Count() override {
    return rel_tag_data_.size();
  }
  KStatus GetTagData(TagData *tagData, void **bitmap, k_uint32 line) override;
  void Init(TABLE *table) override;
  void SetPipeEntityNum(k_uint32 pipe_degree);
  KStatus GetEntities(std::vector<EntityResultIndex> *entities,
                      k_uint32 *start_tag_index);
  bool isAllDistributed();
  KStatus AddRelTagJoinRecord(kwdbContext_p ctx, DataChunkPtr &rel_data_chunk,
                              k_uint32 rel_row_index, TagRowBatchPtr tag_row_batch);
  KStatus AddRelTagJoinRecord(kwdbContext_p ctx, RelBatchContainerPtr rel_batch_container,
                              TagRowBatchPtr tag_row_batch, TagData& tag_data,
                              RelRowIndice& row_indice);
  KStatus AddTagRelJoinRecord(kwdbContext_p ctx, RelBatchContainerPtr rel_batch_container,
                              DataChunkPtr &rel_data_chunk, k_uint32 rel_row,
                              RelRowIndice& row_indice);

 private:
  std::vector<TagData> rel_tag_data_;
};

};  // namespace kwdbts
