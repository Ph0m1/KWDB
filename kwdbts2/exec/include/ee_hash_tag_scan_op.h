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

#include <unistd.h>  // For getpid()
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>
#include <utility>
#include <random>
#include <unordered_map>

#include "ee_base_op.h"
#include "ee_flow_param.h"
#include "ee_tag_row_batch.h"
#include "ee_rel_tag_row_batch.h"
#include "ee_hash_tag_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "ee_tag_scan_op.h"
#include "mmap/mmap_tag_column_table.h"
#include "ee_rel_batch_queue.h"
#include "tag_iterator.h"
#include "ee_rel_hash_index.h"

namespace kwdbts {

// scan tag data and hash join with relational data from ME for multiple model processing
class HashTagScanOperator : public TagScanBaseOperator {
 public:
  HashTagScanOperator(TsFetcherCollection *collection,
                      TSTagReaderSpec* spec,
                      TSPostProcessSpec* post,
                      TABLE* table,
                      int32_t processor_id);

  ~HashTagScanOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  RowBatch* GetRowBatch(kwdbContext_p ctx) override;

  KStatus Close(kwdbContext_p ctx) override;

  KStatus GetEntities(kwdbContext_p ctx,
                      std::vector<EntityResultIndex>* entities,
                      TagRowBatchPtr* row_batch_ptr) override;

 protected:
  k_bool ResolveOffset();
  EEIteratorErrCode CreateDynamicHashIndexes(kwdbContext_p ctx);
  EEIteratorErrCode BuildTagIndex(kwdbContext_p ctx);
  EEIteratorErrCode BuildRelIndex(kwdbContext_p ctx);
  EEIteratorErrCode GetJoinColumnValues(kwdbContext_p ctx,
                                        DataChunkPtr& rel_data_chunk,
                                        k_uint32 row_index,
                                        std::vector<void*>& primary_column_values,
                                        std::vector<void*>& other_join_columns_values);
  EEIteratorErrCode InitRelJointIndexes();

 protected:
  TSTagReaderSpec* spec_{nullptr};
  TSPostProcessSpec* post_{nullptr};
  k_uint64 object_id_{0};
  k_uint32 total_read_row_{0};  // total count
  char* data_{nullptr};
  k_uint32 count_{0};
  ReaderPostResolve param_;
  Field* filter_{nullptr};
  HashTagRowBatchPtr tag_rowbatch_{nullptr};
  RelTagRowBatchPtr rel_tag_rowbatch_{nullptr};
  RelBatchContainerPtr rel_batch_container_{nullptr};
  StorageHandler* handler_{nullptr};
  vector<k_uint32> primary_rel_cols_;
  vector<k_uint32> primary_tag_cols_;
  vector<k_uint32> rel_other_join_cols_;
  vector<k_uint32> tag_other_join_cols_;
  RelBatchQueue* rel_batch_queue_;
  TagIterator *tag_iterator_{nullptr};
  RelHashIndex* rel_hash_index_{nullptr};
  std::unordered_map<k_uint32, k_uint32> scan_tag_to_output;
  std::vector<void*> primary_tags_;

 private:
  mutable std::mutex tag_lock_;
  bool is_init_{false};
  bool started_{false};
  EEIteratorErrCode init_code_;
  EEIteratorErrCode start_code_;
  bool tag_index_once_{true};
  bool is_first_entity_{true};
  std::vector<k_uint32> join_column_lengths_;
  k_uint32 total_join_column_length_{0};
  std::vector<std::pair<k_uint32, roachpb::KWDBKTSColumn::ColumnType>> tag_type_mapping;
};

}  // namespace kwdbts
