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
#include "ee_hash_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_rel_hash_index.h"

namespace kwdbts {

class RelBatchContainer;
typedef std::shared_ptr<RelBatchContainer> RelBatchContainerPtr;
class RelBatchLinkedList;
typedef std::unique_ptr<RelBatchLinkedList> RelBatchLinkedListPtr;

// relational batch data linked list of hash index for multiple model processing
class RelBatchLinkedList {
 public:
  explicit RelBatchLinkedList(k_uint16 row_count);
  ~RelBatchLinkedList();

  RelRowIndice* row_indice_list_{nullptr};
  k_uint16  row_count_{0};
};

// relational batch container to keep relational data from ME for multiple model processing
class RelBatchContainer {
 public:
  RelBatchContainer() {}
  ~RelBatchContainer() {
    for (auto data : physical_tag_data_batches_) {
      for (const auto& it : data) {
        for (auto batch : it) {
          delete batch;
        }
      }
    }
  }
  void AddRelDataChunk(DataChunkPtr &rel_data_chunk);
  // add relational data chunk and build the linked list for each relational row
  KStatus AddRelDataChunkAndBuildLinkedList(RelHashIndex* rel_hash_index,
                                            DataChunkPtr &rel_data_chunk,
                                            std::vector<k_uint32>& rel_key_cols,
                                            std::vector<k_uint32>& join_column_lengths,
                                            char* rel_join_column_value,
                                            k_uint32 total_join_column_length);
  // add tag row batch and build the linked list for each tag row
  KStatus AddHashTagRowBatchAndBuildLinkedList(TABLE* table,
                                                RelHashIndex* rel_hash_index,
                                                HashTagRowBatchPtr &tag_rowbatch,
                                                std::vector<k_uint32>& tag_key_cols,
                                                std::vector<k_uint32>& join_column_lengths,
                                                char* tag_join_column_value,
                                                k_uint32 total_join_column_length);
  // add tag data result and keep it from being deleted
  void AddPhysicalTagData(ResultSet& res);

 public:
  std::vector<DataChunkPtr> rel_data_chunks_;
  std::vector<HashTagRowBatchPtr> tag_rowbatches_;
  std::vector<RelBatchLinkedListPtr> rel_data_linked_lists_;
  std::vector<std::vector<std::vector<const Batch*>>> physical_tag_data_batches_;
};

};  // namespace kwdbts
