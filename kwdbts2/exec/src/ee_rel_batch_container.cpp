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

#include "ee_rel_batch_container.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_table.h"

namespace kwdbts {

KStatus RelBatchContainer::AddRelDataChunkAndBuildLinkedList(RelHashIndex* rel_hash_index,
                                                        DataChunkPtr &rel_data_chunk,
                                                        std::vector<k_uint32>& rel_key_cols,
                                                        std::vector<k_uint32>& join_column_lengths,
                                                        char* rel_join_column_value,
                                                        k_uint32 total_join_column_length) {
  // Build linked list for the coming relational batch
  k_uint32 row_count = rel_data_chunk->Count();
  RelRowIndice last_row_indice;
  RelRowIndice new_row_indice;
  rel_data_linked_lists_.push_back(std::make_unique<RelBatchLinkedList>(row_count));
  new_row_indice.batch_no = rel_data_chunks_.size() + 1;
  for (new_row_indice.offset_in_batch = 1; new_row_indice.offset_in_batch <= row_count; ++new_row_indice.offset_in_batch) {
    // Get the join key value from relational batch
    char* p = rel_join_column_value;
    for (int i = 0; i < rel_key_cols.size(); ++i) {
      memcpy(p, rel_data_chunk->GetDataPtr(new_row_indice.offset_in_batch - 1, rel_key_cols[i]), join_column_lengths[i]);
      p += join_column_lengths[i];
    }
    // Add or update hash table key value
    if (rel_hash_index->addOrUpdate(rel_join_column_value, total_join_column_length,
                                    new_row_indice, last_row_indice) != 0) {
      return KStatus::FAIL;
    }
    if (last_row_indice.batch_no > 0) {
      // Update linked list
      rel_data_linked_lists_[new_row_indice.batch_no - 1]->row_indice_list_[new_row_indice.offset_in_batch - 1]
        = last_row_indice;
    }
  }
  // Keep the relational data chunk in memory for probe and table reader later.
  rel_data_chunks_.push_back(std::move(rel_data_chunk));
  return KStatus::SUCCESS;
}

KStatus RelBatchContainer::AddHashTagRowBatchAndBuildLinkedList(TABLE* table,
                                                        RelHashIndex* rel_hash_index,
                                                        HashTagRowBatchPtr &tag_rowbatch,
                                                        std::vector<k_uint32>& tag_key_cols,
                                                        std::vector<k_uint32>& join_column_lengths,
                                                        char* tag_join_column_value,
                                                        k_uint32 total_join_column_length) {
  // Build linked list for the coming relational batch
  k_uint32 row_count = tag_rowbatch->count_;
  k_uint32 tag_col_num = table->scan_tags_.size();
  TagData tag_data(tag_col_num);
  void* bitmap = nullptr;
  k_uint32 col_idx_in_rs;

  RelRowIndice new_row_indice;
  RelRowIndice last_row_indice;
  k_int32 current_line = 0;
  rel_data_linked_lists_.push_back(std::make_unique<RelBatchLinkedList>(row_count));
  new_row_indice.batch_no = tag_rowbatches_.size() + 1;
  // Loop each tag to search matched records
  while (tag_rowbatch->GetCurrentTagData(&tag_data, &bitmap) == KStatus::SUCCESS) {
    char* p = tag_join_column_value;
    memset(p, 0, total_join_column_length);
    for (int i = 0; i < tag_key_cols.size(); ++i) {
      k_uint32 index = table->scan_tags_[tag_key_cols[i]] + table->GetMinTagId();
      roachpb::DataType type = table->fields_[index]->get_sql_type();
      switch (type) {
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NVARCHAR:
          strncpy(p, tag_data[tag_key_cols[i]].tag_data, join_column_lengths[i]);
          break;
        default:
          memcpy(p, tag_data[tag_key_cols[i]].tag_data, join_column_lengths[i]);
          break;
      }
      p += join_column_lengths[i];
    }
    // Build linked list for for current tag data in tag row batch
    new_row_indice.offset_in_batch = tag_rowbatch->GetCurrentEntity() + 1;
    // Add or update hash table key value
    if (rel_hash_index->addOrUpdate(tag_join_column_value, total_join_column_length,
                                    new_row_indice, last_row_indice) != 0) {
      return KStatus::FAIL;
    }
    if (last_row_indice.batch_no > 0) {
      // Update linked list
      rel_data_linked_lists_[new_row_indice.batch_no - 1]->row_indice_list_[new_row_indice.offset_in_batch - 1]
        = last_row_indice;
    }
    current_line = tag_rowbatch->NextLine();
    if (current_line < 0) {
      break;
    }
  }
  // Keep the tag row batch in memory for probe later.
  tag_rowbatches_.push_back(std::move(tag_rowbatch));
  return KStatus::SUCCESS;
}

void RelBatchContainer::AddRelDataChunk(DataChunkPtr &rel_data_chunk) {
  rel_data_chunks_.push_back(std::move(rel_data_chunk));
}

void RelBatchContainer::AddPhysicalTagData(ResultSet& res) {
  physical_tag_data_batches_.push_back(std::move(res.data));
}

RelBatchLinkedList::RelBatchLinkedList(k_uint16 row_count) {
  row_count_ = row_count;
  row_indice_list_ = static_cast<RelRowIndice*>(malloc(row_count_ * sizeof(RelRowIndice)));
  memset(row_indice_list_, 0, row_count_ * sizeof(RelRowIndice));
}

RelBatchLinkedList::~RelBatchLinkedList() {
  if (row_indice_list_) {
    free(row_indice_list_);
    row_indice_list_ = nullptr;
    row_count_ = 0;
  }
}

}  // namespace kwdbts
