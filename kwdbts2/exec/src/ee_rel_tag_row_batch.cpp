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

#include "ee_rel_tag_row_batch.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_table.h"

namespace kwdbts {

KStatus RelTagRowBatch::GetTagData(TagData *tagData, void **bitmap,
                                k_uint32 line) {
  if (line >= rel_tag_data_.size()) {
    return KStatus::FAIL;
  }
  *tagData = rel_tag_data_[line];
  return KStatus::SUCCESS;
}

void RelTagRowBatch::Init(TABLE *table) {
  table_ = table;
  SetTagToColOffset(table->GetMinTagId());
  k_uint32 boffset = 1 + (table->GetTagNum() + 7) / 8;
  SetBitmapOffset(boffset);
  k_uint32 primary_tags_len = PRIMARY_TAGS_EXTERN_STORAGE_LENGTH;
  for (k_int32 i = tag_col_offset_; i < table->field_num_; i++) {
    if (table->fields_[i]->get_column_type() ==
        roachpb::KWDBKTSColumn::TYPE_PTAG) {
      primary_tags_len += table->fields_[i]->get_storage_length();
    }
  }
  for (k_int32 i = tag_col_offset_; i < table->field_num_; i++) {
    if (table->fields_[i]->get_column_type() ==
        roachpb::KWDBKTSColumn::TYPE_PTAG) {
      tag_offsets_.emplace_back(primary_tags_len);
    } else {
      roachpb::DataType dt = table_->fields_[i]->get_sql_type();
      if (((dt == roachpb::DataType::VARCHAR) ||
           (dt == roachpb::DataType::NVARCHAR) ||
           (dt == roachpb::DataType::VARBINARY))) {
        tag_offsets_.emplace_back(sizeof(intptr_t) + 1);  // for varchar
      } else {
        tag_offsets_.emplace_back(table->fields_[i]->get_storage_length() + 1);
      }
    }
  }
  res_.setColumnNum(table_->scan_tags_.size());
}

KStatus RelTagRowBatch::GetEntities(std::vector<EntityResultIndex> *entities,
                                 k_uint32 *start_tag_index) {
  k_uint32 entities_num_per_pipe, remainder;
  *(start_tag_index) = current_pipe_line_;
  if (current_pipe_no_ >= pipe_entity_num_.size()) {
    return FAIL;
  }
  if (isFilter_) {
    for (k_uint32 i = 0; i < pipe_entity_num_[current_pipe_no_]; i++) {
      entities->emplace_back(
          entity_indexs_[selection_[current_pipe_line_].entity_]);
      current_pipe_line_++;
    }
  } else {
    for (k_uint32 i = 0; i < pipe_entity_num_[current_pipe_no_]; i++) {
      entities->emplace_back(entity_indexs_[current_pipe_line_]);
      current_pipe_line_++;
    }
  }
  current_pipe_no_++;
  return SUCCESS;
}

bool RelTagRowBatch::isAllDistributed() {
  return current_pipe_no_ >= valid_pipe_no_;
}

void RelTagRowBatch::SetPipeEntityNum(k_uint32 pipe_degree) {
  current_pipe_no_ = 0;
  current_pipe_line_ = 0;
  k_int32 entities_num_per_pipe, remainder;
  if (isFilter_) {
    entities_num_per_pipe = selection_.size() / pipe_degree;
    remainder = selection_.size() % pipe_degree;
  } else {
    entities_num_per_pipe = entity_indexs_.size() / pipe_degree;
    remainder = entity_indexs_.size() % pipe_degree;
  }
  for (k_int32 i = 0; i < pipe_degree; i++) {
    int current_size = entities_num_per_pipe;
    if (remainder > 0) {
      current_size++;
      remainder--;
    }
    pipe_entity_num_.emplace_back(current_size);
    if (current_size > 0) {
      valid_pipe_no_++;
    }
  }
}

KStatus RelTagRowBatch::AddRelTagJoinRecord(kwdbContext_p ctx, RelBatchContainerPtr rel_batch_container,
                                              TagRowBatchPtr tag_row_batch, TagData& tag_data,
                                              RelRowIndice& row_indice) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  void* bitmap = nullptr;
  k_uint32 col_idx_in_rs;
  while (row_indice.offset_in_batch != 0) {
    rel_tag_record.resize(rel_tag_col_num);
    for (int i = 0; i < tag_col_num; ++i) {
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[i] + table_->min_tag_id_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = tag_data[i];
    }
    // Add rel column data to rel_tag_record
    for (int i = 0; i < rel_col_num; ++i) {
      TagRawData rel_col_raw_data;
      rel_batch_container->rel_data_chunks_[row_indice.batch_no - 1]->ConvertToTagData(ctx, row_indice.offset_in_batch - 1,
                                                                  table_->scan_rel_cols_[i], rel_col_raw_data);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    rel_tag_data_.push_back(std::move(rel_tag_record));
    entity_indexs_.push_back(tag_row_batch->GetCurrentEntityIndex());
    row_indice = rel_batch_container->rel_data_linked_lists_[row_indice.batch_no - 1]->
                                                              row_indice_list_[row_indice.offset_in_batch - 1];
  }
  Return(ret);
}

KStatus RelTagRowBatch::AddTagRelJoinRecord(kwdbContext_p ctx, RelBatchContainerPtr rel_batch_container,
                              DataChunkPtr &rel_data_chunk, k_uint32 rel_row,
                              RelRowIndice& row_indice) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  void* bitmap = nullptr;
  k_uint32 col_idx_in_rs;
  while (row_indice.offset_in_batch != 0) {
    rel_tag_record.resize(rel_tag_col_num);
    HashTagRowBatchPtr tag_rowbatch = rel_batch_container->tag_rowbatches_[row_indice.batch_no - 1];
    TagData tag_data(tag_col_num);
    if (tag_rowbatch->GetTagData(&tag_data, &bitmap, row_indice.offset_in_batch - 1) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
    for (int i = 0; i < tag_col_num; ++i) {
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[i] + table_->min_tag_id_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(tag_data[i]);
    }
    // Add rel column data to rel_tag_record
    for (int i = 0; i < rel_col_num; ++i) {
      TagRawData rel_col_raw_data;
      rel_data_chunk->ConvertToTagData(ctx, rel_row,
                                        table_->scan_rel_cols_[i], rel_col_raw_data);
      col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_)->getColIdxInRs();
      rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
    }
    rel_tag_data_.push_back(std::move(rel_tag_record));
    entity_indexs_.push_back(tag_rowbatch->GetEntityIndex(row_indice.offset_in_batch - 1));
    row_indice = rel_batch_container->rel_data_linked_lists_[row_indice.batch_no - 1]->
                                                              row_indice_list_[row_indice.offset_in_batch - 1];
  }
  Return(ret);
}

KStatus RelTagRowBatch::AddRelTagJoinRecord(kwdbContext_p ctx,
                                            DataChunkPtr &rel_data_chunk,
                                            k_uint32 rel_row_index,
                                            TagRowBatchPtr tag_row_batch) {
  EnterFunc();
  TagData rel_tag_record;
  k_uint32 tag_col_num = table_->scan_tags_.size();
  k_uint32 rel_col_num = table_->scan_rel_cols_.size();
  k_uint32 rel_tag_col_num = tag_col_num + rel_col_num;
  rel_tag_record.reserve(rel_tag_col_num);
  TagData tag_data(tag_col_num);
  void* bitmap = nullptr;
  k_uint32 current_line = 0;
  KStatus ret = KStatus::SUCCESS;
  k_uint32 col_idx_in_rs;
  k_uint32 count = tag_row_batch->isFilter_ ? tag_row_batch->effect_count_ : tag_row_batch->count_;
  for (int i = 0; i < count; ++i) {
    if (tag_row_batch->GetCurrentTagData(&tag_data, &bitmap) == KStatus::SUCCESS) {
      rel_tag_record.resize(rel_tag_col_num);
      for (int i = 0; i < tag_col_num; ++i) {
        col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_tags_[i] + table_->min_tag_id_)->getColIdxInRs();
        rel_tag_record[col_idx_in_rs] = std::move(tag_data[i]);
      }
      // Add rel column data to rel_tag_record
      for (int i = 0; i < rel_col_num; ++i) {
        TagRawData rel_col_raw_data;
        rel_data_chunk->ConvertToTagData(ctx, rel_row_index, table_->scan_rel_cols_[i], rel_col_raw_data);
        col_idx_in_rs = table_->GetFieldWithColNum(table_->scan_rel_cols_[i] + table_->field_num_)->getColIdxInRs();
        rel_tag_record[col_idx_in_rs] = std::move(rel_col_raw_data);
      }
      rel_tag_data_.push_back(std::move(rel_tag_record));
      entity_indexs_.push_back(tag_row_batch->GetEntityIndex(tag_row_batch->GetCurrentEntity()));
    }
    // Move to next tag
    tag_row_batch->NextLine();
  }
  Return(ret);
}

}  // namespace kwdbts
