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

#include "ee_hash_tag_scan_op.h"

#include <sstream>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "cm_func.h"
#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "lg_api.h"
#include "ee_kwthd_context.h"
#include "ee_dml_exec.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

HashTagScanOperator::HashTagScanOperator(TsFetcherCollection *collection,
                                         TSTagReaderSpec* spec, TSPostProcessSpec* post,
                                         TABLE* table, int32_t processor_id)
    : TagScanBaseOperator(collection, table, processor_id),
      spec_(spec),
      post_(post),
      param_(post, table) {
  if (spec) {
    table->SetAccessMode(spec->accessmode());
    object_id_ = spec->tableid();
    table->SetTableVersion(spec->tableversion());
  }
}

HashTagScanOperator::~HashTagScanOperator() = default;

KStatus HashTagScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  if (rel_hash_index_) {
    delete rel_hash_index_;
    rel_hash_index_ = nullptr;
  }

  Return(KStatus::SUCCESS);
}

EEIteratorErrCode HashTagScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (is_init_) {
    Return(init_code_);
  }
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    // resolve tag
    param_.ResolveScanTags(ctx);
    for (k_uint32 i = 0; i < table_->scan_tags_.size(); ++i) {
      scan_tag_to_output[table_->scan_tags_[i]] = i;
    }
    // resolve relational cols
    param_.ResolveScanRelCols(ctx);
    // post->filter;
    ret = param_.ResolveFilter(ctx, &filter_, true);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve::ResolveFilter() failed");
      break;
    }
    if (object_id_ > 0) {
      // renders num
      param_.RenderSize(ctx, &num_);
      ret = param_.ResolveRender(ctx, &renders_, num_);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("ResolveRender() failed");
        break;
      }
      // Output Fields
      ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
      if (EEIteratorErrCode::EE_OK != ret) {
        LOG_ERROR("ResolveOutputFields() failed");
        break;
      }
    }
  } while (0);

  // Create a relational batch queue to receive relational batched from ME.
  KStatus status = ctx->dml_exec_handle->CreateRelBatchQueue(ctx, table_->GetRelFields());
  if (status != KStatus::SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  rel_batch_queue_ = ctx->dml_exec_handle->GetRelBatchQueue();

  // Create rel_batch_container_ to keep relational batches, linked list
  // and tag data.
  rel_batch_container_ = std::make_shared<RelBatchContainer>();

  is_init_ = true;
  init_code_ = ret;

  Return(ret);
}

EEIteratorErrCode HashTagScanOperator::InitRelJointIndexes() {
  // Prepare primary rel indexes
  int pos = 0;
  vector<Field*>& rel_fields = table_->GetRelFields();
  std::vector<pair<k_uint32, k_uint32>>& rel_tag_join_column_indexes = table_->GetRelTagJoinColumnIndexes();
  for (int i = 0; i < rel_tag_join_column_indexes.size(); ++i) {
    while (pos < rel_tag_join_column_indexes[i].second) {
      ++pos;
    }
    if (table_->fields_[pos]->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      primary_rel_cols_.push_back(rel_tag_join_column_indexes[i].first);
      k_uint32 length = rel_fields[rel_tag_join_column_indexes[i].first]->get_storage_length();
      join_column_lengths_.push_back(length);
      total_join_column_length_ += length;
    } else {
      rel_other_join_cols_.push_back(rel_tag_join_column_indexes[i].first);
      tag_other_join_cols_.push_back(scan_tag_to_output[rel_tag_join_column_indexes[i].second - table_->GetMinTagId()]);
    }
  }
  k_int32 sz = spec_->primarytags_size();
  if (sz > 0) {
    size_t malloc_size = 0;
    for (int i = 0; i < sz; ++i) {
      k_uint32 tag_id = spec_->mutable_primarytags(i)->colid();
      malloc_size += table_->fields_[tag_id]->get_storage_length();
    }
    KStatus ret = StorageHandler::GeneratePrimaryTags(spec_, table_, malloc_size, sz, &primary_tags_);
    if (ret != SUCCESS) {
      return(EEIteratorErrCode::EE_ERROR);
    }
  }
  return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode HashTagScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  // Check for cancellation at the start
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  std::unique_lock l(tag_lock_);
  // Involving parallelism, ensuring that it is only called once
  if (started_) {
    Return(start_code_);
  }
  // Get TagScan ready
  started_ = true;
  start_code_ = EEIteratorErrCode::EE_ERROR;
  handler_ = new StorageHandler(table_);
  start_code_ = handler_->Init(ctx);
  if (start_code_ == EEIteratorErrCode::EE_ERROR) {
    Return(start_code_);
  }

  k_uint32 access_mode = table_->GetAccessMode();
  switch (access_mode) {
    case TSTableReadMode::hashTagScan:
    case TSTableReadMode::hashRelScan:
      // Build dynamic hash indexes
      start_code_ = CreateDynamicHashIndexes(ctx);
      break;
    case TSTableReadMode::primaryHashTagScan:
      start_code_ = InitRelJointIndexes();
      break;
    default: {
      start_code_ = EEIteratorErrCode::EE_ERROR;
      LOG_ERROR("access mode must be primaryHashTagScan, hashTagScan or hashRelScan, %d", access_mode);
      break;
    }
  }

  Return(start_code_);
}

EEIteratorErrCode HashTagScanOperator::CreateDynamicHashIndexes(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_END_OF_RECORD;
  // Check for cancellation at the start
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  // Initialize relational join columns
  std::vector<pair<k_uint32, k_uint32>>& rel_tag_join_column_indexes = table_->GetRelTagJoinColumnIndexes();
  for (auto join_index : rel_tag_join_column_indexes) {
    primary_rel_cols_.push_back(join_index.first);
    primary_tag_cols_.push_back(scan_tag_to_output[join_index.second - table_->GetMinTagId()]);
  }
  vector<Field*>& rel_fields = table_->GetRelFields();
  for (auto col : primary_rel_cols_) {
    k_uint32 length = rel_fields[col]->get_storage_length();
    join_column_lengths_.push_back(length);
    total_join_column_length_ += length;
  }

  code = handler_->NewTagIterator(ctx);
  if (code != EE_OK) {
    Return(code);
  }

  // Create a in-memory hash index
  rel_hash_index_ = new RelHashIndex();
  rel_hash_index_->init(total_join_column_length_);

  // Time to bulid the dynamic hash index, it's up to optimizer
  // to decide which one to build.
  if (table_->GetAccessMode() == TSTableReadMode::hashRelScan) {
    // Use relation data to build hash index
    code = BuildRelIndex(ctx);
  } else if (table_->GetAccessMode() == TSTableReadMode::hashTagScan) {
    // Use tag data to build hash index
    code = BuildTagIndex(ctx);
  } else {
    code = EEIteratorErrCode::EE_ERROR;
  }
  Return(code)
}

EEIteratorErrCode HashTagScanOperator::BuildRelIndex(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // Create tag_rowbatch for tag data
  tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
  tag_rowbatch_->Init(table_);
  handler_->SetTagRowBatch(tag_rowbatch_);

  // Move the relational batch to rel_tag_rowbatch and build linked list
  DataChunkPtr rel_data_chunk;

  char* rel_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
  while ((code = rel_batch_queue_->Next(ctx, rel_data_chunk)) != EEIteratorErrCode::EE_END_OF_RECORD) {
    // TODO(Yongyan): Check if stopped, exit if true.
    if (code == EEIteratorErrCode::EE_OK) {
      rel_batch_container_->AddRelDataChunkAndBuildLinkedList(rel_hash_index_, rel_data_chunk,
                                                            primary_rel_cols_, join_column_lengths_,
                                                            rel_join_column_value, total_join_column_length_);
    } else if (code == EEIteratorErrCode::EE_TIMESLICE_OUT) {
      // Continue to get next relational batch
    } else {
      Return(code)
    }
  }
  free(rel_join_column_value);
  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode HashTagScanOperator::BuildTagIndex(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // Create tag_rowbatch for tag data
  tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
  tag_rowbatch_->Init(table_);
  handler_->SetTagRowBatch(tag_rowbatch_);

  char* tag_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
  // Scan all tag data with filter
  while ((code = handler_->SingletonTagNext(ctx, filter_)) != EEIteratorErrCode::EE_END_OF_RECORD) {
    // Check for cancellation within the loop
    if (CheckCancel(ctx) != SUCCESS) {
      free(tag_join_column_value);
      Return(EEIteratorErrCode::EE_ERROR);
    }
    if (code != EEIteratorErrCode::EE_OK) {
      free(tag_join_column_value);
      Return(code);
    }
    // Move the tag row batch into hyper container and build the linked list
    rel_batch_container_->AddHashTagRowBatchAndBuildLinkedList(table_, rel_hash_index_, tag_rowbatch_,
                                                          primary_tag_cols_, join_column_lengths_,
                                                          tag_join_column_value, total_join_column_length_);
    // Create a new tag_rowbatch for next tag data
    tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
    tag_rowbatch_->Init(table_);
    handler_->SetTagRowBatch(tag_rowbatch_);
  }
  free(tag_join_column_value);

  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode HashTagScanOperator::GetJoinColumnValues(kwdbContext_p ctx,
                                                      DataChunkPtr& rel_data_chunk,
                                                      k_uint32 row_index,
                                                      std::vector<void*>& primary_column_values,
                                                      std::vector<void*>& other_join_columns_values) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  char* p = static_cast<char*>(primary_column_values[0]);
  for (int i = 0; i < primary_rel_cols_.size(); ++i) {
    memcpy(p, rel_data_chunk->GetDataPtr(row_index, primary_rel_cols_[i]), join_column_lengths_[i]);
    p += join_column_lengths_[i];
  }
  for (int i = 0; i < rel_other_join_cols_.size(); ++i) {
    other_join_columns_values[i] = rel_data_chunk->GetDataPtr(row_index, rel_other_join_cols_[i]);
  }
  Return(code);
}

EEIteratorErrCode HashTagScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  int64_t read_bytes = 0;
  // check when enter
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (!rel_batch_queue_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "rel_batch_queue_ was not created.");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  if (table_->GetAccessMode() == TSTableReadMode::hashRelScan) {
    // Probe tag table with rel hash index
    handler_->SetTagRowBatch(tag_rowbatch_);
    rel_tag_rowbatch_ = std::make_shared<RelTagRowBatch>();
    rel_tag_rowbatch_->Init(table_);
    char* rel_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
    while ((code = handler_->SingletonTagNext(ctx, filter_)) != EEIteratorErrCode::EE_END_OF_RECORD) {
      // Check for cancellation within the loop
      if (CheckCancel(ctx) != SUCCESS) {
        free(rel_join_column_value);
        Return(EEIteratorErrCode::EE_ERROR);
      }
      if (code != EEIteratorErrCode::EE_OK) {
        free(rel_join_column_value);
        Return(code);
      }
      k_uint32 tag_col_num = table_->scan_tags_.size();
      TagData tag_data(tag_col_num);
      void* bitmap = nullptr;
      KStatus ret = KStatus::SUCCESS;
      k_uint32 col_idx_in_rs;

      bool has_matched_record = false;
      // Loop each tag to search matched records
      while (tag_rowbatch_->GetCurrentTagData(&tag_data, &bitmap) == KStatus::SUCCESS) {
        char* p = rel_join_column_value;
        memset(p, 0, total_join_column_length_);
        for (int i = 0; i < primary_tag_cols_.size(); ++i) {
          k_uint32 index = table_->scan_tags_[primary_tag_cols_[i]] + table_->GetMinTagId();
          roachpb::DataType type = table_->fields_[index]->get_sql_type();
          switch (type) {
            case roachpb::DataType::VARCHAR:
            case roachpb::DataType::CHAR:
            case roachpb::DataType::NVARCHAR:
              strncpy(p, tag_data[primary_tag_cols_[i]].tag_data, join_column_lengths_[i]);
              break;
            default:
              memcpy(p, tag_data[primary_tag_cols_[i]].tag_data, join_column_lengths_[i]);
              break;
          }
          p += join_column_lengths_[i];
        }
        // search for the linked list
        RelRowIndice row_indice;
        if (rel_hash_index_->get(rel_join_column_value, total_join_column_length_, row_indice) == 0) {
          // for each matched record, combine them to output
          rel_tag_rowbatch_->AddRelTagJoinRecord(ctx, rel_batch_container_, tag_rowbatch_, tag_data, row_indice);
          has_matched_record = true;
        }

        k_int32 current_line = tag_rowbatch_->NextLine();
        if (current_line < 0) {
          break;
        }
      }
      if (has_matched_record) {
        // We can only keep the matched tags for future optimization.
        rel_batch_container_->AddPhysicalTagData(tag_rowbatch_->res_);
        if (rel_tag_rowbatch_->Count() >= ONE_FETCH_COUNT) {
          break;
        }
      }
    }
    free(rel_join_column_value);
    rel_tag_rowbatch_->SetPipeEntityNum(current_thd->GetDegree());
    if (rel_tag_rowbatch_->Count() > 0) {
      code = EEIteratorErrCode::EE_OK;
    } else {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
    }
    // tmp bytes
    if (rel_tag_rowbatch_ != nullptr) {
      read_bytes += (sizeof(rel_tag_rowbatch_));
    }
  } else {
    // Create tag_rowbatch for tag data
    tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
    tag_rowbatch_->Init(table_);
    handler_->SetTagRowBatch(tag_rowbatch_);

    // Create rel_tag_rowbatch for output data with tag and relational columns
    rel_tag_rowbatch_ = std::make_shared<RelTagRowBatch>();
    rel_tag_rowbatch_->Init(table_);

    // Get primary column values and other join column values.
    std::vector<void*> primary_column_values(1);
    primary_column_values[0] = malloc(total_join_column_length_);
    std::vector<void*> other_join_columns_values(rel_other_join_cols_.size());
    Defer defer{[&]() {
      free(primary_column_values[0]);
    }};

    DataChunkPtr rel_data_chunk;
    do {
      while ((code = rel_batch_queue_->Next(ctx, rel_data_chunk)) == EEIteratorErrCode::EE_TIMESLICE_OUT) {
        if (CheckCancel(ctx) != SUCCESS) {
          Return(EEIteratorErrCode::EE_ERROR);
        }
      }
      if (code != EEIteratorErrCode::EE_OK) {
        Return(code);
      }
      if (table_->GetAccessMode() == TSTableReadMode::primaryHashTagScan) {
        k_uint32 row_count = rel_data_chunk->Count();
        for (k_uint32 i = 0; i < row_count; ++i) {
          code = GetJoinColumnValues(ctx, rel_data_chunk, i, primary_column_values, other_join_columns_values);
          if (code != EEIteratorErrCode::EE_OK) {
            Return(code);
          }
          if (primary_tags_.size() > 0) {
            // Call new GetRelEntityIdList to search hash index, tagFilter & join match.
            code = handler_->GetRelEntityIdList(ctx, spec_, filter_,
                                                primary_tags_, other_join_columns_values, tag_other_join_cols_);
          } else {
            // Call new GetRelEntityIdList to search hash index, tagFilter & join match.
            code = handler_->GetRelEntityIdList(ctx, spec_, filter_,
                                                primary_column_values, other_join_columns_values, tag_other_join_cols_);
          }
          if (code != EE_OK && code != EE_END_OF_RECORD) {
            Return(code);
          }

          if (code == EE_OK) {
            rel_tag_rowbatch_->AddRelTagJoinRecord(ctx, rel_data_chunk, i, tag_rowbatch_);
            rel_batch_container_->AddPhysicalTagData(tag_rowbatch_->res_);
          }
        }
        rel_tag_rowbatch_->SetPipeEntityNum(current_thd->GetDegree());
        rel_batch_container_->AddRelDataChunk(rel_data_chunk);
        if (rel_data_chunk != nullptr) {
          read_bytes += int64_t(rel_data_chunk->Capacity()) * int64_t(rel_data_chunk->RowSize());
        }
      } else if (table_->GetAccessMode() == TSTableReadMode::hashTagScan) {
        k_uint32 row_count = rel_data_chunk->Count();
        bool has_matched_record = false;
        for (k_uint32 i = 0; i < row_count; ++i) {
          code = GetJoinColumnValues(ctx, rel_data_chunk, i, primary_column_values, other_join_columns_values);
          if (code != EEIteratorErrCode::EE_OK) {
            Return(code);
          }
          // search for the linked list
          RelRowIndice row_indice;
          if (rel_hash_index_->get(static_cast<char*>(primary_column_values[0]),
                                    total_join_column_length_, row_indice) == 0) {
            // for each matched record, combine them to output
            // rel_tag_rowbatch_->AddRelTagJoinRecord(ctx, rel_batch_container_, tag_rowbatch_, tag_data, row_indice);
            rel_tag_rowbatch_->AddTagRelJoinRecord(ctx, rel_batch_container_, rel_data_chunk, i, row_indice);
            has_matched_record = true;
          }
        }
        read_bytes += int64_t(rel_data_chunk->Capacity()) * int64_t(rel_data_chunk->RowSize());
        rel_tag_rowbatch_->SetPipeEntityNum(current_thd->GetDegree());
        if (has_matched_record) {
          rel_batch_container_->AddRelDataChunk(rel_data_chunk);
        }
      } else {
          LOG_ERROR("access mode must be primaryHashTagScan or hashTagScan, %d", table_->GetAccessMode());
          Return(EEIteratorErrCode::EE_ERROR);
      }
    } while (rel_tag_rowbatch_->Count() == 0);
    primary_column_values.clear();  // Clear the vector after cleanup
    other_join_columns_values.clear();  // Clear the vector after cleanup
  }

  Return(code);
}

EEIteratorErrCode HashTagScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  // not supported yet
  Return(EEIteratorErrCode::EE_ERROR);
}

RowBatch* HashTagScanOperator::GetRowBatch(kwdbContext_p ctx) {
  EnterFunc();

  Return(tag_rowbatch_.get());
}

EEIteratorErrCode HashTagScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();

  if (handler_) {
    SafeDeletePointer(handler_);
  }
  total_read_row_ = 0;
  data_ = nullptr;
  count_ = 0;
  tag_index_once_ = false;
  started_ = false;
  tag_index_once_ = true;

  Return(EEIteratorErrCode::EE_OK)
}

KStatus HashTagScanOperator::GetEntities(kwdbContext_p ctx,
                                     std::vector<EntityResultIndex> *entities,
                                     TagRowBatchPtr *row_batch_ptr) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (*row_batch_ptr == nullptr) {
    *row_batch_ptr = rel_tag_rowbatch_;
  }
  if (is_first_entity_ || (*row_batch_ptr != nullptr &&
                           (row_batch_ptr->get()->isAllDistributed()))) {
    if (is_first_entity_ || *row_batch_ptr == rel_tag_rowbatch_ || rel_tag_rowbatch_->isAllDistributed()) {
      is_first_entity_ = false;
      EEIteratorErrCode code = Next(ctx);
      if (code != EE_OK) {
        Return(FAIL);
      }
    } else if (rel_tag_rowbatch_.get()->Count() == 0) {
      Return(FAIL);
    }

    // construct ts_iterator
    *row_batch_ptr = rel_tag_rowbatch_;
  }
  KStatus ret = row_batch_ptr->get()->GetEntities(entities);
  Return(ret);
}

}  // namespace kwdbts
