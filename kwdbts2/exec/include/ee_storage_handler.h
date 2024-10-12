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
// Created by liguoliang on 2022/07/18.
/***************************************************
 * @brief
 *              Encapsulating the engine abstraction interface, all engines
 *              should implement the following interface calls
 * @author
 *              guoliang Li
 * @date
 *              2022-07-20
 ***************************************************/
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <list>
#include <utility>

#include "cm_kwdb_context.h"
#include "ee_global.h"
#include "ee_table.h"
#include "ee_tag_row_batch.h"
#include "ee_tag_scan_op.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "mmap/mmap_tag_column_table.h"

namespace kwdbts {

class TsTable;
class TsTableIterator;
class TagIterator;
class BaseEntityIterator;
class TABLE;
class Field;

/**
 * @brief   Engine base class
 *
 * @author  liguoliang
 *
 * @note    This class is designed as a top-level class, and is currently
 *          designed as an all-virtual interface function, because it is not
 *          clear what changes will occur to the subsequent storage engine, add
 *          multiple engines, whether it is necessary to perform
 * differentiation, etc., * so a large expansion space is reserved
 */
class TagScanIterator;
class ScanRowBatch;
class StorageHandler {
 public:
  explicit StorageHandler(TABLE *table) : table_(table) {}

  virtual ~StorageHandler();

  void SetTagRowBatch(TagRowBatchPtr tag_datahandle) {
    tag_rowbatch_ = tag_datahandle;
  }

  void SetReadMode(TSTableReadMode read_mode) { read_mode_ = read_mode; }

  /**
   * @brief             init
   *
   * @param             ctx
   * @return            EEIteratorErrCode
   *                    function
   */
  virtual EEIteratorErrCode Init(kwdbContext_p ctx);

  /**
   * @brief
   *
   * @param ctx
   * @param min_ts
   * @param max_ts
   */
  virtual void SetSpans(std::vector<KwTsSpan> *ts_spans);

  /**
   * @brief           read data
   *
   * @param ctx
   * @param data
   * @param count
   * @return EEIteratorErrCode
   */
  virtual EEIteratorErrCode TsNext(kwdbContext_p ctx);

  /**
   * @brief           read data
   *
   * @param ctx
   * @param data
   * @param count
   * @return EEIteratorErrCode
   */
  virtual EEIteratorErrCode TagNext(kwdbContext_p ctx, Field *tag_filter);

  /**
   * @brief           singleton read data without pipe parallelism for multiple model processing
   *
   * @param ctx
   * @param data
   * @param count
   * @return EEIteratorErrCode
   */
  virtual EEIteratorErrCode SingletonTagNext(kwdbContext_p ctx, Field *tag_filter);

  /**
   * @brief               Close the query
   *
   * @param ctx
   * @return KStatus
   */
  KStatus Close();

  /**
   * @brief         alloc iterator
   *
   * @param ctx
   * @param min_ts
   * @param max_ts
   * @return EEIteratorErrCode
   */
  virtual EEIteratorErrCode NewTsIterator(kwdbContext_p ctx);

  virtual EEIteratorErrCode NewTagIterator(kwdbContext_p ctx);

  virtual EEIteratorErrCode GetNextTagData(kwdbContext_p ctx, ScanRowBatch *row_batch);

  EEIteratorErrCode GetEntityIdList(kwdbContext_p ctx, TSTagReaderSpec* spec, Field* tag_filter);
  // get entity id list in HashTagScan to scan tag data for multiple model processing
  EEIteratorErrCode GetRelEntityIdList(kwdbContext_p ctx, TSTagReaderSpec* spec, Field* tag_filter,
          std::vector<void *>& primary_tags, std::vector<void *>& secondary_tags,
          const vector<k_uint32> tag_other_join_cols);

  // genearate primary tags, which can also be used in HashTagScan for multiple model processing
  static KStatus GeneratePrimaryTags(TSTagReaderSpec *spec, TABLE *table,
                           size_t malloc_size, kwdbts::k_int32 sz,
                           std::vector<void *> *primary_tags);

  void tagFilter(kwdbContext_p ctx, Field* tag_filter);
  // filter tag data inside HashTagScan with relational joining data for multiple model processing
  void tagRelFilter(kwdbContext_p ctx,
                    std::vector<void *> secondary_tags,
                    const vector<k_uint32> tag_other_join_cols);
  void SetTagScan(TagScanBaseOperator* tag_scan) { tag_scan_ = tag_scan; }

  bool isDisorderedMetrics();

 private:
  TABLE *table_{nullptr};
  std::shared_ptr<TsTable> ts_table_{nullptr};
  std::vector<KwTsSpan> *ts_spans_{nullptr};
  TsTableIterator *ts_iterator{nullptr};
  //  TagIterator *tag_iterator{nullptr};
  BaseEntityIterator *tag_iterator{nullptr};
  TagRowBatchPtr tag_rowbatch_;
  TSTableReadMode read_mode_{
      TSTableReadMode::tableTableMeta};
  TagScanBaseOperator* tag_scan_{nullptr};
  k_uint32 current_line_{0};
  std::vector<EntityResultIndex> entities_;
  uint64_t total_read_rows_{0};
};

}  // namespace kwdbts

