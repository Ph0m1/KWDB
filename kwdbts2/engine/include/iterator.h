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

#include <deque>
#include <map>
#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "lt_rw_latch.h"
#include "ts_time_partition.h"
#include "mmap/mmap_segment_table_iterator.h"
#include "ts_common.h"

namespace kwdbts {
class TsEntityGroup;
class SubEntityGroupManager;

/**
 * @brief This is the iterator base class implemented internally in the storage layer, and its two derived classes are:
 *        (1) TsRawDataIterator, used for raw data queries (2) TsAggIterator, used for aggregate queries
 */
class TsIterator {
 public:
  TsIterator(std::shared_ptr<TsEntityGroup> entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
             vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans,
             std::vector<uint32_t>& kw_scan_cols, std::vector<uint32_t>& ts_scan_cols,
             uint32_t table_version);

  virtual ~TsIterator();

  virtual KStatus Init(std::vector<TsTimePartition*>& p_bts, bool is_reversed);

  /**
   * @brief An internally implemented iterator query interface that provides a subgroup data query result to the TsTableIterator class
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   */
  virtual KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) = 0;

  bool IsDisordered() {
    for (auto& pt : p_bts_) {
      EntityItem* entity_item = pt->getEntityItem(entity_ids_[cur_entity_idx_]);
      if (entity_item->is_disordered) {
        return true;
      }
    }
    return false;
  }

 protected:
  inline bool checkIfTsInSpan(timestamp64 ts) {
    for (auto& ts_span : ts_spans_) {
      if (ts >= ts_span.begin && ts <= ts_span.end) {
        return true;
      }
    }
    return false;
  }
  int nextBlockItem(k_uint32 entity_id);
  void fetchBlockItems(k_uint32 entity_id);

 protected:
  uint64_t entity_group_id_{0};
  uint32_t subgroup_id_{0};
  vector<uint32_t> entity_ids_{};
  // the data time range queried by the iterator
  std::vector<KwTsSpan> ts_spans_;
  // column index
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  // column attributes
  vector<AttributeInfo> attrs_;
  // table version
  uint32_t table_version_;
  std::vector<TsTimePartition*> p_bts_;
  // save all BlockItem objects in the partition table being queried
  std::deque<BlockItem*> block_item_queue_;
  // save the data offset within the BlockItem object being queried, used for traversal
  k_uint32 cur_blockdata_offset_ = 1;
  BlockItem* cur_block_item_ = nullptr;
  k_int32 cur_p_bts_idx_ = 0;
  uint32_t cur_entity_id_ = 0;
  k_uint32 cur_entity_idx_ = 0;
  MMapSegmentTableIterator* segment_iter_{nullptr};
  std::shared_ptr<TsEntityGroup> entity_group_;
  // Identifies whether the iterator returns blocks in reverse order
  bool is_reversed_ = false;
  // need sorting
  bool sort_flag_ = false;
};

// used for raw data queries
class TsRawDataIterator : public TsIterator {
 public:
  TsRawDataIterator(std::shared_ptr<TsEntityGroup> entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                    vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans,
                    std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols, uint32_t table_version) :
                    TsIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans,
                               kw_scan_cols, ts_scan_cols, table_version) {}

  /**
   * @brief The internal implementation of the row data query interface returns the maximum number of consecutive data
   *        in a BlockItem that meets the query criteria when called once.
   *        When is_finished is true, it indicates the end of the query.
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   */
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 private:
  void nextEntity() {
    cur_p_bts_idx_ = -1;
    cur_block_item_ = nullptr;
    cur_blockdata_offset_ = 1;
    block_item_queue_.clear();
    ++cur_entity_idx_;
  }
};

class TsSortedRowDataIterator : public TsIterator {
 public:
  TsSortedRowDataIterator(std::shared_ptr<TsEntityGroup> entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                          vector<uint32_t>& entity_ids, std::vector<KwTsSpan>& ts_spans,
                          std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                          uint32_t table_version, SortOrder order_type = ASC, bool compaction = false) :
      TsIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans,
                 kw_scan_cols, ts_scan_cols, table_version), order_type_(order_type), compaction_(compaction) {}

  KStatus Init(std::vector<TsTimePartition*>& p_bts, bool is_reversed) override;
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 private:
  int nextBlockSpan(k_uint32 entity_id);
  void fetchBlockSpans(k_uint32 entity_id);

  KStatus GetBatch(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* cur_block_item, size_t block_start_idx,
                   ResultSet* res, k_uint32 count);

  std::deque<BlockSpan> block_spans_;
  BlockSpan cur_block_span_;
  SortOrder order_type_ = SortOrder::ASC;
  bool compaction_ = false;
};

struct BlockBitmap {
  uint32_t col_idx;
  BLOCK_ID block_id;
  void* bitmap;
  bool need_free_bitmap;
  BlockBitmap(BLOCK_ID id, uint32_t idx, void* mem, bool need_free) : block_id(id), col_idx(idx),
                                                                      bitmap(mem), need_free_bitmap(need_free) { }
  ~BlockBitmap() {
    if (need_free_bitmap) {
      free(bitmap);
      bitmap = nullptr;
    }
  }

  bool IsNull(uint32_t row_idx) const {
    if (!bitmap) {
      return true;
    }
    return kwdbts::IsObjectColNull(reinterpret_cast<char*>(bitmap), row_idx);
  }
};

// used for aggregate queries
class TsAggIterator : public TsIterator {
 public:
  TsAggIterator(std::shared_ptr<TsEntityGroup> entity_group, uint64_t entity_group_id, uint32_t subgroup_id,
                vector<uint32_t>& entity_ids, vector<KwTsSpan>& ts_spans,
                std::vector<k_uint32>& kw_scan_cols, std::vector<k_uint32>& ts_scan_cols,
                std::vector<Sumfunctype>& scan_agg_types, uint32_t table_version) :
                TsIterator(entity_group, entity_group_id, subgroup_id, entity_ids, ts_spans, kw_scan_cols,
                           ts_scan_cols, table_version), scan_agg_types_(scan_agg_types) {
    // When creating an aggregate query iterator, the elements of the ts_scan_cols_ and scan_agg_types_ arrays
    // correspond one-to-one, and their lengths must be consistent.
    assert(scan_agg_types_.empty() || ts_scan_cols_.size() == scan_agg_types_.size());
    first_pairs_.assign(scan_agg_types_.size(), {});
    last_pairs_.assign(scan_agg_types_.size(), {});
    for (size_t i = 0; i < ts_scan_cols_.size(); ++i) {
      // If the query aggregation type contains first/last correlation, the corresponding member variables need to be
      // initialized to record the results during the query process.
      if (scan_agg_types_[i] == Sumfunctype::FIRST || scan_agg_types_[i] == Sumfunctype::FIRSTTS) {
        no_first_last_type_ = false;
        first_pairs_[i] = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::FIRST_ROW || scan_agg_types_[i] == Sumfunctype::FIRSTROWTS) {
        no_first_last_type_ = false;
        first_row_pair_ = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
        no_first_last_type_ = false;
        last_pairs_[i] = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::LAST_ROW || scan_agg_types_[i] == Sumfunctype::LASTROWTS) {
        no_first_last_type_ = false;
        last_row_pair_ = {-1, {INVALID_TS, MetricRowID{}}};
      }
    }
  }

  KStatus Init(std::vector<TsTimePartition*>& p_bts, bool is_reverse) override;
  /**
   * @brief The internally implemented aggregate data query interface returns the aggregate query result of an entity
   *        when called once. When is_finished is true, it indicates that the query has ended.
   *
   * @param res            the set of returned query results
   * @param count          number of rows of data
   * @param is_finished    identify whether the iterator has completed querying
   */
  KStatus Next(ResultSet* res, k_uint32* count, bool* is_finished, timestamp64 ts = INVALID_TS) override;

 private:
  /**
   * @brief    When the Next function is completed once, it indicates that the aggregated data of a entity has been queried.
   *           It is necessary to switch the entity ID and restore the variables that record the query status
   *           in order to query the next entity normally in the future.
   */
  void reset() {
    cur_p_bts_idx_ = -1;
    cur_block_item_ = nullptr;
    cur_blockdata_offset_ = 1;

    ++cur_entity_idx_;
    if (only_first_type_ || only_first_last_type_) {
      cur_first_idx_ = 0;
    }
    if (only_last_type_ || only_first_last_type_) {
      cur_last_idx_ = p_bts_.size() - 1;
    }

    has_found_first_data = false;
    has_found_last_data = false;
    first_pairs_.clear();
    last_pairs_.clear();
    first_pairs_.assign(scan_agg_types_.size(), {});
    last_pairs_.assign(scan_agg_types_.size(), {});
    for (size_t i = 0; i < ts_scan_cols_.size(); ++i) {
      if (scan_agg_types_[i] == Sumfunctype::FIRST || scan_agg_types_[i] == Sumfunctype::FIRSTTS) {
        first_pairs_[i] = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::FIRST_ROW || scan_agg_types_[i] == Sumfunctype::FIRSTROWTS) {
        first_row_pair_ = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::LAST || scan_agg_types_[i] == Sumfunctype::LASTTS) {
        last_pairs_[i] = {-1, {INVALID_TS, MetricRowID{}}};
      } else if (scan_agg_types_[i] == Sumfunctype::LAST_ROW || scan_agg_types_[i] == Sumfunctype::LASTROWTS) {
        last_row_pair_ = {-1, {INVALID_TS, MetricRowID{}}};
      }
    }
  }

  inline bool onlyHasFirstAggType() {
    for (auto& agg_type : scan_agg_types_) {
      if (agg_type != FIRST && agg_type != FIRSTTS && agg_type != FIRST_ROW && agg_type != FIRSTROWTS) {
        return false;
      }
      if (agg_type == FIRST_ROW || agg_type == FIRSTROWTS) {
        no_first_row_type_ = false;
      }
    }
    cur_first_idx_ = 0;
    return true;
  }

  inline bool onlyHasLastAggType() {
    for (auto& agg_type : scan_agg_types_) {
      if (agg_type != LAST && agg_type != LASTTS && agg_type != LAST_ROW && agg_type != LASTROWTS) {
        return false;
      }
      if (agg_type == LAST_ROW || agg_type == LASTROWTS) {
        no_last_row_type_ = false;
      }
    }
    cur_last_idx_ = p_bts_.size() - 1;
    return true;
  }

  inline bool onlyHasFirstLastAggType() {
    for (auto& agg_type : scan_agg_types_) {
      if (agg_type != FIRST && agg_type != FIRSTTS && agg_type != FIRST_ROW && agg_type != FIRSTROWTS &&
          agg_type != LAST && agg_type != LASTTS && agg_type != LAST_ROW && agg_type != LASTROWTS) {
        return false;
      }
    }
    cur_first_idx_ = 0;
    cur_last_idx_ = p_bts_.size() - 1;
    return true;
  }

  inline bool hasFoundFirstAggData() {
    for (k_uint32 i = 0; i < scan_agg_types_.size(); ++i) {
      switch (scan_agg_types_[i]) {
        case FIRST:
        case FIRSTTS:
          if (first_pairs_[i].first == -1) {
            return false;
          }
          break;
        case FIRST_ROW:
        case FIRSTROWTS:
          if (first_row_pair_.first == -1) {
            return false;
          }
          break;
        default:
          break;
      }
    }
    has_found_first_data = true;
    return true;
  }

  inline bool hasFoundLastAggData() {
    for (k_uint32 i = 0; i < scan_agg_types_.size(); ++i) {
      switch (scan_agg_types_[i]) {
        case LAST:
        case LASTTS:
          if (last_pairs_[i].first == -1) {
            return false;
          }
          break;
        case LAST_ROW:
        case LASTROWTS:
          if (last_row_pair_.first == -1) {
            return false;
          }
          break;
        default:
          break;
      }
    }
    has_found_last_data = true;
    return true;
  }

  // Determine whether certain column types have query results of certain aggregation types,
  // such as non-numeric types do not have SUM aggregation type.
  inline bool colTypeHasAggResult(DATATYPE col_type, Sumfunctype agg_type) {
    if (agg_type == COUNT) {
      return true;
    }
    if (agg_type == FIRST || agg_type == FIRSTTS ||agg_type == FIRST_ROW ||agg_type == FIRSTROWTS ||
        agg_type == LAST || agg_type == LASTTS || agg_type == LAST_ROW || agg_type == LASTROWTS) {
      return false;
    }
    if (agg_type == SUM && !isSumType(col_type)) {
      return false;
    }
    return true;
  }

  inline bool isAllAggResNull(ResultSet* res) {
    for (auto& it : res->data) {
      if (it.size() > 0 && it[0]->count > 0) {
        return false;
      }
    }
    return true;
  }

  // Used for first/last query optimization:
  // If only the first/last type is included in a single aggregation query process,
  // as most temporal data is written in sequence, optimization can be carried out for this special scenario.
  // 1. For first related queries, start traversing from the head of the partition table array, which is the smallest
  // partition table.
  // 2. For last related queries, start traversing from the end of the partition table array, which is the largest
  // partition table.
  void updateFirstCols(timestamp64 ts, MetricRowID row_id,
                       const std::unordered_map<uint32_t, std::shared_ptr<BlockBitmap>>& bitmaps);
  void updateLastCols(timestamp64 ts, MetricRowID row_id,
                      const std::unordered_map<uint32_t, std::shared_ptr<BlockBitmap>>& bitmaps);
  void updateFirstLastCols(timestamp64 ts, MetricRowID row_id,
                           const std::unordered_map<uint32_t, std::shared_ptr<BlockBitmap>>& bitmaps);
  KStatus findFirstData(ResultSet* res, k_uint32* count, timestamp64 ts);
  KStatus findLastData(ResultSet* res, k_uint32* count, timestamp64 ts);
  KStatus findFirstLastData(ResultSet* res, k_uint32* count, timestamp64 ts);

  KStatus getBlockBitmap(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* block_item,
                         std::unordered_map<uint32_t, std::shared_ptr<BlockBitmap>>& bitmaps);

  /**
   * @brief Used internally in the Next function, which returns the aggregated result of the most consecutive data in a BlockItem
   *        that meets the query criteria, is an intermediate result. When count is 0, it indicates that the query is completed.
   *
   *        The Next function obtains the intermediate aggregation result of all data within an entity that meets the query criteria
   *        by continuously calling traverseAllBlocks, and then integrates and calculates the result set to obtain the final
   *        aggregation result of the entity.
   */
  KStatus traverseAllBlocks(ResultSet* res, k_uint32* count, timestamp64 ts);

  /**
   * @brief Convert a continuous piece of data into a query column type, and then aggregate and calculate the converted data.
   *
   * @param segment_tbl     segment table pointer, where stores the continuous piece of data
   * @param start_row       the starting row id of this continuous piece of data
   * @param col_idx         queried col index
   * @param count           the number of rows in this continuous piece of data
   * @param mem             memory address for storing converted fixed length data
   * @param var_mem         memory address for storing variable length data after conversion
   * @param bitmap          memory address of bitmap for the data
   * @param need_free_bitmap need free bitmap if the bitmap address is generated using malloc
   */
  KStatus getActualColMemAndBitmap(std::shared_ptr<MMapSegmentTable> segment_tbl, BLOCK_ID block_id, size_t start_row,
                                   uint32_t col_idx, k_uint32 count, std::shared_ptr<void>* mem,
                                   std::vector<std::shared_ptr<void>>& var_mem, void** bitmap, bool& need_free_bitmap);

  KStatus getActualColBitmap(std::shared_ptr<MMapSegmentTable> segment_tbl, BLOCK_ID block_id, size_t start_row,
                             uint32_t col_idx, k_uint32 count, void** bitmap, bool& need_free_bitmap);
  /**
   * @brief Used to obtain the actual Batch object for a specific row of data, if a column type conversion occurs,
   *        the corresponding conversion needs to be performed on the row of data first.
   *        Otherwise, the address can be directly obtained.
   *
   * @param p_bt             partition table pointer, where stores the row of data
   * @param real_row         row id
   * @param col_idx          queried col index
   * @param b                Batch objects that encapsulate data
   */
  int getActualColAggBatch(TsTimePartition* p_bt, MetricRowID real_row, uint32_t col_idx, Batch** b);

 private:
  // The aggregation type corresponding to each column.
  // It can be empty, but if it is not empty, the size must be consistent with the size of scan.cols_
  std::vector<Sumfunctype> scan_agg_types_;
  // Used for the first/last query optimization process, recording the array index of the partition table being queried,
  // where cur_first_idx_ starts from the head and cur_last_idx_ starts from the tail.
  k_int32 cur_first_idx_;
  k_int32 cur_last_idx_;

  bool only_first_type_ = false;
  bool no_first_row_type_ = true;
  bool only_last_type_ = false;
  bool no_last_row_type_ = true;
  bool only_first_last_type_ = false;
  bool no_first_last_type_ = true;

  bool has_found_first_data = false;
  bool has_found_last_data = false;

  // Used to record first/last related results during a traversal process.
  // std::map<index of column, std::pair<index of partition table, std::pair<timestamp64, row id>>>
  std::vector<std::pair<k_int32, std::pair<timestamp64, MetricRowID>>> first_pairs_;
  std::vector<std::pair<k_int32, std::pair<timestamp64, MetricRowID>>> last_pairs_;
  // std::pair<index of column, std::pair<timestamp64, row id>>
  std::pair<k_int32, std::pair<timestamp64, MetricRowID>> first_row_pair_;
  std::pair<k_int32, std::pair<timestamp64, MetricRowID>> last_row_pair_;
};

/**
 * @brief The iterator class provided to the execution layer.
 */
class TsTableIterator {
 public:
  TsTableIterator() : latch_(LATCH_ID_TSTABLE_ITERATOR_MUTEX) {}
  ~TsTableIterator() {
    for (auto iter : iterators_) {
      delete iter;
    }
  }

  void AddEntityIterator(TsIterator* iter) {
    iterators_.push_back(iter);
  }

  /**
   * @brief Check whether the partition table of entity being queried is disordered.
   */
  bool IsDisordered() {
    return iterators_[current_iter_]->IsDisordered();
  }

  /**
   * @brief The iterator query interface provided to the execution layer, When count is 0, it indicates the end of the query.
   *
   * @param res     the set of returned query results
   * @param count   number of rows of data
   */
  KStatus Next(ResultSet* res, k_uint32* count, timestamp64 ts = INVALID_TS);

 private:
  KLatch latch_;
  size_t current_iter_ = 0;
  // an array of TsIterator objects, where one TsIterator corresponds to the data of a subgroup
  std::vector<TsIterator*> iterators_;
};

}  //  namespace kwdbts
