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

#include "ee_op_spec_utils.h"
#include "ee_op_engine_utils.h"
#include "ee_sort_op.h"

namespace kwdbts {

class TestOpBase {
 public:
  TestOpBase() = default;

  virtual ~TestOpBase() {
    delete iter_;
    iter_ = nullptr;
  }

  TABLE* table_{nullptr};
  BaseOperator* iter_{nullptr};

  TSPostProcessSpec post_;
};

//class TestTagReader : public TestOpBase {
// public:
//  TestTagReader(kwdbContext_p ctx, k_uint32 table_id) {
//    PrepareTagReaderSpec(spec_, table_id);
//    table_ = KNEW TABLE(1, table_id);
//    table_->Init(ctx, &spec_);
//    iter_ = NewIterator<TagScanIterator>(&spec_, &post_, table_);
//  }
//
// private:
//  TSTagReaderSpec spec_;
//};
//
//class TestTableReader : public TestOpBase {
// public:
//  TestTableReader(kwdbContext_p ctx, k_uint32 table_id) : tag_reader_(ctx, table_id) {
//    PrepareReaderSpec(spec_, table_id);
//    PrepareReaderTSPostProcessSpec(post_);
//    table_ = tag_reader_.table_;
//    iter_ = NewIterator<TableScanIterator>(&spec_, &post_, table_, tag_reader_.iter_);
//  }
//
// private:
//  TestTagReader tag_reader_;
//  TSReaderSpec spec_;
//};
//
//class TestAggregate : public TestOpBase {
// public:
//  TestAggregate(kwdbContext_p ctx, k_uint32 table_id) : table_reader_(ctx, table_id) {
//    PrepareAggSpecs(spec_, post_);
//    table_ = table_reader_.table_;
//    iter_ = NewIterator<HashAggregateIterator>(table_reader_.iter_, &spec_,
//                                               &post_, table_);
//  }
//
// private:
//  TestTableReader table_reader_;
//  TSAggregatorSpec spec_;
//};
//
//class TestMergeAgg : public TestOpBase {
// public:
//  explicit TestMergeAgg(TestOpBase* iter) {
//    childIter_ = iter;
//    PrepareMergeSpecs(spec_, post_);
//    table_ = childIter_->table_;
//    iter_ =
//        NewIterator<CMergeAggIterator>(childIter_->iter_, &spec_, &post_, table_);
//    // iter_->Init(ctx);
//    // iter_->Init(ctx);
//  }
//
// private:
//  TestOpBase* childIter_;
//  TSSynchronizerSpec spec_;
//};
//
//class TestMergeThrough : public TestOpBase {
// public:
//  TestMergeThrough(kwdbContext_p ctx, k_uint32 table_id) : table_reader_(ctx, table_id) {
//    PrepareMergeSpecs(spec_, post_);
//    table_ = table_reader_.table_;
//
//    iter_ = NewIterator<CMergeThroughIterator>(table_reader_.iter_, &spec_,
//                                               &post_, table_);
//  }
//
// private:
//  TestTableReader table_reader_;
//  TSSynchronizerSpec spec_;
//};
//
//
//class TestSortOp : public TestOpBase {
// public:
//  TestSortOp(kwdbContext_p ctx, k_uint32 table_id) : merge_through_(ctx, table_id) {
//    PrepareSortSpecs4Operator(spec_, post_);
//    table_ = merge_through_.table_;
//    iter_ = NewIterator<SortOperator>(merge_through_.iter_, &spec_, &post_, table_);
//  }
//
// private:
//  TestMergeThrough merge_through_;
//  TSSorterSpec spec_;
//};
}  // namespace kwdbts
