// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package colexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// TODO(yuzefovich): reading the data through a pair of colBatchScan and
// materializer turns out to be more efficient than through a table reader (at
// the moment, the exception is the case of reading very small number of rows
// because we still pre-allocate batches of 1024 size). Once we can control the
// initial size of pre-allocated batches (probably via a batch allocator), we
// should get rid off table readers entirely. We will have to be careful about
// propagating the metadata though.

// colBatchScan is the exec.Operator implementation of TableReader. It reads a table
// from kv, presenting it as coldata.Batches via the exec.Operator interface.
type colBatchScan struct {
	ZeroInputNode
	// mu protects from concurrent DrainMeta and Next calls. This is unfortunate
	// behavior present in 20.1 but fixed in 20.2.
	mu        syncutil.Mutex
	spans     roachpb.Spans
	flowCtx   *execinfra.FlowCtx
	rf        *cFetcher
	limitHint int64
	ctx       context.Context
	// maxResults is non-zero if there is a limit on the total number of rows
	// that the colBatchScan will read.
	maxResults uint64
	// init is true after Init() has been called.
	init bool
}

var _ Operator = &colBatchScan{}

func (s *colBatchScan) Init() {
	s.ctx = context.Background()
	s.init = true

	limitBatches := execinfra.ScanShouldLimitBatches(s.maxResults, s.limitHint, s.flowCtx)

	if err := s.rf.StartScan(
		s.ctx, s.flowCtx.Txn, s.spans,
		limitBatches, s.limitHint, s.flowCtx.TraceKV,
	); err != nil {
		execerror.VectorizedInternalPanic(err)
	}
}

func (s *colBatchScan) Next(ctx context.Context) coldata.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	bat, err := s.rf.NextBatch(ctx)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	if bat.Selection() != nil {
		execerror.VectorizedInternalPanic("unexpectedly a selection vector is set on the batch coming from CFetcher")
	}
	return bat
}

// DrainMeta is part of the MetadataSource interface.
func (s *colBatchScan) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.init {
		// In some pathological queries like `SELECT 1 FROM t HAVING true`, Init()
		// and Next() may never get called. Return early to avoid using an
		// uninitialized fetcher.
		return nil
	}
	var trailingMeta []execinfrapb.ProducerMetadata
	if !s.flowCtx.Local {
		ranges := execinfra.MisplannedRanges(ctx, s.rf.GetRangesInfo(), s.flowCtx.NodeID)
		if ranges != nil {
			trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{Ranges: ranges})
		}
	}
	if tfs := execinfra.GetLeafTxnFinalState(ctx, s.flowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta, execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs})
	}
	return trailingMeta
}

// newColBatchScan creates a new colBatchScan operator.
func newColBatchScan(
	allocator *Allocator,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.TableReaderSpec,
	post *execinfrapb.PostProcessSpec,
) (*colBatchScan, error) {
	if flowCtx.NodeID == 0 {
		return nil, errors.Errorf("attempting to create a colBatchScan with uninitialized NodeID")
	}

	limitHint := execinfra.LimitHint(spec.LimitHint, post)

	returnMutations := spec.Visibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
	typs := spec.Table.ColumnTypesWithMutations(returnMutations)
	helper := execinfra.ProcOutputHelper{}
	if err := helper.Init(
		post,
		typs,
		flowCtx.NewEvalCtx(),
		nil,
	); err != nil {
		return nil, err
	}

	neededColumns := helper.NeededColumns()

	columnIdxMap := spec.Table.ColumnIdxMapWithMutations(returnMutations)
	fetcher := cFetcher{}
	if _, _, err := initCRowFetcher(
		allocator, &fetcher, &spec.Table, int(spec.IndexIdx), columnIdxMap, spec.Reverse,
		neededColumns, spec.IsCheck, spec.Visibility, spec.LockingStrength,
	); err != nil {
		return nil, err
	}

	nSpans := len(spec.Spans)
	spans := make(roachpb.Spans, nSpans)
	for i := range spans {
		spans[i] = spec.Spans[i].Span
	}
	return &colBatchScan{
		spans:      spans,
		flowCtx:    flowCtx,
		rf:         &fetcher,
		limitHint:  limitHint,
		maxResults: spec.MaxResults,
	}, nil
}

// initCRowFetcher initializes a row.cFetcher. See initRowFetcher.
func initCRowFetcher(
	allocator *Allocator,
	fetcher *cFetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	scanVisibility execinfrapb.ScanVisibility,
	lockStr sqlbase.ScanLockingStrength,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		allocator, reverseScan, lockStr, true /* returnRangeInfo */, isCheck, tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
