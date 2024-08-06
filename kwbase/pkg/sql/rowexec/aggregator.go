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

package rowexec

import (
	"context"
	"fmt"
	"math"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/stringarena"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

type aggregateFuncs []tree.AggregateFunc

func (af aggregateFuncs) close(ctx context.Context) {
	for _, f := range af {
		f.Close(ctx)
	}
}

// aggregatorBase is the foundation of the processor core type that does
// "aggregation" in the SQL sense. It groups rows and computes an aggregate for
// each group. The group is configured using the group key and the aggregator
// can be configured with one or more aggregation functions, as defined in the
// AggregatorSpec_Func enum.
//
// aggregatorBase's output schema is comprised of what is specified by the
// accompanying SELECT expressions.
type aggregatorBase struct {
	execinfra.ProcessorBase

	// runningState represents the state of the aggregator. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState aggregatorState
	input        execinfra.RowSource
	inputDone    bool
	inputTypes   []types.T
	funcs        []*aggregateFuncHolder
	outputTypes  []types.T
	datumAlloc   sqlbase.DatumAlloc
	rowAlloc     sqlbase.EncDatumRowAlloc
	gapfill      gapfilltype
	imputation   []imputationtype
	bucketsAcc   mon.BoundAccount
	aggFuncsAcc  mon.BoundAccount

	// isScalar can only be set if there are no groupCols, and it means that we
	// will generate a result row even if there are no input rows. Used for
	// queries like SELECT MAX(n) FROM t.
	isScalar         bool
	groupCols        []uint32
	orderedGroupCols []uint32
	aggregations     []execinfrapb.AggregatorSpec_Aggregation
	interpolated     bool

	lastOrdGroupCols sqlbase.EncDatumRow
	arena            stringarena.Arena
	row              sqlbase.EncDatumRow
	scratch          []byte

	cancelChecker *sqlbase.CancelChecker

	// keep track of all internal ts columns for last/last_row
	internalTsCols []uint32
}

// gapfilltype is used to store data for functions of time_bucket_gapfill and interpolate.
// It records previous data record and keeps track of gapfilling status along the way
// while we keep reading the next rows.
type gapfilltype struct {
	// prevtime is the timestamp of the previous row
	prevtime time.Time
	// prevgaptime is the last timestamp that we used for gapfilling.
	// we use this value to keep track of the gapfilling progress.
	prevgaptime int64
	// endgapfilling marks the end of gapfilling if true. We need to emit the gapfillingrow
	// at the end of gapfilling
	endgapfilling bool
	// gapfillingrow records the current row if gapfilling is needed. This row is saved temporarily
	// and emitted after gapfilling is finished.
	gapfillingrow sqlbase.EncDatumRow
	// gapfillingbucket records the current buckets if gapfilling is needed.
	gapfillingbucket aggregateFuncs
	// startgapfilling flags the start of gapfilling
	startgapfilling bool
	// linearPos is used for linear methods. It keeps track of the position
	// of a linear interpolation.
	linearPos int
	// linearGap is used for linear methods.  It is the total distance from previous
	// timestamp to current timestamp.
	// linearGap = (int)((currTime-prevTime)/t.Timebucket + 1)
	// The linear interpolation equation is val := ((nextval-prevval)*pos/gap + prevval)
	linearGap int
}

type imputationtype struct {
	// prevValue is used for imputation for prev and linear methods
	prevValue tree.Datum
	// prevValueTmp is used to facilitate the assignment of prevValue
	prevValueTmp tree.Datum
	// nextValue is used for imputation for next and linear methods
	nextValue tree.Datum
}

func (it *imputationtype) GetPrevValueFloat() (float64, error) {
	switch it.prevValue.(type) {
	case *tree.DInt:
		return float64(*it.prevValue.(*tree.DInt)), nil
	case *tree.DFloat:
		return float64(*it.prevValue.(*tree.DFloat)), nil
	case *tree.DDecimal:
		val, err := it.prevValue.(*tree.DDecimal).Float64()
		return val, err
	}
	return 0, errors.Newf("Unexpected type(%T) for value %v", it.prevValue, it.prevValue)
}

func (it *imputationtype) GetNextValueFloat() (float64, error) {
	switch it.nextValue.(type) {
	case *tree.DInt:
		return float64(*it.nextValue.(*tree.DInt)), nil
	case *tree.DFloat:
		return float64(*it.nextValue.(*tree.DFloat)), nil
	case *tree.DDecimal:
		val, err := it.nextValue.(*tree.DDecimal).Float64()
		return val, err
	}
	return 0, errors.Newf("Unexpected type(%T) for value %v", it.nextValue, it.nextValue)
}

// init initializes the aggregatorBase.
//
// trailingMetaCallback is passed as part of ProcStateOpts; the inputs to drain
// are in aggregatorBase.
func (ag *aggregatorBase) init(
	self execinfra.RowSource,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	trailingMetaCallback func(context.Context) []execinfrapb.ProducerMetadata,
) error {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "aggregator-mem")
	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		input = newInputStatCollector(input)
		ag.FinishTrace = ag.outputStatsToTrace
	}
	ag.input = input
	ag.isScalar = spec.IsScalar()
	ag.groupCols = spec.GroupCols
	ag.orderedGroupCols = spec.OrderedGroupCols
	ag.aggregations = spec.Aggregations
	ag.funcs = make([]*aggregateFuncHolder, len(spec.Aggregations))
	ag.outputTypes = make([]types.T, len(spec.Aggregations))
	ag.row = make(sqlbase.EncDatumRow, len(spec.Aggregations))
	ag.bucketsAcc = memMonitor.MakeBoundAccount()
	ag.arena = stringarena.Make(&ag.bucketsAcc)
	ag.aggFuncsAcc = memMonitor.MakeBoundAccount()
	ag.gapfill = gapfilltype{
		prevgaptime:      time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
		endgapfilling:    false, // marks the end of gapfilling if true
		gapfillingrow:    nil,
		gapfillingbucket: nil,
		startgapfilling:  false,
		linearPos:        0,
		linearGap:        0,
	}
	ag.imputation = make([]imputationtype, len(spec.Aggregations))
	// Loop over the select expressions and extract any aggregate functions --
	// non-aggregation functions are replaced with parser.NewIdentAggregate,
	// (which just returns the last value added to them for a bucket) to provide
	// grouped-by values for each bucket.  ag.funcs is updated to contain all
	// the functions which need to be fed values.
	ag.inputTypes = input.OutputTypes()
	for i, aggInfo := range spec.Aggregations {
		if aggInfo.FilterColIdx != nil {
			col := *aggInfo.FilterColIdx
			if col >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("FilterColIdx out of range (%d)", col)
			}
			t := ag.inputTypes[col].Family()
			if t != types.BoolFamily && t != types.UnknownFamily {
				return errors.Errorf(
					"filter column %d must be of boolean type, not %s", *aggInfo.FilterColIdx, t,
				)
			}
		}
		argTypes := make([]types.T, len(aggInfo.ColIdx)+len(aggInfo.Arguments))
		for j, c := range aggInfo.ColIdx {
			if c >= uint32(len(ag.inputTypes)) {
				return errors.Errorf("ColIdx out of range (%d)", aggInfo.ColIdx)
			}
			argTypes[j] = ag.inputTypes[c]
		}

		arguments := make(tree.Datums, len(aggInfo.Arguments))
		for j, argument := range aggInfo.Arguments {
			h := execinfra.ExprHelper{}
			// Pass nil types and row - there are no variables in these expressions.
			if err := h.Init(argument, nil /* types */, flowCtx.EvalCtx); err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			d, err := h.Eval(nil /* row */)
			if err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			argTypes[len(aggInfo.ColIdx)+j] = *d.ResolvedType()
			if err != nil {
				return errors.Wrapf(err, "%s", argument)
			}
			arguments[j] = d
		}

		aggConstructor, retType, err := execinfrapb.GetAggregateInfo(aggInfo.Func, argTypes...)
		if err != nil {
			return err
		}

		ag.funcs[i] = ag.newAggregateFuncHolder(aggConstructor, arguments)
		if aggInfo.Distinct {
			ag.funcs[i].seen = make(map[string]struct{})
		}

		ag.outputTypes[i] = *retType
		ag.imputation[i] = imputationtype{
			prevValue:    tree.DNull,
			prevValueTmp: tree.DNull,
			nextValue:    tree.DNull}
	}

	return ag.ProcessorBase.Init(
		self, post, ag.outputTypes, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			InputsToDrain:        []execinfra.RowSource{ag.input},
			TrailingMetaCallback: trailingMetaCallback,
		},
	)
}

var _ execinfrapb.DistSQLSpanStats = &AggregatorStats{}

const aggregatorTagPrefix = "aggregator."

// Stats implements the SpanStats interface.
func (as *AggregatorStats) Stats() map[string]string {
	inputStatsMap := as.InputStats.Stats(aggregatorTagPrefix)
	inputStatsMap[aggregatorTagPrefix+MaxMemoryTagSuffix] = humanizeutil.IBytes(as.MaxAllocatedMem)
	return inputStatsMap
}

// TsStats is stats of analyse in time series
func (as *AggregatorStats) TsStats() map[int32]map[string]string {
	return nil
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (as *AggregatorStats) StatsForQueryPlan() []string {
	stats := as.InputStats.StatsForQueryPlan("" /* prefix */)

	if as.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(as.MaxAllocatedMem)))
	}

	return stats
}

// TsStatsForQueryPlan key is processorid, value is list of statistics in time series
func (as *AggregatorStats) TsStatsForQueryPlan() map[int32][]string {
	stats := as.InputStats.StatsForQueryPlan("" /* prefix */)
	stats = append(stats,
		fmt.Sprintf("%s:%d", outputRowsQueryPlanSuffix, as.OutputRowNum))
	if as.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", MaxMemoryQueryPlanSuffix, humanizeutil.IBytes(as.MaxAllocatedMem)))
	}
	mapStats := make(map[int32][]string, 0)
	mapStats[0] = stats

	return mapStats
}

func (ag *aggregatorBase) outputStatsToTrace() {
	is, ok := getInputStats(ag.FlowCtx, ag.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(ag.PbCtx()); sp != nil {
		tracing.SetSpanStats(
			sp,
			&AggregatorStats{
				InputStats:      is,
				MaxAllocatedMem: ag.MemMonitor.MaximumBytes(),
			},
		)
	}
}

// ChildCount is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) ChildCount(verbose bool) int {
	if _, ok := ag.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (ag *aggregatorBase) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := ag.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to aggregatorBase is not an execinfra.OpNode")
	}
	panic(fmt.Sprintf("invalid index %d", nth))
}

const (
	// hashAggregatorBucketsInitialLen is a guess on how many "items" the
	// 'buckets' map of hashAggregator has the capacity for initially.
	hashAggregatorBucketsInitialLen = 8
	// hashAggregatorSizeOfBucketsItem is a guess on how much space (in bytes)
	// each item added to 'buckets' map of hashAggregator takes up in the map
	// (i.e. it is memory internal to the map, orthogonal to "key-value" pair
	// that we're adding to the map).
	hashAggregatorSizeOfBucketsItem = 64
)

// hashAggregator is a specialization of aggregatorBase that must keep track of
// multiple grouping buckets at a time.
type hashAggregator struct {
	aggregatorBase

	// buckets is used during the accumulation phase to track the bucket keys
	// that have been seen. After accumulation, the keys are extracted into
	// bucketsIter for iteration.
	buckets     map[string]aggregateFuncs
	bucketsIter []string
	// bucketsLenGrowThreshold is the threshold which, when reached by the
	// number of items in 'buckets', will trigger the update to memory
	// accounting. It will start out at hashAggregatorBucketsInitialLen and
	// then will be doubling in size.
	bucketsLenGrowThreshold int
	// alreadyAccountedFor tracks the number of items in 'buckets' memory for
	// which we have already accounted for.
	alreadyAccountedFor int
}

// orderedAggregator is a specialization of aggregatorBase that only needs to
// keep track of a single grouping bucket at a time.
type orderedAggregator struct {
	aggregatorBase

	// bucket is used during the accumulation phase to aggregate results.
	bucket aggregateFuncs
}

var _ execinfra.Processor = &hashAggregator{}
var _ execinfra.RowSource = &hashAggregator{}
var _ execinfra.OpNode = &hashAggregator{}

const hashAggregatorProcName = "hash aggregator"

var _ execinfra.Processor = &orderedAggregator{}
var _ execinfra.RowSource = &orderedAggregator{}
var _ execinfra.OpNode = &orderedAggregator{}

const orderedAggregatorProcName = "ordered aggregator"

// aggregatorState represents the state of the processor.
type aggregatorState int

const (
	aggStateUnknown aggregatorState = iota
	// aggAccumulating means that rows are being read from the input and used to
	// compute intermediary aggregation results.
	aggAccumulating
	// aggEmittingRows means that accumulation has finished and rows are being
	// sent to the output.
	aggEmittingRows
)

func newAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	if spec.IsRowCount() {
		return newCountAggregator(flowCtx, processorID, input, post, output)
	}
	if len(spec.OrderedGroupCols) == len(spec.GroupCols) {
		return newOrderedAggregator(flowCtx, processorID, spec, input, post, output)
	}

	ag := &hashAggregator{
		buckets:                 make(map[string]aggregateFuncs),
		bucketsLenGrowThreshold: hashAggregatorBucketsInitialLen,
	}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func(context.Context) []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

func newOrderedAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.AggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*orderedAggregator, error) {
	ag := &orderedAggregator{}

	if err := ag.init(
		ag,
		flowCtx,
		processorID,
		spec,
		input,
		post,
		output,
		func(context.Context) []execinfrapb.ProducerMetadata {
			ag.close()
			return nil
		},
	); err != nil {
		return nil, err
	}

	// A new tree.EvalCtx was created during initializing aggregatorBase above
	// and will be used only by this aggregator, so it is ok to update EvalCtx
	// directly.
	ag.EvalCtx.SingleDatumAggMemAccount = &ag.aggFuncsAcc
	return ag, nil
}

// Start is part of the RowSource interface.
func (ag *hashAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, hashAggregatorProcName)
}

// Start is part of the RowSource interface.
func (ag *orderedAggregator) Start(ctx context.Context) context.Context {
	return ag.start(ctx, orderedAggregatorProcName)
}

func (ag *aggregatorBase) start(ctx context.Context, procName string) context.Context {
	ag.input.Start(ctx)
	ctx = ag.StartInternal(ctx, procName)
	ag.cancelChecker = sqlbase.NewCancelChecker(ctx)
	ag.runningState = aggAccumulating
	return ctx
}

func (ag *hashAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.PbCtx(), 2, "exiting aggregator")
		// If we have started emitting rows, bucketsIter will represent which
		// buckets are still open, since buckets are closed once their results are
		// emitted.
		if ag.bucketsIter == nil {
			for _, bucket := range ag.buckets {
				bucket.close(ag.PbCtx())
			}
		} else {
			for _, bucket := range ag.bucketsIter {
				ag.buckets[bucket].close(ag.PbCtx())
			}
		}
		// Make sure to release any remaining memory under 'buckets'.
		ag.buckets = nil
		// Note that we should be closing accounts only after closing all the
		// buckets since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the buckets.
		ag.bucketsAcc.Close(ag.PbCtx())
		ag.aggFuncsAcc.Close(ag.PbCtx())
		ag.MemMonitor.Stop(ag.PbCtx())
	}
}

func (ag *orderedAggregator) close() {
	if ag.InternalClose() {
		log.VEventf(ag.PbCtx(), 2, "exiting aggregator")
		if ag.bucket != nil {
			ag.bucket.close(ag.PbCtx())
		}
		// Note that we should be closing accounts only after closing the
		// bucket since the latter might be releasing some precisely tracked
		// memory, and if we were to close the accounts first, there would be
		// no memory to release for the bucket.
		ag.bucketsAcc.Close(ag.PbCtx())
		ag.aggFuncsAcc.Close(ag.PbCtx())
		ag.MemMonitor.Stop(ag.PbCtx())
	}
}

// matchLastOrdGroupCols takes a row and matches it with the row stored by
// lastOrdGroupCols. It returns true if the two rows are equal on the grouping
// columns, and false otherwise.
func (ag *aggregatorBase) matchLastOrdGroupCols(row sqlbase.EncDatumRow) (bool, error) {
	for _, colIdx := range ag.orderedGroupCols {
		res, err := ag.lastOrdGroupCols[colIdx].Compare(
			&ag.inputTypes[colIdx], &ag.datumAlloc, ag.EvalCtx, &row[colIdx],
		)
		if res != 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *hashAggregator) accumulateRows() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.PbCtx(), 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if len(ag.buckets) < 1 && len(ag.groupCols) == 0 {
		bucket, err := ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		ag.buckets[""] = bucket
	}

	// Note that, for simplicity, we're ignoring the overhead of the slice of
	// strings.
	if err := ag.bucketsAcc.Grow(ag.PbCtx(), int64(len(ag.buckets))*sizeOfString); err != nil {
		ag.MoveToDraining(err)
		return aggStateUnknown, nil, nil
	}
	ag.bucketsIter = make([]string, 0, len(ag.buckets))
	for bucket := range ag.buckets {
		ag.bucketsIter = append(ag.bucketsIter, bucket)
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// accumulateRows continually reads rows from the input and accumulates them
// into intermediary aggregate results. If it encounters metadata, the metadata
// is immediately returned. Subsequent calls of this function will resume row
// accumulation.
func (ag *orderedAggregator) accumulateRows() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	for {
		row, meta := ag.input.Next()
		if meta != nil {
			if meta.Err != nil {
				ag.MoveToDraining(nil /* err */)
				return aggStateUnknown, nil, meta
			}
			return aggAccumulating, nil, meta
		}
		if row == nil {
			log.VEvent(ag.PbCtx(), 1, "accumulation complete")
			ag.inputDone = true
			break
		}

		if ag.lastOrdGroupCols == nil {
			ag.lastOrdGroupCols = ag.rowAlloc.CopyRow(row)
		} else {
			matched, err := ag.matchLastOrdGroupCols(row)
			if err != nil {
				ag.MoveToDraining(err)
				return aggStateUnknown, nil, nil
			}
			if !matched {
				copy(ag.lastOrdGroupCols, row)
				break
			}
		}
		if err := ag.accumulateRow(row); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was
	// aggregated.
	if ag.bucket == nil && ag.isScalar {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
	}

	// Transition to aggEmittingRows, and let it generate the next row/meta.
	return aggEmittingRows, nil, nil
}

// getAggResults returns the new aggregatorState and the results from the
// bucket. The bucket is closed.
func (ag *aggregatorBase) getAggResults(
	bucket aggregateFuncs,
) (aggregatorState, sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	defer bucket.close(ag.PbCtx())
	ag.Out.Gapfill = false
	for i, b := range bucket {
		result, err := b.Result()
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			result = tree.DNull
		}
		ag.outputTypes[i] = *result.ResolvedType()
		ag.row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], result)
	}
	var outRow sqlbase.EncDatumRow
	if !ag.gapfill.startgapfilling {
		outRow = ag.ProcessRowHelper(ag.row)
		if outRow == nil {
			return aggEmittingRows, nil, nil
		}
	}
	// If this is in the middle of gapfilling, we use imputation structure to emit rows for the gaps.
	// row is used to store the gap filling values for aggregates in bucket. We emit the row during
	// gapfilling process. We emit ag.row for original data.
	row := make(sqlbase.EncDatumRow, len(ag.row))
	var curRowData tree.Datum
	var haveGapFilled bool
	haveGapFilled = false
	errorOut := false
	if ag.gapfill.startgapfilling {
		for i, b := range ag.gapfill.gapfillingbucket {
			switch t := b.(type) {
			case *(builtins.TimeBucketAggregate):
				// We check the gap between currTime and last filled timestamp value and update the
				// gapfilling status accordingly.
				newTime := ag.gapfill.prevgaptime + t.Timebucket
				currTime := t.Time.Unix()
				if !(newTime < currTime) {
					ag.gapfill.startgapfilling = false
					ag.gapfill.endgapfilling = true
					break
				}
				// increment prevgaptime by t.Timebucket
				ag.gapfill.prevgaptime = newTime
				timeTmp := timeutil.Unix(newTime, 0)
				// record data in row
				ag.outputTypes[i] = *types.Timestamp
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestamp{Time: timeTmp})
			case *(builtins.TimestamptzBucketAggregate):
				// We check the gap between currTime and last filled timestamp value and update the
				// gapfilling status accordingly.
				newTime := ag.gapfill.prevgaptime + t.Timebucket
				currTime := t.Time.Unix()
				if !(newTime < currTime) {
					ag.gapfill.startgapfilling = false
					ag.gapfill.endgapfilling = true
					break
				}
				ag.gapfill.prevgaptime = newTime
				timeTmp := timeutil.Unix(newTime, 0)
				ag.outputTypes[i] = *types.TimestampTZ
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
				// row[0] is a group by column
				row[0] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
			case *(builtins.ImputationAggregate):
				imputationMethod := t.Exp
				switch imputationMethod {
				case builtins.ConstantIntMethod:
					curRowData = tree.NewDInt(tree.DInt(t.IntConstant))
				case builtins.ConstantFloatMethod:
					curRowData = tree.NewDFloat(tree.DFloat(t.FloatConstant))
				case builtins.PrevMethod:
					curRowData = ag.imputation[i].prevValue
				case builtins.NextMethod:
					curRowData = ag.imputation[i].nextValue
				case builtins.LinearMethod:
					if ag.imputation[i].nextValue == tree.DNull || ag.imputation[i].prevValue == tree.DNull {
						curRowData = tree.DNull
					} else {
						nextval, _ := ag.imputation[i].GetNextValueFloat()
						prevval, _ := ag.imputation[i].GetPrevValueFloat()
						pos := float64(ag.gapfill.linearPos) //TODO: potential unnecessary mem alloc
						gap := float64(ag.gapfill.linearGap)
						val := (nextval-prevval)*pos/gap + prevval
						switch t.OriginalType {
						case types.Float:
							curRowData = tree.NewDFloat(tree.DFloat(val))
						case types.Int:
							curRowData = tree.NewDInt(tree.DInt(math.Round(val)))
						default:
							errorOut = true
						}
						// ag.gapfill.linearPos = ag.gapfill.linearPos + 1
						haveGapFilled = true
					}
				case builtins.NullMethod:
					curRowData = tree.DNull
				default:
					errorOut = true
				}
				if !errorOut {
					row[i] = sqlbase.DatumToEncDatum(curRowData.ResolvedType(), curRowData)
				} else {
					row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], tree.DNull)
				}
			// If we have a normal agg function, we fill it with null value.
			default:
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], tree.DNull)
			}
		}
		if haveGapFilled {
			ag.gapfill.linearPos = ag.gapfill.linearPos + 1
			haveGapFilled = false
		}
		// emit gapfilling row
		if ag.gapfill.startgapfilling && !ag.gapfill.endgapfilling {
			ag.Out.Gapfill = true
			if outRow := ag.ProcessRowHelper(row); outRow != nil {
				ag.Out.Gapfill = false
				return aggEmittingRows, outRow, nil
			}
		}
	}
	// If this is the end of gapfilling, we need to emit the last row, which was saved in imputation struct.
	// This row is original row/data
	if ag.gapfill.endgapfilling {
		ag.Out.Gapfill = true
		for _, b := range ag.gapfill.gapfillingbucket {
			switch b.(type) {
			case *(builtins.ImputationAggregate):
				ag.gapfill.linearGap = 0
				ag.gapfill.linearPos = 0
			}
		}
		if outRow := ag.ProcessRowHelper(ag.gapfill.gapfillingrow); outRow != nil {
			ag.gapfill.endgapfilling = false
			ag.gapfill.gapfillingrow = nil
			ag.gapfill.gapfillingbucket = nil
			ag.Out.Gapfill = false
			return aggEmittingRows, outRow, nil
		}
	}
	// traverse agg in bucket
	for i, b := range bucket {
		result, err := b.Result()
		switch t := b.(type) {
		case *(builtins.TimeBucketAggregate):
			if ag.gapfill.prevtime.IsZero() {
				ag.gapfill.prevtime = timeutil.Unix(t.Time.Unix()-t.Timebucket, 0)
			}
			prevTime := ag.gapfill.prevtime
			newTime := prevTime.Unix() + t.Timebucket
			currTime := t.Time.Unix()
			ag.gapfill.prevtime = t.Time
			ag.gapfill.prevgaptime = newTime - t.Timebucket
			// we check if gapfilling is needed by comparing currTime with Time in the last row
			if newTime < currTime {
				ag.gapfill.linearGap = (int)((currTime-newTime)/t.Timebucket + 1)
				ag.gapfill.linearPos = 1
				timeTmp := timeutil.Unix(newTime, 0)
				ag.gapfill.gapfillingbucket = bucket
				ag.outputTypes[i] = *types.Timestamp
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestamp{Time: timeTmp})
				ag.gapfill.startgapfilling = true
			}
		case *(builtins.TimestamptzBucketAggregate):
			if ag.gapfill.prevtime.IsZero() {
				ag.gapfill.prevtime = timeutil.Unix(t.Time.Unix()-t.Timebucket, 0)
			}
			prevTime := ag.gapfill.prevtime
			newTime := prevTime.Unix() + t.Timebucket
			currTime := t.Time.Unix()
			ag.gapfill.prevtime = t.Time
			ag.gapfill.prevgaptime = newTime - t.Timebucket
			// we check if gapfilling is needed by comparing currTime with Time in the last row
			if newTime < currTime {
				ag.gapfill.linearGap = (int)((currTime-newTime)/t.Timebucket + 1)
				ag.gapfill.linearPos = 1
				timeTmp := timeutil.Unix(newTime, 0)
				ag.gapfill.gapfillingbucket = bucket
				ag.outputTypes[i] = *types.TimestampTZ
				row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], &tree.DTimestampTZ{Time: timeTmp})
				ag.gapfill.startgapfilling = true
			}
		case *(builtins.ImputationAggregate):
			ag.imputation[i].nextValue = result
			ag.imputation[i].prevValue = ag.imputation[i].prevValueTmp
			ag.imputation[i].prevValueTmp = result
		// For normal agg, we use result
		default:
			ag.outputTypes[i] = *result.ResolvedType()
			row[i] = sqlbase.DatumToEncDatum(&ag.outputTypes[i], result)
		}
		if err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		if result == nil {
			// We can't encode nil into an EncDatum, so we represent it with DNull.
			result = tree.DNull
		}
		if _, ok := b.(*builtins.ImputationAggregate); ok {
			ag.outputTypes[i] = *result.ResolvedType()
		}
	}
	// We record the original ag.row in imputation struct and emit row. Marks the start of gapfilling.
	if ag.gapfill.startgapfilling {
		ag.gapfill.gapfillingrow = ag.row
		// The row that triggers interpolation needs to be subtracted from rowIdx.
		ag.Out.ReduceRowIdx(1)
		// When starting interpolation and rowIdx equals maxRowIdx and, aggregator should continue exec.
		ag.State = execinfra.StateRunning
		return aggEmittingRows, nil, nil
	}
	if outRow != nil {
		return aggEmittingRows, outRow, nil
	}
	// We might have switched to draining, we might not have. In case we
	// haven't, aggEmittingRows is accurate. If we have, it will be ignored by
	// the caller.
	return aggEmittingRows, nil, nil
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered the current row out.
func (ag *hashAggregator) emitRow() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if len(ag.bucketsIter) == 0 {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.PbCtx()); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		// Before we create a new 'buckets' map below, we need to "release" the
		// already accounted for memory of the current map.
		ag.bucketsAcc.Shrink(ag.PbCtx(), int64(ag.alreadyAccountedFor)*hashAggregatorSizeOfBucketsItem)
		// Note that, for simplicity, we're ignoring the overhead of the slice of
		// strings.
		ag.bucketsAcc.Shrink(ag.PbCtx(), int64(len(ag.buckets))*sizeOfString)
		ag.bucketsIter = nil
		ag.buckets = make(map[string]aggregateFuncs)
		ag.bucketsLenGrowThreshold = hashAggregatorBucketsInitialLen
		ag.alreadyAccountedFor = 0
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucketsIter[0]
	ag.bucketsIter = ag.bucketsIter[1:]

	// Once we get the results from the bucket, we can delete it from the map.
	// This will allow us to return the memory to the system before the hash
	// aggregator is fully done (which matters when we have many buckets).
	// NOTE: accounting for the memory under aggregate builtins in the bucket
	// is updated in getAggResults (the bucket will be closed), however, we
	// choose to not reduce our estimate of the map's internal footprint
	// because it is error-prone to estimate the new footprint (we don't know
	// whether and when Go runtime will release some of the underlying memory).
	// This behavior is ok, though, since actual usage of buckets will be lower
	// than what we accounted for - in the worst case, the query might hit a
	// memory budget limit and error out when it might actually be within the
	// limit. However, we might be under accounting memory usage in other
	// places, so having some over accounting here might be actually beneficial
	// as a defensive mechanism against OOM crashes.
	state, row, meta := ag.getAggResults(ag.buckets[bucket])
	delete(ag.buckets, bucket)
	return state, row, meta
}

// emitRow constructs an output row from an accumulated bucket and returns it.
//
// emitRow() might move to stateDraining. It might also not return a row if the
// ProcOutputHelper filtered a the current row out.
func (ag *orderedAggregator) emitRow() (
	aggregatorState,
	sqlbase.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if ag.bucket == nil {
		// We've exhausted all of the aggregation buckets.
		if ag.inputDone {
			// The input has been fully consumed. Transition to draining so that we
			// emit any metadata that we've produced.
			ag.MoveToDraining(nil /* err */)
			return aggStateUnknown, nil, nil
		}

		// We've only consumed part of the input where the rows are equal over
		// the columns specified by ag.orderedGroupCols, so we need to continue
		// accumulating the remaining rows.

		if err := ag.arena.UnsafeReset(ag.PbCtx()); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}
		for _, f := range ag.funcs {
			if f.seen != nil {
				f.seen = make(map[string]struct{})
			}
		}

		if err := ag.accumulateRow(ag.lastOrdGroupCols); err != nil {
			ag.MoveToDraining(err)
			return aggStateUnknown, nil, nil
		}

		return aggAccumulating, nil, nil
	}

	bucket := ag.bucket
	if !ag.gapfill.startgapfilling {
		ag.bucket = nil
	}
	return ag.getAggResults(bucket)
}

// Next is part of the RowSource interface.
func (ag *hashAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			ag.runningState, row, meta = ag.emitRow()
		default:
			log.Fatalf(ag.PbCtx(), "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// Next is part of the RowSource interface.
func (ag *orderedAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ag.State == execinfra.StateRunning {
		var row sqlbase.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch ag.runningState {
		case aggAccumulating:
			ag.runningState, row, meta = ag.accumulateRows()
		case aggEmittingRows:
			if ag.gapfill.endgapfilling || ag.gapfill.startgapfilling {
				// adding row after gapfilling
				ag.runningState, row, meta = ag.getAggResults(nil)
			} else {
				ag.runningState, row, meta = ag.emitRow()
			}
		default:
			log.Fatalf(ag.PbCtx(), "unsupported state: %d", ag.runningState)
		}

		if row == nil && meta == nil {
			continue
		}
		return row, meta
	}
	return nil, ag.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *hashAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

// ConsumerClosed is part of the RowSource interface.
func (ag *orderedAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ag.close()
}

func (ag *aggregatorBase) accumulateRowIntoBucket(
	row sqlbase.EncDatumRow, groupKey []byte, bucket aggregateFuncs,
) error {
	var err error
	// Feed the func holders for this bucket the non-grouping datums.
	for i, a := range ag.aggregations {
		if a.FilterColIdx != nil {
			col := *a.FilterColIdx
			if err = row[col].EnsureDecoded(&ag.inputTypes[col], &ag.datumAlloc); err != nil {
				return err
			}
			if row[*a.FilterColIdx].Datum != tree.DBoolTrue {
				// This row doesn't contribute to this aggregation.
				continue
			}
		}
		// Extract the corresponding arguments from the row to feed into the
		// aggregate function.
		// Most functions require at most one argument thus we separate
		// the first argument and allocation of (if applicable) a variadic
		// collection of arguments thereafter.
		var firstArg tree.Datum
		var otherArgs tree.Datums

		if len(a.ColIdx) > 1 {
			otherArgs = make(tree.Datums, len(a.ColIdx)-1)
		}
		isFirstArg := true
		for j, c := range a.ColIdx {
			if err = row[c].EnsureDecoded(&ag.inputTypes[c], &ag.datumAlloc); err != nil {
				return err
			}
			if isFirstArg {
				firstArg = row[c].Datum
				isFirstArg = false
				continue
			}
			otherArgs[j-1] = row[c].Datum
		}

		canAdd := true
		if a.Distinct {
			canAdd, err = ag.funcs[i].isDistinct(
				ag.PbCtx(),
				&ag.datumAlloc,
				groupKey,
				firstArg,
				otherArgs,
			)
			if err != nil {
				return err
			}
		}
		if !canAdd {
			continue
		}

		if err = bucket[i].Add(ag.PbCtx(), firstArg, otherArgs...); err != nil {
			return err
		}
	}
	return nil
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *hashAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	// The encoding computed here determines which bucket the non-grouping
	// datums are accumulated to.
	encoded, err := ag.encode(ag.scratch, row)
	if err != nil {
		return err
	}
	ag.scratch = encoded[:0]

	bucket, ok := ag.buckets[string(encoded)]
	if !ok {
		s, err := ag.arena.AllocBytes(ag.PbCtx(), encoded)
		if err != nil {
			return err
		}
		bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
		ag.buckets[s] = bucket
		if len(ag.buckets) == ag.bucketsLenGrowThreshold {
			toAccountFor := ag.bucketsLenGrowThreshold - ag.alreadyAccountedFor
			if err := ag.bucketsAcc.Grow(ag.PbCtx(), int64(toAccountFor)*hashAggregatorSizeOfBucketsItem); err != nil {
				return err
			}
			ag.alreadyAccountedFor = ag.bucketsLenGrowThreshold
			ag.bucketsLenGrowThreshold *= 2
		}
	}

	return ag.accumulateRowIntoBucket(row, encoded, bucket)
}

// accumulateRow accumulates a single row, returning an error if accumulation
// failed for any reason.
func (ag *orderedAggregator) accumulateRow(row sqlbase.EncDatumRow) error {
	if err := ag.cancelChecker.Check(); err != nil {
		return err
	}

	if ag.bucket == nil {
		var err error
		ag.bucket, err = ag.createAggregateFuncs()
		if err != nil {
			return err
		}
	}

	return ag.accumulateRowIntoBucket(row, nil /* groupKey */, ag.bucket)
}

type aggregateFuncHolder struct {
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc

	// arguments is the list of constant (non-aggregated) arguments to the
	// aggregate, for instance, the separator in string_agg.
	arguments tree.Datums

	group *aggregatorBase
	seen  map[string]struct{}
	arena *stringarena.Arena
}

const (
	sizeOfString         = int64(unsafe.Sizeof(""))
	sizeOfAggregateFuncs = int64(unsafe.Sizeof(aggregateFuncs{}))
	sizeOfAggregateFunc  = int64(unsafe.Sizeof(tree.AggregateFunc(nil)))
)

func (ag *aggregatorBase) newAggregateFuncHolder(
	create func(*tree.EvalContext, tree.Datums) tree.AggregateFunc, arguments tree.Datums,
) *aggregateFuncHolder {
	return &aggregateFuncHolder{
		create:    create,
		group:     ag,
		arena:     &ag.arena,
		arguments: arguments,
	}
}

// isDistinct returns whether this aggregateFuncHolder has not already seen the
// encoding of grouping columns and argument columns. It should be used *only*
// when we have DISTINCT aggregation so that we can aggregate only the "first"
// row in the group.
func (a *aggregateFuncHolder) isDistinct(
	ctx context.Context,
	alloc *sqlbase.DatumAlloc,
	prefix []byte,
	firstArg tree.Datum,
	otherArgs tree.Datums,
) (bool, error) {
	// Allocate one EncDatum that will be reused when encoding every argument.
	ed := sqlbase.EncDatum{Datum: firstArg}
	encoded, err := ed.Fingerprint(firstArg.ResolvedType(), alloc, prefix)
	if err != nil {
		return false, err
	}
	if otherArgs != nil {
		for _, arg := range otherArgs {
			ed.Datum = arg
			encoded, err = ed.Fingerprint(arg.ResolvedType(), alloc, encoded)
			if err != nil {
				return false, err
			}
		}
	}

	if _, ok := a.seen[string(encoded)]; ok {
		// We have already seen a row with such combination of grouping and
		// argument columns.
		return false, nil
	}
	s, err := a.arena.AllocBytes(ctx, encoded)
	if err != nil {
		return false, err
	}
	a.seen[s] = struct{}{}
	return true, nil
}

// encode returns the encoding for the grouping columns, this is then used as
// our group key to determine which bucket to add to.
func (ag *aggregatorBase) encode(
	appendTo []byte, row sqlbase.EncDatumRow,
) (encoding []byte, err error) {
	for _, colIdx := range ag.groupCols {
		appendTo, err = row[colIdx].Fingerprint(
			&ag.inputTypes[colIdx], &ag.datumAlloc, appendTo)
		if err != nil {
			return appendTo, err
		}
	}
	return appendTo, nil
}

func (ag *aggregatorBase) createAggregateFuncs() (aggregateFuncs, error) {
	if err := ag.bucketsAcc.Grow(ag.PbCtx(), sizeOfAggregateFuncs+sizeOfAggregateFunc*int64(len(ag.funcs))); err != nil {
		return nil, err
	}
	bucket := make(aggregateFuncs, len(ag.funcs))
	//var reserve int
	for i, f := range ag.funcs {
		agg := f.create(ag.EvalCtx, f.arguments)
		if err := ag.bucketsAcc.Grow(ag.PbCtx(), agg.Size()); err != nil {
			return nil, err
		}
		bucket[i] = agg
	}
	if !ag.interpolated {
		for i := 0; i < len(bucket); i++ {
			if imputationbucket, ok := bucket[i].(*builtins.ImputationAggregate); ok {
				imputationbucket.Aggfunc = bucket[i+1]
				bucket = append(bucket[:i+1], bucket[i+2:]...)
				ag.aggregations = append(ag.aggregations[:i+1], ag.aggregations[i+2:]...)
				ag.outputTypes = append(ag.outputTypes[:i+1], ag.outputTypes[i+2:]...)
				ag.interpolated = true
			}
		}
	} else {
		for i := 0; i < len(bucket); i++ {
			if imputationbucket, ok := bucket[i].(*builtins.ImputationAggregate); ok {
				imputationbucket.Aggfunc = bucket[i+1]
				bucket = append(bucket[:i+1], bucket[i+2:]...)
			}
		}
	}
	return bucket, nil
}
