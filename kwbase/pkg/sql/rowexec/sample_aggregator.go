// Copyright 2017 The Cockroach Authors.
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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// A sample aggregator processor aggregates results from multiple sampler
// processors. See SampleAggregatorSpec for more details.
type sampleAggregator struct {
	execinfra.ProcessorBase

	spec    *execinfrapb.SampleAggregatorSpec
	input   execinfra.RowSource
	inTypes []types.T
	sr      stats.SampleReservoir

	// memAcc accounts for memory accumulated throughout the life of the
	// sampleAggregator.
	memAcc mon.BoundAccount

	// tempMemAcc is used to account for memory that is allocated temporarily
	// and released before the sampleAggregator is finished.
	tempMemAcc mon.BoundAccount

	tableID     sqlbase.ID
	sampledCols []sqlbase.ColumnID
	sketches    []sketchInfo

	// Input column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sketchCol    int
}

var _ execinfra.Processor = &sampleAggregator{}

const sampleAggregatorProcName = "sample aggregator"

// SampleAggregatorProgressInterval is the frequency at which the
// SampleAggregator processor will report progress. It is mutable for testing.
var SampleAggregatorProgressInterval = 5 * time.Second

func newSampleAggregator(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SampleAggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (*sampleAggregator, error) {
	for _, s := range spec.Sketches {
		if len(s.Columns) == 0 {
			return nil, errors.Errorf("no columns")
		}
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
		if s.GenerateHistogram && s.HistogramMaxBuckets == 0 {
			return nil, errors.Errorf("histogram max buckets not specified")
		}
		if s.GenerateHistogram && len(s.Columns) != 1 {
			return nil, errors.Errorf("histograms require one column")
		}
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will disable histogram collection if this limit is not
	// enough.
	memMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "sample-aggregator-mem")
	rankCol := len(input.OutputTypes()) - 5
	s := &sampleAggregator{
		spec:         spec,
		input:        input,
		inTypes:      input.OutputTypes(),
		memAcc:       memMonitor.MakeBoundAccount(),
		tempMemAcc:   memMonitor.MakeBoundAccount(),
		tableID:      spec.TableID,
		sampledCols:  spec.SampledColumnIDs,
		sketches:     make([]sketchInfo, len(spec.Sketches)),
		rankCol:      rankCol,
		sketchIdxCol: rankCol + 1,
		numRowsCol:   rankCol + 2,
		numNullsCol:  rankCol + 3,
		sketchCol:    rankCol + 4,
	}

	var sampleCols util.FastIntSet
	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:     spec.Sketches[i],
			sketch:   hyperloglog.New14(),
			numNulls: 0,
			numRows:  0,
		}
		if spec.Sketches[i].GenerateHistogram {
			sampleCols.Add(int(spec.Sketches[i].Columns[0]))
		}
	}

	s.sr.Init(int(spec.SampleSize), input.OutputTypes()[:rankCol], &s.memAcc, sampleCols)

	if err := s.Init(
		nil, post, []types.T{}, flowCtx, processorID, output, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sampleAggregator) pushTrailingMeta(ctx context.Context) {
	execinfra.SendTraceData(ctx, s.Out.Output())
}

// Run is part of the Processor interface.
func (s *sampleAggregator) Run(ctx context.Context) {
	s.input.Start(ctx)
	s.StartInternal(ctx, sampleAggregatorProcName)

	earlyExit, err := s.mainLoop(s.PbCtx())
	if err != nil {
		execinfra.DrainAndClose(s.PbCtx(), s.Out.Output(), err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(s.PbCtx())
		s.input.ConsumerClosed()
		s.Out.Close()
	}
	s.MoveToDraining(nil /* err */)
}

// RunTS is part of the Processor interface.
func (s *sampleAggregator) RunTS(ctx context.Context) {
	s.input.Start(ctx)
	s.StartInternal(ctx, sampleAggregatorProcName)

	earlyExit, err := s.mainLoop(s.PbCtx())
	if err != nil {
		execinfra.DrainAndClose(s.PbCtx(), s.Out.Output(), err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(s.PbCtx())
		s.input.ConsumerClosed()
		s.Out.Close()
	}
	s.MoveToDraining(nil /* err */)
}

func (s *sampleAggregator) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.PbCtx())
		s.tempMemAcc.Close(s.PbCtx())
		s.MemMonitor.Stop(s.PbCtx())
	}
}

func (s *sampleAggregator) mainLoop(ctx context.Context) (earlyExit bool, err error) {
	var job *jobs.Job
	jobID := s.spec.JobID
	// Some tests run this code without a job, so check if the jobID is 0.
	if jobID != 0 {
		job, err = s.FlowCtx.Cfg.JobRegistry.LoadJob(ctx, s.spec.JobID)
		if err != nil {
			return false, err
		}
	}

	var tableType tree.TableType
	err = s.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tbl, err := sqlbase.GetTableDescFromID(ctx, txn, s.tableID)
		if err != nil {
			log.Errorf(ctx, "Failed to get table desc when create statistic : %v", err)
			return err
		}
		tableType = tbl.TableType
		return nil
	})
	if err != nil {
		log.Errorf(ctx, "Failed to get table desc when collect statistic : %v", err)
		return false, err
	}

	lastReportedFractionCompleted := float32(-1)
	// Report progress (0 to 1).
	progFn := func(fractionCompleted float32) error {
		if jobID == 0 {
			return nil
		}
		// If it changed by less than 1%, just check for cancellation (which is more
		// efficient).
		if fractionCompleted < 1.0 && fractionCompleted < lastReportedFractionCompleted+0.01 {
			return job.CheckStatus(ctx)
		}
		lastReportedFractionCompleted = fractionCompleted
		return job.FractionProgressed(ctx, jobs.FractionUpdater(fractionCompleted))
	}

	var rowsProcessed uint64
	progressUpdates := util.Every(SampleAggregatorProgressInterval)
	var da sqlbase.DatumAlloc
	var tmpSketch hyperloglog.Sketch
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if meta.SamplerProgress != nil {
				rowsProcessed += meta.SamplerProgress.RowsProcessed
				if progressUpdates.ShouldProcess(timeutil.Now()) {
					// Periodically report fraction progressed and check that the job has
					// not been paused or canceled.
					var fractionCompleted float32
					if s.spec.RowsExpected > 0 {
						fractionCompleted = float32(float64(rowsProcessed) / float64(s.spec.RowsExpected))
						const maxProgress = 0.99
						if fractionCompleted > maxProgress {
							// Since the total number of rows expected is just an estimate,
							// don't report more than 99% completion until the very end.
							fractionCompleted = maxProgress
						}
					}

					if err := progFn(fractionCompleted); err != nil {
						return false, err
					}
				}
				if meta.SamplerProgress.HistogramDisabled {
					// One of the sampler processors probably ran out of memory while
					// collecting histogram samples. Disable sample collection so we
					// don't create a biased histogram.
					s.sr.Disable()
				}
			} else if !emitHelper(ctx, &s.Out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		// The row is either:
		//  - a sampled row, which has NULLs on all columns from sketchIdxCol
		//    onward, or
		//  - a sketch row, which has all NULLs on all columns before sketchIdxCol.
		if row[s.sketchIdxCol].IsNull() {
			// This must be a sampled row.
			rank, err := row[s.rankCol].GetInt()
			if err != nil {
				return false, errors.NewAssertionErrorWithWrappedErrf(err, "decoding rank column")
			}
			// Retain the rows with the top ranks.
			if err := s.sr.SampleRow(ctx, s.EvalCtx, row[:s.rankCol], uint64(rank)); err != nil {
				if code := pgerror.GetPGCode(err); code != pgcode.OutOfMemory {
					return false, err
				}
				// We hit an out of memory error. Clear the sample reservoir and
				// disable histogram sample collection.
				s.sr.Disable()
				log.Info(ctx, "disabling histogram collection due to excessive memory utilization")
			}
			continue
		}
		// This is a sketch row.
		sketchIdx, err := row[s.sketchIdxCol].GetInt()
		if err != nil {
			return false, err
		}
		if sketchIdx < 0 || sketchIdx > int64(len(s.sketches)) {
			return false, errors.Errorf("invalid sketch index %d", sketchIdx)
		}

		numRows, err := row[s.numRowsCol].GetInt()
		if err != nil {
			return false, err
		}
		s.sketches[sketchIdx].numRows += numRows

		numNulls, err := row[s.numNullsCol].GetInt()
		if err != nil {
			return false, err
		}
		s.sketches[sketchIdx].numNulls += numNulls
		// Normal columns need to calculate distinct-count
		// Primary Tag is a foreign key index, no need to calculate distinct-count
		if canEncodeBytes(tableType, s.sketches[sketchIdx].spec, s.inTypes[sketchIdx].Family()) {
			// Decode the sketch.
			if err := row[s.sketchCol].EnsureDecoded(&s.inTypes[s.sketchCol], &da); err != nil {
				return false, err
			}
			d := row[s.sketchCol].Datum
			if d == tree.DNull {
				return false, errors.AssertionFailedf("NULL sketch data")
			}
			if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
				return false, err
			}
			if err := s.sketches[sketchIdx].sketch.Merge(&tmpSketch); err != nil {
				return false, errors.NewAssertionErrorWithWrappedErrf(err, "merging sketch data")
			}
		}
	}
	// Report progress one last time so we don't write results if the job was
	// canceled.
	if err = progFn(1.0); err != nil {
		return false, err
	}
	return false, s.writeResults(ctx)
}

// writeResults inserts the new statistics into system.table_statistics.
func (s *sampleAggregator) writeResults(ctx context.Context) error {
	// Turn off tracing so these writes don't affect the results of EXPLAIN
	// ANALYZE.
	if span := opentracing.SpanFromContext(ctx); span != nil && tracing.IsRecording(span) {
		// TODO(rytaft): this also hides writes in this function from SQL session
		// traces.
		ctx = opentracing.ContextWithSpan(ctx, nil)
	}

	// TODO(andrei): This method would benefit from a session interface on the
	// internal executor instead of doing this weird thing where it uses the
	// internal executor to execute one statement at a time inside a db.Txn()
	// closure.
	if err := s.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var distinctCount int64
		for _, si := range s.sketches {
			// Primary tag is a foreign key index, distinctCount is same as rowCount
			if len(si.spec.ColumnTypes) > 0 && (si.spec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_PTAG) && si.spec.HasAllPTag) {
				distinctCount = si.numRows
			} else {
				distinctCount = int64(si.sketch.Estimate())
			}
			var histogram *stats.HistogramData
			if si.spec.GenerateHistogram && len(s.sr.Get()) != 0 {
				colIdx := int(si.spec.Columns[0])
				typ := &s.inTypes[colIdx]

				h, err := s.generateHistogram(
					ctx,
					s.EvalCtx,
					s.sr.Get(),
					colIdx,
					typ,
					si.numRows-si.numNulls,
					distinctCount,
					int(si.spec.HistogramMaxBuckets),
				)
				if err != nil {
					return err
				}
				histogram = &h
			}

			columnIDs := make([]sqlbase.ColumnID, len(si.spec.Columns))
			for i, c := range si.spec.Columns {
				columnIDs[i] = s.sampledCols[c]
			}

			// Delete old stats that have been superseded.
			if err := stats.DeleteOldStatsForColumns(
				ctx,
				s.FlowCtx.Cfg.Executor,
				txn,
				s.tableID,
				columnIDs,
			); err != nil {
				return err
			}

			// Insert the new stat.
			if err := stats.InsertNewStat(
				ctx,
				s.FlowCtx.Cfg.Executor,
				txn,
				s.tableID,
				si.spec.StatName,
				columnIDs,
				si.numRows,
				distinctCount,
				si.numNulls,
				histogram,
			); err != nil {
				return err
			}

			// Release any memory temporarily used for this statistic.
			s.tempMemAcc.Clear(ctx)
		}

		return nil
	}); err != nil {
		return err
	}

	// Gossip refresh of the stat caches for this table.
	return stats.GossipTableStatAdded(s.FlowCtx.Cfg.Gossip, s.tableID)
}

// generateHistogram returns a histogram (on a given column) from a set of
// samples.
// numRows is the total number of rows from which values were sampled
// (excluding rows that have NULL values on the histogram column).
func (s *sampleAggregator) generateHistogram(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	samples []stats.SampledRow,
	colIdx int,
	colType *types.T,
	numRows int64,
	distinctCount int64,
	maxBuckets int,
) (stats.HistogramData, error) {
	// Account for the memory we'll use copying the samples into values.
	if err := s.tempMemAcc.Grow(ctx, sizeOfDatum*int64(len(samples))); err != nil {
		return stats.HistogramData{}, err
	}
	values := make(tree.Datums, 0, len(samples))

	var da sqlbase.DatumAlloc
	for _, sample := range samples {
		ed := &sample.Row[colIdx]
		// Ignore NULLs (they are counted separately).
		if !ed.IsNull() {
			beforeSize := ed.Datum.Size()
			if err := ed.EnsureDecoded(colType, &da); err != nil {
				return stats.HistogramData{}, err
			}
			afterSize := ed.Datum.Size()

			// Perform memory accounting. This memory is not added to the temporary
			// account since it won't be released until the sampleAggregator is
			// destroyed.
			if afterSize > beforeSize {
				if err := s.memAcc.Grow(ctx, int64(afterSize-beforeSize)); err != nil {
					return stats.HistogramData{}, err
				}
			}

			values = append(values, ed.Datum)
		}
	}
	return stats.EquiDepthHistogram(evalCtx, colType, values, numRows, distinctCount, maxBuckets)
}

// canEncodeBytes is used to determine whether you need to encode bytes for distinct-count
func canEncodeBytes(
	tableType tree.TableType, sketchSpec execinfrapb.SketchSpec, family types.Family,
) bool {
	// Check if the table type is RELATIONAL_TABLE, which directly allows encoding
	if tableType == tree.RelationalTable {
		return true
	}

	// Check if there are any column types defined
	if len(sketchSpec.ColumnTypes) == 0 {
		return false
	}

	// Check if the first column type is one of the specified types and meets the condition for TYPE_PTAG
	isSpecifiedType := sketchSpec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_DATA) ||
		sketchSpec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_TAG) ||
		(sketchSpec.ColumnTypes[0] == uint32(sqlbase.ColumnType_TYPE_PTAG) && !sketchSpec.HasAllPTag)

	// Check if the family is one of the allowed types
	isAllowedFamily := family == types.IntFamily || family == types.TimestampTZFamily ||
		family == types.TimestampFamily || family == types.FloatFamily ||
		family == types.StringFamily || family == types.BytesFamily || family == types.BoolFamily

	// Return true if both the type and family checks pass
	return isSpecifiedType && isAllowedFamily
}
