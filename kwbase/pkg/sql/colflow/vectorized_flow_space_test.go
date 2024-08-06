// Copyright 2019 The Cockroach Authors.
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

package colflow_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestVectorizeInternalMemorySpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
		EvalCtx: &evalCtx,
	}

	oneInput := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
	}
	twoInputs := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
		{ColumnTypes: []types.T{*types.Int}},
	}

	testCases := []struct {
		desc string
		spec *execinfrapb.ProcessorSpec
	}{
		{
			desc: "CASE",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Noop: &execinfrapb.NoopCoreSpec{},
				},
				Post: execinfrapb.PostProcessSpec{
					RenderExprs: []execinfrapb.Expression{{Expr: "CASE WHEN @1 = 1 THEN 1 ELSE 2 END"}},
				},
			},
		},
		{
			desc: "MERGE JOIN",
			spec: &execinfrapb.ProcessorSpec{
				Input: twoInputs,
				Core: execinfrapb.ProcessorCoreUnion{
					MergeJoiner: &execinfrapb.MergeJoinerSpec{},
				},
			},
		},
	}

	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, success), func(t *testing.T) {
				inputs := []colexec.Operator{colexec.NewZeroOp(nil)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, colexec.NewZeroOp(nil))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				if success {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := colexec.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              inputs,
					StreamingMemAccount: &acc,
				}
				args.TestingKnobs.UseStreamingMemAccountForBuffering = true
				result, err := colexec.NewColOperator(ctx, flowCtx, args)
				if err != nil {
					t.Fatal(err)
				}
				err = acc.Grow(ctx, int64(result.InternalMemUsage))
				if success {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}

func TestVectorizeAllocatorSpaceError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
		EvalCtx: &evalCtx,
	}

	oneInput := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
	}
	twoInputs := []execinfrapb.InputSyncSpec{
		{ColumnTypes: []types.T{*types.Int}},
		{ColumnTypes: []types.T{*types.Int}},
	}

	testCases := []struct {
		desc string
		spec *execinfrapb.ProcessorSpec
		// spillingSupported, if set to true, indicates that disk spilling for the
		// operator is supported and we expect success only.
		spillingSupported bool
	}{
		{
			desc: "SORTER",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Sorter: &execinfrapb.SorterSpec{
						OutputOrdering: execinfrapb.Ordering{
							Columns: []execinfrapb.Ordering_Column{
								{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
							},
						},
					},
				},
			},
			spillingSupported: true,
		},
		{
			desc: "HASH AGGREGATOR",
			spec: &execinfrapb.ProcessorSpec{
				Input: oneInput,
				Core: execinfrapb.ProcessorCoreUnion{
					Aggregator: &execinfrapb.AggregatorSpec{
						Type: execinfrapb.AggregatorSpec_SCALAR,
						Aggregations: []execinfrapb.AggregatorSpec_Aggregation{
							{
								Func:   execinfrapb.AggregatorSpec_MAX,
								ColIdx: []uint32{0},
							},
						},
					},
				},
			},
		},
		{
			desc: "HASH JOINER",
			spec: &execinfrapb.ProcessorSpec{
				Input: twoInputs,
				Core: execinfrapb.ProcessorCoreUnion{
					HashJoiner: &execinfrapb.HashJoinerSpec{
						LeftEqColumns:  []uint32{0},
						RightEqColumns: []uint32{0},
					},
				},
			},
			spillingSupported: true,
		},
	}

	batch := testAllocator.NewMemBatchWithSize(
		[]coltypes.T{coltypes.Int64}, 1, /* size */
	)
	for _, tc := range testCases {
		for _, success := range []bool{true, false} {
			expectNoMemoryError := success || tc.spillingSupported
			t.Run(fmt.Sprintf("%s-success-expected-%t", tc.desc, expectNoMemoryError), func(t *testing.T) {
				inputs := []colexec.Operator{colexec.NewRepeatableBatchSource(testAllocator, batch)}
				if len(tc.spec.Input) > 1 {
					inputs = append(inputs, colexec.NewRepeatableBatchSource(testAllocator, batch))
				}
				memMon := mon.MakeMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, st)
				flowCtx.Cfg.TestingKnobs = execinfra.TestingKnobs{}
				if expectNoMemoryError {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
					if !success {
						// These are the cases that we expect in-memory operators to hit a
						// memory error. To enable testing this case, force disk spills. We
						// do this in this if branch to allow the external algorithms to use
						// an unlimited monitor.
						flowCtx.Cfg.TestingKnobs.ForceDiskSpill = true
					}
				} else {
					memMon.Start(ctx, nil, mon.MakeStandaloneBudget(1))
					flowCtx.Cfg.TestingKnobs.MemoryLimitBytes = 1
				}
				defer memMon.Stop(ctx)
				acc := memMon.MakeBoundAccount()
				defer acc.Close(ctx)
				args := colexec.NewColOperatorArgs{
					Spec:                tc.spec,
					Inputs:              inputs,
					StreamingMemAccount: &acc,
					FDSemaphore:         colexec.NewTestingSemaphore(256),
				}
				// The disk spilling infrastructure relies on different memory
				// accounts, so if the spilling is supported, we do *not* want to use
				// streaming memory account.
				args.TestingKnobs.UseStreamingMemAccountForBuffering = !tc.spillingSupported
				var (
					result colexec.NewColOperatorResult
					err    error
				)
				// The memory error can occur either during planning or during
				// execution, and we want to actually execute the "query" only
				// if there was no error during planning. That is why we have
				// two separate panic-catchers.
				if err = execerror.CatchVectorizedRuntimeError(func() {
					result, err = colexec.NewColOperator(ctx, flowCtx, args)
					require.NoError(t, err)
				}); err == nil {
					err = execerror.CatchVectorizedRuntimeError(func() {
						result.Op.Init()
						result.Op.Next(ctx)
						result.Op.Next(ctx)
					})
				}
				for _, memAccount := range result.OpAccounts {
					memAccount.Close(ctx)
				}
				for _, memMonitor := range result.OpMonitors {
					memMonitor.Stop(ctx)
				}
				if expectNoMemoryError {
					require.NoError(t, err, "expected success, found: ", err)
				} else {
					require.Error(t, err, "expected memory error, found nothing")
				}
			})
		}
	}
}
