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

package colexec

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestSelectInInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		filterRow    []int64
		hasNulls     bool
		negate       bool
	}{
		{
			desc:         "Simple in test",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{0}, {1}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       false,
		},
		{
			desc:         "Simple not in test",
			inputTuples:  tuples{{0}, {1}, {2}},
			outputTuples: tuples{{2}},
			filterRow:    []int64{0, 1},
			hasNulls:     false,
			negate:       true,
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  tuples{{nil}, {1}, {2}},
			outputTuples: tuples{{1}},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       false,
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  tuples{{nil}, {1}, {2}},
			outputTuples: tuples{},
			filterRow:    []int64{1},
			hasNulls:     true,
			negate:       true,
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			opConstructor := func(input []Operator) (Operator, error) {
				op := selectInOpInt64{
					OneInputNode: NewOneInputNode(input[0]),
					colIdx:       0,
					filterRow:    c.filterRow,
					negate:       c.negate,
					hasNulls:     c.hasNulls,
				}
				return &op, nil
			}
			if !c.hasNulls || !c.negate {
				runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier, opConstructor)
			} else {
				// When the input tuples already have nulls and we have NOT IN
				// operator, then the nulls injection might not change the output. For
				// example, we have this test case "1 NOT IN (NULL, 1, 2)" with the
				// output of length 0; similarly, we will get the same zero-length
				// output for the corresponding nulls injection test case
				// "1 NOT IN (NULL, NULL, NULL)".
				runTestsWithoutAllNullsInjection(t, []tuples{c.inputTuples}, nil /* typs */, c.outputTuples, orderedVerifier, opConstructor)
			}
		})
	}
}

func benchmarkSelectInInt64(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	col1 := batch.ColVec(0).Int64()

	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i] = -1
		} else {
			col1[i] = 1
		}
	}

	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
		}
	}

	batch.SetLength(coldata.BatchSize())

	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}

	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()
	inOp := &selectInOpInt64{
		OneInputNode: NewOneInputNode(source),
		colIdx:       0,
		filterRow:    []int64{1, 2, 3},
	}
	inOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inOp.Next(ctx)
	}
}

func BenchmarkSelectInInt64(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelectInInt64(b, useSel, hasNulls)
			})
		}
	}
}

func TestProjectInInt64(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	testCases := []struct {
		desc         string
		inputTuples  tuples
		outputTuples tuples
		inClause     string
	}{
		{
			desc:         "Simple in test",
			inputTuples:  tuples{{0}, {1}},
			outputTuples: tuples{{0, true}, {1, true}},
			inClause:     "IN (0, 1)",
		},
		{
			desc:         "Simple not in test",
			inputTuples:  tuples{{2}},
			outputTuples: tuples{{2, true}},
			inClause:     "NOT IN (0, 1)",
		},
		{
			desc:         "In test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, true}, {2, nil}, {nil, nil}},
			inClause:     "IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, nil}, {nil, nil}},
			inClause:     "NOT IN (1, NULL)",
		},
		{
			desc:         "Not in test with NULLs and no nulls in filter",
			inputTuples:  tuples{{1}, {2}, {nil}},
			outputTuples: tuples{{1, false}, {2, true}, {nil, nil}},
			inClause:     "NOT IN (1)",
		},
		{
			desc:         "Test with false values",
			inputTuples:  tuples{{1}, {2}},
			outputTuples: tuples{{1, false}, {2, false}},
			inClause:     "IN (3)",
		},
	}

	for _, c := range testCases {
		t.Run(c.desc, func(t *testing.T) {
			runTests(t, []tuples{c.inputTuples}, c.outputTuples, orderedVerifier,
				func(input []Operator) (Operator, error) {
					expr, err := parser.ParseExpr(fmt.Sprintf("@1 %s", c.inClause))
					if err != nil {
						return nil, err
					}
					p := &mockTypeContext{typs: []types.T{*types.Int, *types.MakeTuple([]types.T{*types.Int})}}
					typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
					if err != nil {
						return nil, err
					}
					spec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: []types.T{*types.Int}}},
						Core: execinfrapb.ProcessorCoreUnion{
							Noop: &execinfrapb.NoopCoreSpec{},
						},
						Post: execinfrapb.PostProcessSpec{
							RenderExprs: []execinfrapb.Expression{
								{Expr: "@1"},
								{LocalExpr: typedExpr},
							},
						},
					}
					args := NewColOperatorArgs{
						Spec:                spec,
						Inputs:              input,
						StreamingMemAccount: testMemAcc,
						// TODO(yuzefovich): figure out how to make the second
						// argument of IN comparison as DTuple not Tuple.
						// TODO(yuzefovich): reuse createTestProjectingOperator
						// once we don't need to provide the processor
						// constructor.
						ProcessorConstructor: rowexec.NewProcessor,
					}
					args.TestingKnobs.UseStreamingMemAccountForBuffering = true
					result, err := NewColOperator(ctx, flowCtx, args)
					if err != nil {
						return nil, err
					}
					return result.Op, nil
				})
		})
	}
}
