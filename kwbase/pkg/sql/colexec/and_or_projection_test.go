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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

type andOrTestCase struct {
	tuples                []tuple
	expected              []tuple
	skipAllNullsInjection bool
}

var (
	andTestCases []andOrTestCase
	orTestCases  []andOrTestCase
)

func init() {
	andTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: tuples{
				{false}, {false}, {false},
				{true}, {false}, {nil},
				{nil}, {false}, {nil},
			},
		},
	}

	orTestCases = []andOrTestCase{
		// All variations of pairs separately first.
		{
			tuples:   tuples{{false, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{false, nil}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{false, false}},
			expected: tuples{{false}},
		},
		{
			tuples:   tuples{{true, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, false}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{true, nil}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{nil, true}},
			expected: tuples{{true}},
		},
		{
			tuples:   tuples{{nil, false}},
			expected: tuples{{nil}},
			// The case of {nil, nil} is explicitly tested below.
			skipAllNullsInjection: true,
		},
		{
			tuples:   tuples{{nil, nil}},
			expected: tuples{{nil}},
		},
		// Now all variations of pairs combined together to make sure that nothing
		// funky going on with multiple tuples.
		{
			tuples: tuples{
				{false, true}, {false, nil}, {false, false},
				{true, true}, {true, false}, {true, nil},
				{nil, true}, {nil, false}, {nil, nil},
			},
			expected: tuples{
				{true}, {nil}, {false},
				{true}, {true}, {true},
				{true}, {nil}, {nil},
			},
		},
	}
}

func TestAndOrOps(t *testing.T) {
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

	for _, test := range []struct {
		operation string
		cases     []andOrTestCase
	}{
		{
			operation: "AND",
			cases:     andTestCases,
		},
		{
			operation: "OR",
			cases:     orTestCases,
		},
	} {
		t.Run(test.operation, func(t *testing.T) {
			for _, tc := range test.cases {
				var runner testRunner
				if tc.skipAllNullsInjection {
					// We're omitting all nulls injection test. See comments for each such
					// test case.
					runner = runTestsWithoutAllNullsInjection
				} else {
					runner = runTestsWithTyps
				}
				runner(
					t,
					[]tuples{tc.tuples},
					[][]coltypes.T{{coltypes.Bool, coltypes.Bool}},
					tc.expected,
					orderedVerifier,
					func(input []Operator) (Operator, error) {
						projOp, err := createTestProjectingOperator(
							ctx, flowCtx, input[0], []types.T{*types.Bool, *types.Bool},
							fmt.Sprintf("@1 %s @2", test.operation), false, /* canFallbackToRowexec */
						)
						if err != nil {
							return nil, err
						}
						// We will project out the first two columns in order
						// to have test cases less verbose.
						return NewSimpleProjectOp(projOp, 3 /* numInputCols */, []uint32{2}), nil
					})
			}
		})
	}
}

func benchmarkLogicalProjOp(
	b *testing.B, operation string, useSelectionVector bool, hasNulls bool,
) {
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
	rng, _ := randutil.NewPseudoRand()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Bool, coltypes.Bool})
	col1 := batch.ColVec(0).Bool()
	col2 := batch.ColVec(0).Bool()
	for i := 0; i < coldata.BatchSize(); i++ {
		col1[i] = rng.Float64() < 0.5
		col2[i] = rng.Float64() < 0.5
	}
	if hasNulls {
		nulls1 := batch.ColVec(0).Nulls()
		nulls2 := batch.ColVec(0).Nulls()
		for i := 0; i < coldata.BatchSize(); i++ {
			if rng.Float64() < nullProbability {
				nulls1.SetNull(i)
			}
			if rng.Float64() < nullProbability {
				nulls2.SetNull(i)
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
	input := NewRepeatableBatchSource(testAllocator, batch)
	logicalProjOp, err := createTestProjectingOperator(
		ctx, flowCtx, input, []types.T{*types.Bool, *types.Bool},
		fmt.Sprintf("@1 %s @2", operation), false, /* canFallbackToRowexec */
	)
	require.NoError(b, err)
	logicalProjOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		logicalProjOp.Next(ctx)
	}
}

func BenchmarkLogicalProjOp(b *testing.B) {
	for _, operation := range []string{"AND", "OR"} {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(fmt.Sprintf("%s,useSel=%t,hasNulls=%t", operation, useSel, hasNulls), func(b *testing.B) {
					benchmarkLogicalProjOp(b, operation, useSel, hasNulls)
				})
			}
		}
	}
}
