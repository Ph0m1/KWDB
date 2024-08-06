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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOrdinality(t *testing.T) {
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
	tcs := []struct {
		tuples     []tuple
		expected   []tuple
		inputTypes []types.T
	}{
		{
			tuples:     tuples{{1}},
			expected:   tuples{{1, 1}},
			inputTypes: []types.T{*types.Int},
		},
		{
			tuples:     tuples{{}, {}, {}, {}, {}},
			expected:   tuples{{1}, {2}, {3}, {4}, {5}},
			inputTypes: []types.T{},
		},
		{
			tuples:     tuples{{5}, {6}, {7}, {8}},
			expected:   tuples{{5, 1}, {6, 2}, {7, 3}, {8, 4}},
			inputTypes: []types.T{*types.Int},
		},
		{
			tuples:     tuples{{5, 'a'}, {6, 'b'}, {7, 'c'}, {8, 'd'}},
			expected:   tuples{{5, 'a', 1}, {6, 'b', 2}, {7, 'c', 3}, {8, 'd', 4}},
			inputTypes: []types.T{*types.Int, *types.String},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				return createTestOrdinalityOperator(ctx, flowCtx, input[0], tc.inputTypes)
			})
	}
}

func BenchmarkOrdinality(b *testing.B) {
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

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(testAllocator, batch)
	ordinality, err := createTestOrdinalityOperator(ctx, flowCtx, source, []types.T{*types.Int, *types.Int, *types.Int})
	require.NoError(b, err)
	ordinality.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	for i := 0; i < b.N; i++ {
		ordinality.Next(ctx)
	}
}

func createTestOrdinalityOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, input Operator, inputTypes []types.T,
) (Operator, error) {
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{ColumnTypes: inputTypes}},
		Core: execinfrapb.ProcessorCoreUnion{
			Ordinality: &execinfrapb.OrdinalitySpec{},
		},
	}
	args := NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []Operator{input},
		StreamingMemAccount: testMemAcc,
	}
	args.TestingKnobs.UseStreamingMemAccountForBuffering = true
	result, err := NewColOperator(ctx, flowCtx, args)
	if err != nil {
		return nil, err
	}
	return result.Op, nil
}
