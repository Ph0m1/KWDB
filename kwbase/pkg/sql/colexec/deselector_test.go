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
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestDeselector(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		colTypes []coltypes.T
		tuples   []tuple
		sel      []int
		expected []tuple
	}{
		{
			colTypes: []coltypes.T{coltypes.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      nil,
			expected: tuples{{0}, {1}, {2}},
		},
		{
			colTypes: []coltypes.T{coltypes.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{},
			expected: tuples{},
		},
		{
			colTypes: []coltypes.T{coltypes.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{1},
			expected: tuples{{1}},
		},
		{
			colTypes: []coltypes.T{coltypes.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{0, 2},
			expected: tuples{{0}, {2}},
		},
		{
			colTypes: []coltypes.T{coltypes.Int64},
			tuples:   tuples{{0}, {1}, {2}},
			sel:      []int{0, 1, 2},
			expected: tuples{{0}, {1}, {2}},
		},
	}

	for _, tc := range tcs {
		runTestsWithFixedSel(t, []tuples{tc.tuples}, tc.sel, func(t *testing.T, input []Operator) {
			op := NewDeselectorOp(testAllocator, input[0], tc.colTypes)
			out := newOpTestOutput(op, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkDeselector(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	nCols := 1
	inputTypes := make([]coltypes.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		inputTypes[colIdx] = coltypes.Int64
	}

	batch := testAllocator.NewMemBatch(inputTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize(); i++ {
			col[i] = int64(i)
		}
	}
	for _, probOfOmitting := range []float64{0.1, 0.9} {
		sel := randomSel(rng, coldata.BatchSize(), probOfOmitting)
		batchLen := len(sel)

		for _, nBatches := range []int{1 << 1, 1 << 2, 1 << 4, 1 << 8} {
			b.Run(fmt.Sprintf("rows=%d/after selection=%d", nBatches*coldata.BatchSize(), nBatches*batchLen), func(b *testing.B) {
				// We're measuring the amount of data that is not selected out.
				b.SetBytes(int64(8 * nBatches * batchLen * nCols))
				batch.SetSelection(true)
				copy(batch.Selection(), sel)
				batch.SetLength(batchLen)
				input := NewRepeatableBatchSource(testAllocator, batch)
				op := NewDeselectorOp(testAllocator, input, inputTypes)
				op.Init()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					input.ResetBatchesToReturn(nBatches)
					for b := op.Next(ctx); b.Length() != 0; b = op.Next(ctx) {
					}
					// We don't need to reset the deselector because it doesn't keep any
					// state. We do, however, want to keep its already allocated memory
					// so that this memory allocation doesn't impact the benchmark.
				}
				b.StopTimer()
			})
		}
	}
}
