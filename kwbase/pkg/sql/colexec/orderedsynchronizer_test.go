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
	"math/rand"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// Adapted from the same-named test in the rowflow package.
func TestOrderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		sources  []tuples
		ordering sqlbase.ColumnOrdering
		expected tuples
	}{
		{
			sources: []tuples{
				{
					{0, 1, 4},
					{0, 1, 2},
					{0, 2, 3},
					{1, 1, 3},
				},
				{
					{1, 0, 4},
				},
				{
					{0, 0, 0},
					{4, 4, 4},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 1, Direction: encoding.Ascending},
			},
			expected: tuples{
				{0, 0, 0},
				{0, 1, 4},
				{0, 1, 2},
				{0, 2, 3},
				{1, 0, 4},
				{1, 1, 3},
				{4, 4, 4},
			},
		},
		{
			sources: []tuples{
				{
					{1, 0, 4},
				},
				{
					{3, 4, 1},
					{4, 4, 4},
					{3, 2, 0},
				},
				{
					{4, 4, 5},
					{3, 3, 0},
					{0, 0, 0},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 1, Direction: encoding.Descending},
				{ColIdx: 0, Direction: encoding.Ascending},
				{ColIdx: 2, Direction: encoding.Ascending},
			},
			expected: tuples{
				{3, 4, 1},
				{4, 4, 4},
				{4, 4, 5},
				{3, 3, 0},
				{3, 2, 0},
				{0, 0, 0},
				{1, 0, 4},
			},
		},
		{
			sources: []tuples{
				{
					{-1},
				},
				{
					{1},
				},
				{
					{nil},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			},
			expected: tuples{
				{nil},
				{-1},
				{1},
			},
		},
		{
			sources: []tuples{
				{
					{-1},
				},
				{
					{1},
				},
				{
					{nil},
				},
			},
			ordering: sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Descending},
			},
			expected: tuples{
				{1},
				{-1},
				{nil},
			},
		},
	}
	for _, tc := range testCases {
		numCols := len(tc.sources[0][0])
		columnTypes := make([]coltypes.T, numCols)
		for i := range columnTypes {
			columnTypes[i] = coltypes.Int64
		}
		runTests(t, tc.sources, tc.expected, orderedVerifier, func(inputs []Operator) (Operator, error) {
			return &OrderedSynchronizer{
				allocator:   testAllocator,
				inputs:      inputs,
				ordering:    tc.ordering,
				columnTypes: columnTypes,
			}, nil
		})
	}
}

func TestOrderedSyncRandomInput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	numInputs := 3
	inputLen := 1024
	batchSize := int(16)
	if batchSize > coldata.BatchSize() {
		batchSize = coldata.BatchSize()
	}

	// Generate a random slice of sorted ints.
	randInts := make([]int, inputLen)
	for i := range randInts {
		randInts[i] = rand.Int()
	}
	sort.Ints(randInts)

	// Randomly distribute them among the inputs.
	expected := make(tuples, inputLen)
	sources := make([]tuples, numInputs)
	for i := range expected {
		t := tuple{randInts[i]}
		expected[i] = t
		sourceIdx := rand.Int() % 3
		if i < numInputs {
			// Make sure each input has at least one row.
			sourceIdx = i
		}
		sources[sourceIdx] = append(sources[sourceIdx], t)
	}
	inputs := make([]Operator, numInputs)
	for i := range inputs {
		inputs[i] = newOpTestInput(batchSize, sources[i], nil /* typs */)
	}

	op := OrderedSynchronizer{
		allocator: testAllocator,
		inputs:    inputs,
		ordering: sqlbase.ColumnOrdering{
			{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		columnTypes: []coltypes.T{coltypes.Int64},
	}
	op.Init()
	out := newOpTestOutput(&op, expected)
	if err := out.Verify(); err != nil {
		t.Error(err)
	}
}

func BenchmarkOrderedSynchronizer(b *testing.B) {
	ctx := context.Background()

	numInputs := int64(3)
	batches := make([]coldata.Batch, numInputs)
	for i := range batches {
		batches[i] = testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
		batches[i].SetLength(coldata.BatchSize())
	}
	for i := int64(0); i < int64(coldata.BatchSize())*numInputs; i++ {
		batch := batches[i%numInputs]
		batch.ColVec(0).Int64()[i/numInputs] = i
	}

	inputs := make([]Operator, len(batches))
	for i := range batches {
		inputs[i] = NewRepeatableBatchSource(testAllocator, batches[i])
	}

	op := OrderedSynchronizer{
		allocator: testAllocator,
		inputs:    inputs,
		ordering: sqlbase.ColumnOrdering{
			{ColIdx: 0, Direction: encoding.Ascending},
		},
		columnTypes: []coltypes.T{coltypes.Int64},
	}
	op.Init()

	b.SetBytes(8 * int64(coldata.BatchSize()) * numInputs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}
