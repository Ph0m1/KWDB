// Copyright 2018 The Cockroach Authors.
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
	"math"
	"math/rand"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

var sortAllTestCases []sortTestCase

func init() {
	sortAllTestCases = []sortTestCase{
		{
			tuples:   tuples{{1}, {2}, {nil}, {4}, {5}, {nil}},
			expected: tuples{{nil}, {nil}, {1}, {2}, {4}, {5}},
			logTypes: []types.T{*types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1, 2}, {1, 1}, {1, nil}, {2, nil}, {2, 3}, {2, nil}, {5, 1}},
			expected: tuples{{1, nil}, {1, 1}, {1, 2}, {2, nil}, {2, nil}, {2, 3}, {5, 1}},
			logTypes: []types.T{*types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
		},
		{
			tuples:   tuples{{1, 2}, {1, 1}, {1, nil}, {2, nil}, {2, 3}, {2, nil}, {5, 1}},
			expected: tuples{{5, 1}, {2, 3}, {2, nil}, {2, nil}, {1, 2}, {1, 1}, {1, nil}},
			logTypes: []types.T{*types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0, Direction: execinfrapb.Ordering_Column_DESC}, {ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC}},
		},
		{
			tuples:   tuples{{nil, nil}, {nil, 3}, {1, nil}, {nil, 1}, {1, 2}, {nil, nil}, {5, nil}},
			expected: tuples{{nil, nil}, {nil, nil}, {nil, 1}, {nil, 3}, {1, nil}, {1, 2}, {5, nil}},
			logTypes: []types.T{*types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}},
		},
		{
			tuples:   tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			logTypes: []types.T{*types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			expected: tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			logTypes: []types.T{*types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1, 1}, {3, 2}, {2, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			expected: tuples{{1, 1}, {2, 3}, {3, 2}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			logTypes: []types.T{*types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1, 1}, {5, 2}, {3, 3}, {7, 4}, {2, 5}, {6, 6}, {4, 7}},
			expected: tuples{{1, 1}, {2, 5}, {3, 3}, {4, 7}, {5, 2}, {6, 6}, {7, 4}},
			logTypes: []types.T{*types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			expected: tuples{{1}, {2}, {3}, {3}, {4}, {5}, {6}},
			logTypes: []types.T{*types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{false}, {true}},
			expected: tuples{{false}, {true}},
			logTypes: []types.T{*types.Bool},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{true}, {false}},
			expected: tuples{{false}, {true}},
			logTypes: []types.T{*types.Bool},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},
		{
			tuples:   tuples{{3.2}, {2.0}, {2.4}, {math.NaN()}, {math.Inf(-1)}, {math.Inf(1)}},
			expected: tuples{{math.NaN()}, {math.Inf(-1)}, {2.0}, {2.4}, {3.2}, {math.Inf(1)}},
			logTypes: []types.T{*types.Float},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}},
		},

		{
			tuples:   tuples{{0, 1, 0}, {1, 2, 0}, {2, 3, 2}, {3, 7, 1}, {4, 2, 2}},
			expected: tuples{{0, 1, 0}, {1, 2, 0}, {3, 7, 1}, {4, 2, 2}, {2, 3, 2}},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 2}, {ColIdx: 1}},
		},

		{
			// ensure that sort partitions stack: make sure that a run of identical
			// values in a later column doesn't get sorted if the run is broken up
			// by previous columns.
			tuples: tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			expected: tuples{
				{0, 0, 0},
				{0, 0, 1},
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
			},
			logTypes: []types.T{*types.Int, *types.Int, *types.Int},
			ordCols:  []execinfrapb.Ordering_Column{{ColIdx: 0}, {ColIdx: 1}, {ColIdx: 2}},
		},
	}
}

func TestSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range sortAllTestCases {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
			physTypes, err := typeconv.FromColumnTypes(tc.logTypes)
			if err != nil {
				return nil, err
			}
			return NewSorter(testAllocator, input[0], physTypes, tc.ordCols)
		})
	}
}

func TestSortRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	nTups := coldata.BatchSize()*2 + 1
	maxCols := 3
	// TODO(yuzefovich): randomize types as well.
	typs := make([]coltypes.T, maxCols)
	for i := range typs {
		typs[i] = coltypes.Int64
	}
	for nCols := 1; nCols < maxCols; nCols++ {
		for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
			for _, k := range []uint16{0, uint16(rng.Intn(nTups)) + 1} {
				topK := k != 0
				name := fmt.Sprintf("nCols=%d/nOrderingCols=%d/topK=%t", nCols, nOrderingCols, topK)
				t.Run(name, func(t *testing.T) {
					tups, expected, ordCols := generateRandomDataForTestSort(rng, nTups, nCols, nOrderingCols)
					if topK {
						expected = expected[:k]
					}
					runTests(t, []tuples{tups}, expected, orderedVerifier, func(input []Operator) (Operator, error) {
						if topK {
							return NewTopKSorter(testAllocator, input[0], typs[:nCols], ordCols, k), nil
						}
						return NewSorter(testAllocator, input[0], typs[:nCols], ordCols)
					})
				})
			}
		}
	}
}

// generateRandomDataForTestSort is a utility function that generates data to
// be used in randomized unit test of a sort operation. It returns:
// - tups - the data to be sorted
// - expected - the same data but already sorted
// - ordCols - ordering columns used in the sort operation.
func generateRandomDataForTestSort(
	rng *rand.Rand, nTups, nCols, nOrderingCols int,
) (tups, expected tuples, ordCols []execinfrapb.Ordering_Column) {
	ordCols = generateColumnOrdering(rng, nCols, nOrderingCols)
	tups = make(tuples, nTups)
	for i := range tups {
		tups[i] = make(tuple, nCols)
		for j := range tups[i] {
			// Small range so we can test partitioning
			if rng.Float64() < nullProbability {
				tups[i][j] = nil
			} else {
				tups[i][j] = rng.Int63() % 2048
			}
		}
		// Enforce that the last ordering column is always unique. Otherwise
		// there would be multiple valid sort orders.
		tups[i][ordCols[nOrderingCols-1].ColIdx] = int64(i)
	}

	expected = make(tuples, nTups)
	copy(expected, tups)
	sort.Slice(expected, less(expected, ordCols))
	return tups, expected, ordCols
}

func TestAllSpooler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tcs := []struct {
		tuples tuples
		typ    []coltypes.T
	}{
		{
			tuples: tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			typ:    []coltypes.T{coltypes.Int64},
		},
		{
			tuples: tuples{{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}},
			typ:    []coltypes.T{coltypes.Int64},
		},
		{
			tuples: tuples{{1, 1}, {3, 2}, {2, 3}, {4, 4}, {5, 5}, {6, 6}, {7, 7}},
			typ:    []coltypes.T{coltypes.Int64, coltypes.Int64},
		},
		{
			tuples: tuples{{1, 1}, {5, 2}, {3, 3}, {7, 4}, {2, 5}, {6, 6}, {4, 7}},
			typ:    []coltypes.T{coltypes.Int64, coltypes.Int64},
		},
		{
			tuples: tuples{{1}, {5}, {3}, {3}, {2}, {6}, {4}},
			typ:    []coltypes.T{coltypes.Int64},
		},
		{
			tuples: tuples{{0, 1, 0}, {1, 2, 0}, {2, 3, 2}, {3, 7, 1}, {4, 2, 2}},
			typ:    []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
		},
		{
			tuples: tuples{
				{0, 1, 0},
				{0, 1, 0},
				{0, 1, 1},
				{0, 0, 1},
				{0, 0, 0},
			},
			typ: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
		},
	}
	for _, tc := range tcs {
		runTestsWithFn(t, []tuples{tc.tuples}, nil /* typs */, func(t *testing.T, input []Operator) {
			allSpooler := newAllSpooler(testAllocator, input[0], tc.typ)
			allSpooler.init()
			allSpooler.spool(context.Background())
			if len(tc.tuples) != allSpooler.getNumTuples() {
				t.Fatal(fmt.Sprintf("allSpooler spooled wrong number of tuples: expected %d, but received %d", len(tc.tuples), allSpooler.getNumTuples()))
			}
			if allSpooler.getPartitionsCol() != nil {
				t.Fatal("allSpooler returned non-nil partitionsCol")
			}
			for col := 0; col < len(tc.typ); col++ {
				colVec := allSpooler.getValues(col).Int64()
				for i := 0; i < allSpooler.getNumTuples(); i++ {
					if colVec[i] != int64(tc.tuples[i][col].(int)) {
						t.Fatal(fmt.Sprintf("allSpooler returned wrong value in %d column of %d'th tuple : expected %v, but received %v",
							col, i, tc.tuples[i][col].(int), colVec[i]))
					}
				}
			}
		})
	}
}

func BenchmarkSort(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()
	k := uint16(128)

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			for _, topK := range []bool{false, true} {
				name := fmt.Sprintf("rows=%d/cols=%d/topK=%t", nBatches*coldata.BatchSize(), nCols, topK)
				b.Run(name, func(b *testing.B) {
					// 8 (bytes / int64) * nBatches (number of batches) * coldata.BatchSize() (rows /
					// batch) * nCols (number of columns / row).
					b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
					typs := make([]coltypes.T, nCols)
					for i := range typs {
						typs[i] = coltypes.Int64
					}
					batch := testAllocator.NewMemBatch(typs)
					batch.SetLength(coldata.BatchSize())
					ordCols := make([]execinfrapb.Ordering_Column, nCols)
					for i := range ordCols {
						ordCols[i].ColIdx = uint32(i)
						ordCols[i].Direction = execinfrapb.Ordering_Column_Direction(rng.Int() % 2)

						col := batch.ColVec(i).Int64()
						for j := 0; j < coldata.BatchSize(); j++ {
							col[j] = rng.Int63() % int64((i*1024)+1)
						}
					}
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						source := newFiniteBatchSource(batch, nBatches)
						var sorter Operator
						if topK {
							sorter = NewTopKSorter(testAllocator, source, typs, ordCols, k)
						} else {
							var err error
							sorter, err = NewSorter(testAllocator, source, typs, ordCols)
							if err != nil {
								b.Fatal(err)
							}
						}
						sorter.Init()
						for out := sorter.Next(ctx); out.Length() != 0; out = sorter.Next(ctx) {
						}
					}
				})
			}
		}
	}
}

func BenchmarkAllSpooler(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	for _, nBatches := range []int{1 << 1, 1 << 4, 1 << 8} {
		for _, nCols := range []int{1, 2, 4} {
			b.Run(fmt.Sprintf("rows=%d/cols=%d", nBatches*coldata.BatchSize(), nCols), func(b *testing.B) {
				// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
				// batch) * nCols (number of columns / row).
				b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
				typs := make([]coltypes.T, nCols)
				for i := range typs {
					typs[i] = coltypes.Int64
				}
				batch := testAllocator.NewMemBatch(typs)
				batch.SetLength(coldata.BatchSize())
				for i := 0; i < nCols; i++ {
					col := batch.ColVec(i).Int64()
					for j := 0; j < coldata.BatchSize(); j++ {
						col[j] = rng.Int63() % int64((i*1024)+1)
					}
				}
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					source := newFiniteBatchSource(batch, nBatches)
					allSpooler := newAllSpooler(testAllocator, source, typs)
					allSpooler.init()
					allSpooler.spool(ctx)
				}
			})
		}
	}
}

func less(tuples tuples, ordCols []execinfrapb.Ordering_Column) func(i, j int) bool {
	return func(i, j int) bool {
		for _, col := range ordCols {
			n1 := tuples[i][col.ColIdx] == nil
			n2 := tuples[j][col.ColIdx] == nil
			if col.Direction == execinfrapb.Ordering_Column_ASC {
				if n1 && n2 {
					continue
				} else if n1 {
					return true
				} else if n2 {
					return false
				}
			} else {
				if n1 && n2 {
					continue
				} else if n1 {
					return false
				} else if n2 {
					return true
				}
			}
			if tuples[i][col.ColIdx].(int64) < tuples[j][col.ColIdx].(int64) {
				return col.Direction == execinfrapb.Ordering_Column_ASC
			} else if tuples[i][col.ColIdx].(int64) > tuples[j][col.ColIdx].(int64) {
				return col.Direction == execinfrapb.Ordering_Column_DESC
			}
		}
		return false
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols.
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []execinfrapb.Ordering_Column {
	if nOrderingCols > nCols {
		execerror.VectorizedInternalPanic("nOrderingCols > nCols in generateColumnOrdering")
	}
	orderingCols := make([]execinfrapb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = execinfrapb.Ordering_Column{ColIdx: uint32(col), Direction: execinfrapb.Ordering_Column_Direction(rng.Intn(2))}
	}
	return orderingCols
}
