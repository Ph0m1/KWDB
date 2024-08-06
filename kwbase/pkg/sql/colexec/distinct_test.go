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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	tcs := []struct {
		distinctCols            []uint32
		colTypes                []coltypes.T
		tuples                  []tuple
		expected                []tuple
		isOrderedOnDistinctCols bool
	}{
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
			isOrderedOnDistinctCols: true,
		},
		{
			distinctCols: []uint32{1, 0, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{nil, nil, nil, nil},
				{nil, nil, "30", nil},
				{1.0, 2, "30", 4},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{2.0, 3, "40", 4},
			},
			isOrderedOnDistinctCols: true,
		},
		{
			distinctCols: []uint32{0, 1, 2},
			colTypes:     []coltypes.T{coltypes.Float64, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			tuples: tuples{
				{1.0, 2, "30", 4},
				{1.0, 2, "30", 4},
				{nil, nil, nil, nil},
				{nil, nil, nil, nil},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{nil, nil, "30", nil},
				{2.0, 3, "40", 4},
				{2.0, 3, "40", 4},
			},
			expected: tuples{
				{1.0, 2, "30", 4},
				{nil, nil, nil, nil},
				{2.0, 2, "30", 4},
				{2.0, 3, "30", 4},
				{nil, nil, "30", nil},
				{2.0, 3, "40", 4},
			},
		},
		{
			distinctCols: []uint32{0},
			colTypes:     []coltypes.T{coltypes.Int64, coltypes.Bytes},
			tuples: tuples{
				{1, "a"},
				{2, "b"},
				{3, "c"},
				{nil, "d"},
				{5, "e"},
				{6, "f"},
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
			expected: tuples{
				{1, "a"},
				{2, "b"},
				{3, "c"},
				{nil, "d"},
				{5, "e"},
				{6, "f"},
			},
		},
		{
			// This is to test hashTable deduplication with various batch size
			// boundaries and ensure it always emits the first tuple it encountered.
			distinctCols: []uint32{0},
			colTypes:     []coltypes.T{coltypes.Int64, coltypes.Bytes},
			tuples: tuples{
				{1, "1"},
				{1, "2"},
				{1, "3"},
				{1, "4"},
				{1, "5"},
				{2, "6"},
				{2, "7"},
				{2, "8"},
				{2, "9"},
				{2, "10"},
				{0, "11"},
				{0, "12"},
				{0, "13"},
				{1, "14"},
				{1, "15"},
				{1, "16"},
			},
			expected: tuples{
				{1, "1"},
				{2, "6"},
				{0, "11"},
			},
		},
	}

	for _, tc := range tcs {
		for _, numOfBuckets := range []uint64{1, 3, 5, hashTableNumBuckets} {
			t.Run(fmt.Sprintf("unordered/numOfBuckets=%d", numOfBuckets), func(t *testing.T) {
				runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier,
					func(input []Operator) (Operator, error) {
						return NewUnorderedDistinct(
							testAllocator, input[0], tc.distinctCols, tc.colTypes,
							numOfBuckets), nil
					})
			})
		}
		if tc.isOrderedOnDistinctCols {
			for numOrderedCols := 1; numOrderedCols < len(tc.distinctCols); numOrderedCols++ {
				t.Run(fmt.Sprintf("partiallyOrdered/ordCols=%d", numOrderedCols), func(t *testing.T) {
					orderedCols := make([]uint32, numOrderedCols)
					for i, j := range rng.Perm(len(tc.distinctCols))[:numOrderedCols] {
						orderedCols[i] = tc.distinctCols[j]
					}
					runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier,
						func(input []Operator) (Operator, error) {
							return newPartiallyOrderedDistinct(
								testAllocator, input[0], tc.distinctCols,
								orderedCols, tc.colTypes,
							)
						})
				})
			}
			t.Run("ordered", func(t *testing.T) {
				runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier,
					func(input []Operator) (Operator, error) {
						return NewOrderedDistinct(input[0], tc.distinctCols, tc.colTypes)
					})
			})
		}
	}
}

func BenchmarkDistinct(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	distinctConstructors := []func(*Allocator, Operator, []uint32, int, []coltypes.T) (Operator, error){
		func(allocator *Allocator, input Operator, distinctCols []uint32, numOrderedCols int, typs []coltypes.T) (Operator, error) {
			return NewUnorderedDistinct(allocator, input, distinctCols, typs, hashTableNumBuckets), nil
		},
		func(allocator *Allocator, input Operator, distinctCols []uint32, numOrderedCols int, typs []coltypes.T) (Operator, error) {
			return newPartiallyOrderedDistinct(allocator, input, distinctCols, distinctCols[:numOrderedCols], typs)
		},
		func(allocator *Allocator, input Operator, distinctCols []uint32, numOrderedCols int, typs []coltypes.T) (Operator, error) {
			return NewOrderedDistinct(input, distinctCols, typs)
		},
	}
	distinctNames := []string{"Unordered", "PartiallyOrdered", "Ordered"}
	orderedColsFraction := []float64{0, 0.5, 1.0}
	for _, hasNulls := range []bool{false, true} {
		for _, newTupleProbability := range []float64{0.001, 0.01, 0.1} {
			for _, nBatches := range []int{1 << 2, 1 << 6} {
				for _, nCols := range []int{2, 4} {
					typs := make([]coltypes.T, nCols)
					for i := range typs {
						typs[i] = coltypes.Int64
					}
					batch := testAllocator.NewMemBatch(typs)
					batch.SetLength(coldata.BatchSize())
					distinctCols := []uint32{0, 1, 2, 3}[:nCols]
					// We have the following equation:
					//   newTupleProbability = 1 - (1 - newValueProbability) ^ nCols,
					// so applying some manipulations we get:
					//   newValueProbability = 1 - (1 - newTupleProbability) ^ (1 / nCols).
					newValueProbability := 1.0 - math.Pow(1-newTupleProbability, 1.0/float64(nCols))
					for i := range distinctCols {
						col := batch.ColVec(i).Int64()
						col[0] = 0
						for j := 1; j < coldata.BatchSize(); j++ {
							col[j] = col[j-1]
							if rng.Float64() < newValueProbability {
								col[j]++
							}
						}
						nulls := batch.ColVec(i).Nulls()
						if hasNulls {
							nulls.SetNull(0)
						} else {
							nulls.UnsetNulls()
						}
					}
					for distinctIdx, distinctConstructor := range distinctConstructors {
						numOrderedCols := int(float64(nCols) * orderedColsFraction[distinctIdx])
						b.Run(
							fmt.Sprintf("%s/hasNulls=%v/newTupleProbability=%.3f/rows=%d/cols=%d/ordCols=%d",
								distinctNames[distinctIdx], hasNulls, newTupleProbability,
								nBatches*coldata.BatchSize(), nCols, numOrderedCols,
							),
							func(b *testing.B) {
								b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols))
								b.ResetTimer()
								for n := 0; n < b.N; n++ {
									// Note that the source will be ordered on all nCols so that the
									// number of distinct tuples doesn't vary between different
									// distinct operator variations.
									source := newFiniteChunksSource(batch, nBatches, nCols)
									distinct, err := distinctConstructor(testAllocator, source, distinctCols, numOrderedCols, typs)
									if err != nil {
										b.Fatal(err)
									}
									distinct.Init()
									for b := distinct.Next(ctx); b.Length() > 0; b = distinct.Next(ctx) {
									}
								}
								b.StopTimer()
							})
					}
				}
			}
		}
	}
}
