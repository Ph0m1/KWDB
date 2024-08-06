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
	"math/rand"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
)

const (
	selectivity     = .5
	nullProbability = .1
)

func TestSelLTInt64Int64ConstOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tups := tuples{{0}, {1}, {2}, {nil}}
	runTests(t, []tuples{tups}, tuples{{0}, {1}}, orderedVerifier, func(input []Operator) (Operator, error) {
		return &selLTInt64Int64ConstOp{
			selConstOpBase: selConstOpBase{
				OneInputNode: NewOneInputNode(input[0]),
				colIdx:       0,
			},
			constArg: 2,
		}, nil
	})
}

func TestSelLTInt64Int64(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tups := tuples{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{nil, 1},
		{-1, nil},
		{nil, nil},
	}
	runTests(t, []tuples{tups}, tuples{{0, 1}}, orderedVerifier, func(input []Operator) (Operator, error) {
		return &selLTInt64Int64Op{
			selOpBase: selOpBase{
				OneInputNode: NewOneInputNode(input[0]),
				col1Idx:      0,
				col2Idx:      1,
			},
		}, nil
	})
}

func TestGetSelectionConstOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cmpOp := tree.LT
	var input Operator
	colIdx := 3
	constVal := int64(31)
	constArg := tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(constVal))
	op, err := GetSelectionConstOperator(types.Date, types.Date, cmpOp, input, colIdx, constArg)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int64ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionConstMixedTypeOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cmpOp := tree.LT
	var input Operator
	colIdx := 3
	constVal := int16(31)
	constArg := tree.NewDInt(tree.DInt(constVal))
	op, err := GetSelectionConstOperator(types.Int, types.Int2, cmpOp, input, colIdx, constArg)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int16ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ct := types.Int2
	cmpOp := tree.GE
	var input Operator
	col1Idx := 5
	col2Idx := 7
	op, err := GetSelectionOperator(ct, ct, cmpOp, input, col1Idx, col2Idx)
	if err != nil {
		t.Error(err)
	}
	expected := &selGEInt16Int16Op{
		selOpBase: selOpBase{
			OneInputNode: NewOneInputNode(input),
			col1Idx:      col1Idx,
			col2Idx:      col2Idx,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func benchmarkSelLTInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	col := batch.ColVec(0).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col[i] = -1
		} else {
			col[i] = 1
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

	plusOp := &selLTInt64Int64ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: NewOneInputNode(source),
			colIdx:       0,
		},
		constArg: 0,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func benchmarkSelLTInt64Int64Op(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i], col2[i] = -1, 1
		} else {
			col1[i], col2[i] = 1, -1
		}
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(i)
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

	plusOp := &selLTInt64Int64Op{
		selOpBase: selOpBase{
			OneInputNode: NewOneInputNode(source),
			col1Idx:      0,
			col2Idx:      1,
		},
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize() * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64Op(b, useSel, hasNulls)
			})
		}
	}
}
