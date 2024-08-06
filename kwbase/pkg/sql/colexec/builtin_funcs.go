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

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type defaultBuiltinFuncOperator struct {
	OneInputNode
	allocator      *Allocator
	evalCtx        *tree.EvalContext
	funcExpr       *tree.FuncExpr
	columnTypes    []types.T
	argumentCols   []int
	outputIdx      int
	outputType     *types.T
	outputPhysType coltypes.T
	converter      func(tree.Datum) (interface{}, error)

	row tree.Datums
	da  sqlbase.DatumAlloc
}

var _ Operator = &defaultBuiltinFuncOperator{}

func (b *defaultBuiltinFuncOperator) Init() {
	b.input.Init()
}

func (b *defaultBuiltinFuncOperator) Next(ctx context.Context) coldata.Batch {
	batch := b.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}

	sel := batch.Selection()
	output := batch.ColVec(b.outputIdx)
	b.allocator.PerformOperation(
		[]coldata.Vec{output},
		func() {
			for i := 0; i < n; i++ {
				rowIdx := i
				if sel != nil {
					rowIdx = sel[i]
				}

				hasNulls := false

				for j := range b.argumentCols {
					col := batch.ColVec(b.argumentCols[j])
					b.row[j] = PhysicalTypeColElemToDatum(col, rowIdx, &b.da, &b.columnTypes[b.argumentCols[j]])
					hasNulls = hasNulls || b.row[j] == tree.DNull
				}

				var (
					res tree.Datum
					err error
				)
				// Some functions cannot handle null arguments.
				if hasNulls && !b.funcExpr.CanHandleNulls() {
					res = tree.DNull
				} else {
					res, err = b.funcExpr.ResolvedOverload().Fn(b.evalCtx, b.row)
					if err != nil {
						execerror.NonVectorizedPanic(err)
					}
				}

				// Convert the datum into a physical type and write it out.
				if res == tree.DNull {
					batch.ColVec(b.outputIdx).Nulls().SetNull(rowIdx)
				} else {
					converted, err := b.converter(res)
					if err != nil {
						execerror.VectorizedInternalPanic(err)
					}
					coldata.SetValueAt(output, converted, rowIdx, b.outputPhysType)
				}
			}
		},
	)
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

// NewBuiltinFunctionOperator returns an operator that applies builtin functions.
func NewBuiltinFunctionOperator(
	allocator *Allocator,
	evalCtx *tree.EvalContext,
	funcExpr *tree.FuncExpr,
	columnTypes []types.T,
	argumentCols []int,
	outputIdx int,
	input Operator,
) (Operator, error) {
	switch funcExpr.ResolvedOverload().SpecializedVecBuiltin {
	case tree.SubstringStringIntInt:
		input = newVectorTypeEnforcer(allocator, input, coltypes.Bytes, outputIdx)
		return newSubstringOperator(
			allocator, columnTypes, argumentCols, outputIdx, input,
		), nil
	default:
		outputType := funcExpr.ResolvedType()
		outputPhysType := typeconv.FromColumnType(outputType)
		if outputPhysType == coltypes.Unhandled {
			return nil, errors.Errorf(
				"unsupported output type %q of %s",
				outputType.String(), funcExpr.String(),
			)
		}
		input = newVectorTypeEnforcer(allocator, input, outputPhysType, outputIdx)
		return &defaultBuiltinFuncOperator{
			OneInputNode:   NewOneInputNode(input),
			allocator:      allocator,
			evalCtx:        evalCtx,
			funcExpr:       funcExpr,
			outputIdx:      outputIdx,
			columnTypes:    columnTypes,
			outputType:     outputType,
			outputPhysType: outputPhysType,
			converter:      typeconv.GetDatumToPhysicalFn(outputType),
			row:            make(tree.Datums, len(argumentCols)),
			argumentCols:   argumentCols,
		}, nil
	}
}
