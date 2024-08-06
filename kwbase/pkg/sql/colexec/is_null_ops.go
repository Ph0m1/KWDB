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
)

// isNullProjOp is an Operator that projects into outputIdx Vec whether the
// corresponding value in colIdx Vec is NULL (i.e. it performs IS NULL check).
// If negate is true, it does the opposite - it performs IS NOT NULL check.
type isNullProjOp struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
	negate    bool
}

func newIsNullProjOp(
	allocator *Allocator, input Operator, colIdx, outputIdx int, negate bool,
) Operator {
	input = newVectorTypeEnforcer(allocator, input, coltypes.Bool, outputIdx)
	return &isNullProjOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		colIdx:       colIdx,
		outputIdx:    outputIdx,
		negate:       negate,
	}
}

var _ Operator = &isNullProjOp{}

func (o *isNullProjOp) Init() {
	o.input.Init()
}

func (o *isNullProjOp) Next(ctx context.Context) coldata.Batch {
	batch := o.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(o.colIdx)
	nulls := vec.Nulls()
	projCol := batch.ColVec(o.outputIdx).Bool()
	if nulls.MaybeHasNulls() {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				projCol[i] = nulls.NullAt(i) != o.negate
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				projCol[i] = nulls.NullAt(i) != o.negate
			}
		}
	} else {
		// There are no NULLs, so we don't need to check each index for nullity.
		result := o.negate
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				projCol[i] = result
			}
		} else {
			projCol = projCol[:n]
			for i := range projCol {
				projCol[i] = result
			}
		}
	}
	return batch
}

// isNullSelOp is an Operator that selects all the tuples that have a NULL
// value in colIdx Vec. If negate is true, then it does the opposite -
// selecting all the tuples that have a non-NULL value in colIdx Vec.
type isNullSelOp struct {
	OneInputNode
	colIdx int
	negate bool
}

func newIsNullSelOp(input Operator, colIdx int, negate bool) Operator {
	return &isNullSelOp{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
		negate:       negate,
	}
}

var _ Operator = &isNullSelOp{}

func (o *isNullSelOp) Init() {
	o.input.Init()
}

func (o *isNullSelOp) Next(ctx context.Context) coldata.Batch {
	for {
		batch := o.input.Next(ctx)
		n := batch.Length()
		if n == 0 {
			return batch
		}
		var idx int
		vec := batch.ColVec(o.colIdx)
		nulls := vec.Nulls()
		if nulls.MaybeHasNulls() {
			// There might be NULLs in the Vec, so we'll need to iterate over all
			// tuples.
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					if nulls.NullAt(i) != o.negate {
						sel[idx] = i
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()[:n]
				for i := range sel {
					if nulls.NullAt(i) != o.negate {
						sel[idx] = i
						idx++
					}
				}
			}
			if idx > 0 {
				batch.SetLength(idx)
				return batch
			}
		} else {
			// There are no NULLs, so we don't need to check each index for nullity.
			if o.negate {
				// o.negate is true, so we select all tuples, i.e. we don't need to
				// modify the batch and can just return it.
				return batch
			}
			// o.negate is false, so we omit all tuples from this batch and move onto
			// the next one.
		}
	}
}
