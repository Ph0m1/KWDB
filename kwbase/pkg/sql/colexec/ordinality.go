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

// ordinalityOp is an operator that implements WITH ORDINALITY, which adds
// an additional column to the result with an ordinal number.
type ordinalityOp struct {
	OneInputNode

	allocator *Allocator
	// outputIdx is the index of the column in which ordinalityOp will write the
	// ordinal number.
	outputIdx int
	// counter is the number of tuples seen so far.
	counter int64
}

var _ Operator = &ordinalityOp{}

// NewOrdinalityOp returns a new WITH ORDINALITY operator.
func NewOrdinalityOp(allocator *Allocator, input Operator, outputIdx int) Operator {
	input = newVectorTypeEnforcer(allocator, input, coltypes.Int64, outputIdx)
	c := &ordinalityOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		outputIdx:    outputIdx,
		counter:      1,
	}
	return c
}

func (c *ordinalityOp) Init() {
	c.input.Init()
}

func (c *ordinalityOp) Next(ctx context.Context) coldata.Batch {
	bat := c.input.Next(ctx)
	if bat.Length() == 0 {
		return coldata.ZeroBatch
	}

	vec := bat.ColVec(c.outputIdx).Int64()
	sel := bat.Selection()

	if sel != nil {
		// Bounds check elimination.
		for _, i := range sel[:bat.Length()] {
			vec[i] = c.counter
			c.counter++
		}
	} else {
		// Bounds check elimination.
		vec = vec[:bat.Length()]
		for i := range vec {
			vec[i] = c.counter
			c.counter++
		}
	}

	return bat
}
