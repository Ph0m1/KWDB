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
)

// offsetOp is an operator that implements offset, returning everything
// after the first n tuples in its input.
type offsetOp struct {
	OneInputNode

	offset int

	// seen is the number of tuples seen so far.
	seen int
}

var _ Operator = &offsetOp{}

// NewOffsetOp returns a new offset operator with the given offset.
func NewOffsetOp(input Operator, offset int) Operator {
	c := &offsetOp{
		OneInputNode: NewOneInputNode(input),
		offset:       offset,
	}
	return c
}

func (c *offsetOp) Init() {
	c.input.Init()
}

func (c *offsetOp) Next(ctx context.Context) coldata.Batch {
	for {
		bat := c.input.Next(ctx)
		length := bat.Length()
		if length == 0 {
			return bat
		}

		c.seen += length

		delta := c.seen - c.offset
		// If the current batch encompasses the offset "boundary",
		// add the elements after the boundary to the selection vector.
		if delta > 0 && delta < length {
			sel := bat.Selection()
			outputStartIdx := length - delta
			if sel != nil {
				copy(sel, sel[outputStartIdx:length])
			} else {
				bat.SetSelection(true)
				sel = bat.Selection()[:delta] // slice for bounds check elimination
				for i := range sel {
					sel[i] = outputStartIdx + i
				}
			}
			bat.SetLength(delta)
		}

		if c.seen > c.offset {
			return bat
		}
	}
}

// Reset resets the offsetOp for another run. Primarily used for
// benchmarks.
func (c *offsetOp) Reset() {
	c.seen = 0
}
