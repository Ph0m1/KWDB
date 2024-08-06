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

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

// countOp is an operator that counts the number of input rows it receives,
// consuming its entire input and outputting a batch with a single integer
// column containing a single integer, the count of rows received from the
// upstream.
type countOp struct {
	OneInputNode

	internalBatch coldata.Batch
	done          bool
	count         int64
}

var _ Operator = &countOp{}

// NewCountOp returns a new count operator that counts the rows in its input.
func NewCountOp(allocator *Allocator, input Operator) Operator {
	c := &countOp{
		OneInputNode: NewOneInputNode(input),
	}
	c.internalBatch = allocator.NewMemBatchWithSize([]coltypes.T{coltypes.Int64}, 1)
	return c
}

func (c *countOp) Init() {
	c.input.Init()
	c.count = 0
	c.done = false
}

func (c *countOp) Next(ctx context.Context) coldata.Batch {
	c.internalBatch.ResetInternalBatch()
	if c.done {
		return coldata.ZeroBatch
	}
	for {
		bat := c.input.Next(ctx)
		length := bat.Length()
		if length == 0 {
			c.done = true
			c.internalBatch.ColVec(0).Int64()[0] = c.count
			c.internalBatch.SetLength(1)
			return c.internalBatch
		}
		c.count += int64(length)
	}
}
