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

// deselectorOp consumes the input operator, and if resulting batches have a
// selection vector, it coalesces them (meaning that tuples will be reordered
// or omitted according to the selection vector). If the batches come with no
// selection vector, it is a noop.
type deselectorOp struct {
	OneInputNode
	NonExplainable
	allocator  *Allocator
	inputTypes []coltypes.T

	output coldata.Batch
}

var _ Operator = &deselectorOp{}

// NewDeselectorOp creates a new deselector operator on the given input
// operator with the given column coltypes.
func NewDeselectorOp(allocator *Allocator, input Operator, colTypes []coltypes.T) Operator {
	return &deselectorOp{
		OneInputNode: NewOneInputNode(input),
		allocator:    allocator,
		inputTypes:   colTypes,
	}
}

func (p *deselectorOp) Init() {
	p.input.Init()
}

func (p *deselectorOp) Next(ctx context.Context) coldata.Batch {
	p.resetOutput()
	batch := p.input.Next(ctx)
	if batch.Selection() == nil {
		return batch
	}

	sel := batch.Selection()
	p.allocator.PerformOperation(p.output.ColVecs(), func() {
		for i, t := range p.inputTypes {
			toCol := p.output.ColVec(i)
			fromCol := batch.ColVec(i)
			toCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						ColType:   t,
						Src:       fromCol,
						Sel:       sel,
						SrcEndIdx: batch.Length(),
					},
				},
			)
		}
	})
	p.output.SetLength(batch.Length())
	return p.output
}

func (p *deselectorOp) resetOutput() {
	if p.output == nil {
		p.output = p.allocator.NewMemBatch(p.inputTypes)
	} else {
		p.output.ResetInternalBatch()
	}
}
