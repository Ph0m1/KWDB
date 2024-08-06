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
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
)

// NewWindowSortingPartitioner creates a new colexec.Operator that orders input
// first based on the partitionIdxs columns and second on ordCols (i.e. it
// handles both PARTITION BY and ORDER BY clauses of a window function) and
// puts true in partitionColIdx'th column (which is appended if needed) for
// every tuple that is the first within its partition.
func NewWindowSortingPartitioner(
	allocator *Allocator,
	input Operator,
	inputTyps []coltypes.T,
	partitionIdxs []uint32,
	ordCols []execinfrapb.Ordering_Column,
	partitionColIdx int,
	createDiskBackedSorter func(input Operator, inputTypes []coltypes.T, orderingCols []execinfrapb.Ordering_Column) (Operator, error),
) (op Operator, err error) {
	partitionAndOrderingCols := make([]execinfrapb.Ordering_Column, len(partitionIdxs)+len(ordCols))
	for i, idx := range partitionIdxs {
		partitionAndOrderingCols[i] = execinfrapb.Ordering_Column{ColIdx: idx}
	}
	copy(partitionAndOrderingCols[len(partitionIdxs):], ordCols)
	input, err = createDiskBackedSorter(input, inputTyps, partitionAndOrderingCols)
	if err != nil {
		return nil, err
	}

	var distinctCol []bool
	input, distinctCol, err = OrderedDistinctColsToOperators(input, partitionIdxs, inputTyps)
	if err != nil {
		return nil, err
	}

	input = newVectorTypeEnforcer(allocator, input, coltypes.Bool, partitionColIdx)
	return &windowSortingPartitioner{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		distinctCol:     distinctCol,
		partitionColIdx: partitionColIdx,
	}, nil
}

type windowSortingPartitioner struct {
	OneInputNode

	allocator *Allocator
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new partition begins with the
	// corresponding tuple.
	distinctCol     []bool
	partitionColIdx int
}

func (p *windowSortingPartitioner) Init() {
	p.input.Init()
}

func (p *windowSortingPartitioner) Next(ctx context.Context) coldata.Batch {
	b := p.input.Next(ctx)
	if b.Length() == 0 {
		return coldata.ZeroBatch
	}
	partitionVec := b.ColVec(p.partitionColIdx).Bool()
	sel := b.Selection()
	if sel != nil {
		for i := 0; i < b.Length(); i++ {
			partitionVec[sel[i]] = p.distinctCol[sel[i]]
		}
	} else {
		copy(partitionVec, p.distinctCol[:b.Length()])
	}
	return b
}
