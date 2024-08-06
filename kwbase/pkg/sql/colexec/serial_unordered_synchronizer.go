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
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
)

// SerialUnorderedSynchronizer is an Operator that combines multiple Operator
// streams into one. It reads its inputs one by one until each one is exhausted,
// at which point it moves to the next input. See ParallelUnorderedSynchronizer
// for a parallel implementation. The serial one is used when concurrency is
// undesirable - for example when the whole query is planned on the gateway and
// we want to run it in the RootTxn.
type SerialUnorderedSynchronizer struct {
	inputs []Operator
	// curSerialInputIdx indicates the index of the current input being consumed.
	curSerialInputIdx int
}

var _ Operator = &SerialUnorderedSynchronizer{}
var _ execinfra.OpNode = &SerialUnorderedSynchronizer{}

// ChildCount implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) ChildCount(verbose bool) int {
	return len(s.inputs)
}

// Child implements the execinfra.OpNode interface.
func (s *SerialUnorderedSynchronizer) Child(nth int, verbose bool) execinfra.OpNode {
	return s.inputs[nth]
}

// NewSerialUnorderedSynchronizer creates a new SerialUnorderedSynchronizer.
func NewSerialUnorderedSynchronizer(
	inputs []Operator, typs []coltypes.T,
) *SerialUnorderedSynchronizer {
	return &SerialUnorderedSynchronizer{
		inputs:            inputs,
		curSerialInputIdx: 0,
	}
}

// Init is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Init() {
	for _, input := range s.inputs {
		input.Init()
	}
}

// Next is part of the Operator interface.
func (s *SerialUnorderedSynchronizer) Next(ctx context.Context) coldata.Batch {
	for {
		if s.curSerialInputIdx == len(s.inputs) {
			return coldata.ZeroBatch
		}
		b := s.inputs[s.curSerialInputIdx].Next(ctx)
		if b.Length() == 0 {
			s.curSerialInputIdx++
		} else {
			return b
		}
	}
}
