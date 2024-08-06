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

// bufferOp is an operator that buffers a single batch at a time from an input,
// and makes it available to be read multiple times by downstream consumers.
type bufferOp struct {
	OneInputNode
	initStatus OperatorInitStatus

	// read is true if someone has read the current batch already.
	read  bool
	batch coldata.Batch
}

var _ Operator = &bufferOp{}

// NewBufferOp returns a new bufferOp, initialized to buffer batches from the
// supplied input.
func NewBufferOp(input Operator) Operator {
	return &bufferOp{
		OneInputNode: NewOneInputNode(input),
	}
}

func (b *bufferOp) Init() {
	// bufferOp can be an input to multiple operator chains, so Init on it can be
	// called multiple times. However, we do not want to call Init many times on
	// the input to bufferOp, so we do this check whether Init has already been
	// performed.
	if b.initStatus == OperatorNotInitialized {
		b.input.Init()
		b.initStatus = OperatorInitialized
	}
}

// rewind resets this buffer to be readable again.
// NOTE: it is the caller responsibility to restore the batch into the desired
// state.
func (b *bufferOp) rewind() {
	b.read = false
}

// advance reads the next batch from the input into the buffer, preparing itself
// for reads.
func (b *bufferOp) advance(ctx context.Context) {
	b.batch = b.input.Next(ctx)
	b.rewind()
}

func (b *bufferOp) Next(ctx context.Context) coldata.Batch {
	if b.read {
		return coldata.ZeroBatch
	}
	b.read = true
	return b.batch
}
