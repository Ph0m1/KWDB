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

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	OneInputNode
	NonExplainable

	fn func()
}

var _ resettableOperator = fnOp{}

func (f fnOp) Init() {
	f.input.Init()
}

func (f fnOp) Next(ctx context.Context) coldata.Batch {
	batch := f.input.Next(ctx)
	f.fn()
	return batch
}

func (f fnOp) reset(ctx context.Context) {
	if resettableOp, ok := f.input.(resetter); ok {
		resettableOp.reset(ctx)
	}
}
