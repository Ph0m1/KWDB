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
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// CancelChecker is an Operator that checks whether query cancellation has
// occurred. The check happens on every batch.
type CancelChecker struct {
	OneInputNode
	NonExplainable

	// Number of times check() has been called since last context cancellation
	// check.
	callsSinceLastCheck uint32
}

// Init is part of the Operator interface.
func (c *CancelChecker) Init() {
	c.input.Init()
}

var _ Operator = &CancelChecker{}

// NewCancelChecker creates a new CancelChecker.
func NewCancelChecker(op Operator) *CancelChecker {
	return &CancelChecker{OneInputNode: NewOneInputNode(op)}
}

// Next is part of Operator interface.
func (c *CancelChecker) Next(ctx context.Context) coldata.Batch {
	c.checkEveryCall(ctx)
	return c.input.Next(ctx)
}

// Interval of check() calls to wait between checks for context cancellation.
// The value is a power of 2 to allow the compiler to use bitwise AND instead
// of division.
const cancelCheckInterval = 1024

// check panics with a query canceled error if the associated query has been
// canceled. The check is performed on every cancelCheckInterval'th call. This
// should be used only during long-running operations.
func (c *CancelChecker) check(ctx context.Context) {
	if c.callsSinceLastCheck%cancelCheckInterval == 0 {
		c.checkEveryCall(ctx)
	}

	// Increment. This may rollover when the 32-bit capacity is reached, but
	// that's all right.
	c.callsSinceLastCheck++
}

// checkEveryCall panics with query canceled error (which will be caught at the
// materializer level and will be propagated forward as metadata) if the
// associated query has been canceled. The check is performed on every call.
func (c *CancelChecker) checkEveryCall(ctx context.Context) {
	select {
	case <-ctx.Done():
		execerror.NonVectorizedPanic(sqlbase.QueryCanceledError)
	default:
	}
}
