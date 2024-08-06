// Copyright 2017 The Cockroach Authors.
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

package contextutil

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// WithCancel adds an info log to context.WithCancel's CancelFunc. Prefer using
// WithCancelReason when possible.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return wrap(context.WithCancel(parent))
}

// reasonKey is a marker struct that's used to save the reason a context was
// canceled.
type reasonKey struct{}

// CancelWithReasonFunc is a context.CancelFunc that also passes along an error
// that is the reason for cancellation.
type CancelWithReasonFunc func(reason error)

// WithCancelReason adds a CancelFunc to this context, returning a new
// cancellable context and a CancelWithReasonFunc, which is like
// context.CancelFunc, except it also takes a "reason" error. The context that
// is canceled with this CancelWithReasonFunc will immediately be updated to
// contain this "reason". The reason can be retrieved with GetCancelReason.
// This function doesn't change the deadline of a context if it already exists.
func WithCancelReason(ctx context.Context) (context.Context, CancelWithReasonFunc) {
	val := new(atomic.Value)
	ctx = context.WithValue(ctx, reasonKey{}, val)
	ctx, cancel := wrap(context.WithCancel(ctx))
	return ctx, func(reason error) {
		val.Store(reason)
		cancel()
	}
}

// GetCancelReason retrieves the cancel reason for a context that has been
// created via WithCancelReason. The reason will be nil if the context was not
// created with WithCancelReason, or if the context has not been canceled yet.
// Otherwise, the reason will be the error that the context's
// CancelWithReasonFunc was invoked with.
func GetCancelReason(ctx context.Context) error {
	i := ctx.Value(reasonKey{})
	switch t := i.(type) {
	case *atomic.Value:
		return t.Load().(error)
	}
	return nil
}

func wrap(ctx context.Context, cancel context.CancelFunc) (context.Context, context.CancelFunc) {
	if !log.V(1) {
		return ctx, cancel
	}
	return ctx, func() {
		if log.V(2) {
			log.InfofDepth(ctx, 1, "canceling context:\n%s", debug.Stack())
		} else if log.V(1) {
			log.InfofDepth(ctx, 1, "canceling context")
		}
		cancel()
	}
}

// TimeoutError is a wrapped ContextDeadlineExceeded error. It indicates that
// an operation didn't complete within its designated timeout.
type TimeoutError struct {
	operation string
	duration  time.Duration
	cause     error
}

var _ error = (*TimeoutError)(nil)
var _ fmt.Formatter = (*TimeoutError)(nil)
var _ errors.Formatter = (*TimeoutError)(nil)

// We implement net.Error the same way that context.DeadlineExceeded does, so
// that people looking for net.Error attributes will still find them.
var _ net.Error = (*TimeoutError)(nil)

func (t *TimeoutError) Error() string { return fmt.Sprintf("%v", t) }

// Format implements fmt.Formatter.
func (t *TimeoutError) Format(s fmt.State, verb rune) { errors.FormatError(t, s, verb) }

// FormatError implements errors.Formatter.
func (t *TimeoutError) FormatError(p errors.Printer) error {
	p.Printf("operation %q timed out after %s", t.operation, t.duration)
	if t.cause != context.DeadlineExceeded {
		// If there were details (wrappers, stack trace etc.) ensure
		// they get printed.
		return t.cause
	}
	// We omit the "context deadline exceeded" suffix in the common case.
	return nil
}

// Timeout implements net.Error.
func (*TimeoutError) Timeout() bool { return true }

// Temporary implements net.Error.
func (*TimeoutError) Temporary() bool { return true }

// Cause implements Causer.
func (t *TimeoutError) Cause() error {
	return t.cause
}

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := fn(ctx)
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		err = &TimeoutError{
			operation: op,
			duration:  timeout,
			cause:     err,
		}
	}
	return err
}
