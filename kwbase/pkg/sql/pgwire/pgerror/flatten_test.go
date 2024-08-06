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

package pgerror_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/testutils"
)

func TestFlatten(t *testing.T) {
	baseErr := errors.New("woo")

	testData := []struct {
		err     error
		checker func(t testutils.T, pgErr *pgerror.Error)
	}{
		{
			baseErr,
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				// Errors without code flatten to Uncategorized.
				t.CheckEqual(e.Code, pgcode.Uncategorized)
			},
		},
		{
			pgerror.WithCandidateCode(baseErr, pgcode.Syntax),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Code, pgcode.Syntax)
			},
		},
		{
			errors.WithHint(baseErr, "My Hint"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Hint, "My Hint")
			},
		},
		{
			errors.WithDetail(baseErr, "My Detail"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.Detail, "My Detail")
			},
		},
		{
			// This case for backward compatibility with 19.1.
			// Remove when the .TelemetryKey is removed from Error.
			errors.WithTelemetry(baseErr, "My Key"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Message, "woo")
				t.CheckEqual(e.TelemetryKey, "My Key")
			},
		},
		{
			unimplemented.New("woo", "woo"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Code, pgcode.FeatureNotSupported)
				t.CheckRegexpEqual(e.Hint, "You have attempted to use a feature that is not yet implemented")
				t.CheckRegexpEqual(e.Hint, "We appreciate your feedback")
			},
		},
		{
			errors.AssertionFailedf("woo"),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckEqual(e.Code, pgcode.Internal)
				t.CheckRegexpEqual(e.Hint, "You have encountered an unexpected error")
				t.CheckRegexpEqual(e.Hint, "We appreciate your feedback")
			},
		},
		{
			errors.Wrap(&roachpb.TransactionRetryWithProtoRefreshError{Msg: "woo"}, ""),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckRegexpEqual(e.Message, "restart transaction: .* woo")
				t.CheckEqual(e.Code, pgcode.SerializationFailure)
			},
		},
		{
			errors.Wrap(&roachpb.AmbiguousResultError{Message: "woo"}, ""),
			func(t testutils.T, e *pgerror.Error) {
				t.CheckRegexpEqual(e.Message, "result is ambiguous.*woo")
				t.CheckEqual(e.Code, pgcode.StatementCompletionUnknown)
			},
		},
	}
	tt := testutils.T{T: t}

	for _, test := range testData {
		// tt.Logf("input error: %# v", pretty.Formatter(test.err))
		pgErr := pgerror.Flatten(test.err)
		// tt.Logf("pg error: %# v", pretty.Formatter(pgErr))

		// Common checks for all errors.
		tt.CheckEqual(pgErr.Source.File, "flatten_test.go")
		tt.CheckEqual(pgErr.Source.Function, "TestFlatten")

		// Per-test specific checks.
		test.checker(tt, pgErr)
	}
}
