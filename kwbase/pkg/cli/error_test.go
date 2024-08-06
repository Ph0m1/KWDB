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

package cli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errBase := errors.New("woo")
	file, line, fn, _ := errors.GetOneLineSource(errBase)
	refLoc := fmt.Sprintf("%s, %s:%d", fn, file, line)
	testData := []struct {
		err                   error
		showSeverity, verbose bool
		exp                   string
	}{
		// Check the basic with/without severity.
		{errBase, false, false, "woo"},
		{errBase, true, false, "ERROR: woo"},
		{pgerror.WithCandidateCode(errBase, pgcode.Syntax), false, false, "woo\nSQLSTATE: " + pgcode.Syntax},
		// Check the verbose output. This includes the uncategorized sqlstate.
		{errBase, false, true, "woo\nSQLSTATE: " + pgcode.Uncategorized + "\nLOCATION: " + refLoc},
		{errBase, true, true, "ERROR: woo\nSQLSTATE: " + pgcode.Uncategorized + "\nLOCATION: " + refLoc},
		// Check the same over pq.Error objects.
		{&pq.Error{Message: "woo"}, false, false, "woo"},
		{&pq.Error{Message: "woo"}, true, false, "ERROR: woo"},
		{&pq.Error{Message: "woo"}, false, true, "woo"},
		{&pq.Error{Message: "woo"}, true, true, "ERROR: woo"},
		{&pq.Error{Severity: "W", Message: "woo"}, false, false, "woo"},
		{&pq.Error{Severity: "W", Message: "woo"}, true, false, "W: woo"},
		// Check hint printed after message.
		{errors.WithHint(errBase, "hello"), false, false, "woo\nHINT: hello"},
		// Check sqlstate printed before hint, location after hint.
		{errors.WithHint(errBase, "hello"), false, true, "woo\nSQLSTATE: " + pgcode.Uncategorized + "\nHINT: hello\nLOCATION: " + refLoc},
		// Check detail printed after message.
		{errors.WithDetail(errBase, "hello"), false, false, "woo\nDETAIL: hello"},
		// Check hint/detail collection, hint printed after detail.
		{errors.WithHint(
			errors.WithDetail(
				errors.WithHint(errBase, "a"),
				"b"),
			"c"), false, false, "woo\nDETAIL: b\nHINT: a\n--\nc"},
		{errors.WithDetail(
			errors.WithHint(
				errors.WithDetail(errBase, "a"),
				"b"),
			"c"), false, false, "woo\nDETAIL: a\n--\nc\nHINT: b"},
		// Check sqlstate printed before detail, location after hint.
		{errors.WithDetail(
			errors.WithHint(errBase, "a"), "b"),
			false, true, "woo\nSQLSTATE: " + pgcode.Uncategorized + "\nDETAIL: b\nHINT: a\nLOCATION: " + refLoc},
	}

	for _, tc := range testData {
		var buf strings.Builder
		cliOutputError(&buf, tc.err, tc.showSeverity, tc.verbose)
		assert.Equal(t, tc.exp+"\n", buf.String())
	}
}

func TestFormatLocation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		file, line, fn string
		exp            string
	}{
		{"", "", "", ""},
		{"a.b", "", "", "a.b"},
		{"", "123", "", "<unknown>:123"},
		{"", "", "abc", "abc"},
		{"a.b", "", "abc", "abc, a.b"},
		{"a.b", "123", "", "a.b:123"},
		{"", "123", "abc", "abc, <unknown>:123"},
	}

	for _, tc := range testData {
		r := formatLocation(tc.file, tc.line, tc.fn)
		assert.Equal(t, tc.exp, r)
	}
}

type logger struct {
	TB       testing.TB
	Severity log.Severity
	Err      error
}

func (l *logger) Log(_ context.Context, sev log.Severity, args ...interface{}) {
	require.Equal(l.TB, 1, len(args), "expected to log one item")
	err, ok := args[0].(error)
	require.True(l.TB, ok, "expected to log an error")
	l.Severity = sev
	l.Err = err
}

func TestErrorReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		desc         string
		err          error
		wantSeverity log.Severity
		wantCLICause bool // should the cause be a *cliError?
	}{
		{
			desc:         "plain",
			err:          errors.New("boom"),
			wantSeverity: log.Severity_ERROR,
			wantCLICause: false,
		},
		{
			desc: "single cliError",
			err: &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause:    errors.New("routine"),
			},
			wantSeverity: log.Severity_INFO,
			wantCLICause: false,
		},
		{
			desc: "double cliError",
			err: &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause: &cliError{
					exitCode: 1,
					severity: log.Severity_ERROR,
					cause:    errors.New("serious"),
				},
			},
			wantSeverity: log.Severity_INFO, // should only unwrap one layer
			wantCLICause: true,
		},
		{
			desc: "wrapped cliError",
			err: fmt.Errorf("some context: %w", &cliError{
				exitCode: 1,
				severity: log.Severity_INFO,
				cause:    errors.New("routine"),
			}),
			wantSeverity: log.Severity_INFO,
			wantCLICause: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			got := &logger{TB: t}
			checked := checkAndMaybeShoutTo(tt.err, got.Log)
			assert.Equal(t, tt.err, checked, "should return error unchanged")
			assert.Equal(t, tt.wantSeverity, got.Severity, "wrong severity log")
			_, gotCLI := got.Err.(*cliError)
			if tt.wantCLICause {
				assert.True(t, gotCLI, "logged cause should be *cliError, got %T", got.Err)
			} else {
				assert.False(t, gotCLI, "logged cause shouldn't be *cliError, got %T", got.Err)
			}
		})
	}
}
