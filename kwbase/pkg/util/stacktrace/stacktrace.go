// Copyright 2019 The Cockroach Authors.
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

package stacktrace

import (
	"bytes"
	"encoding/json"
	"fmt"

	raven "github.com/getsentry/raven-go"
)

// ReportableObject is an interface suitable for the extra detail
// objects provided to SendReport().
type ReportableObject = raven.Interface

// StackTrace is an object suitable for inclusion in errors that can
// ultimately be reported with ReportInternalError() or similar.
type StackTrace = raven.Stacktrace

// It also implements the interface ReportableObject below and is
// thus suitable for use with SendReport().
var _ ReportableObject = &StackTrace{}

// NewStackTrace generates a stacktrace suitable for inclusion in
// error reports.
func NewStackTrace(depth int) *StackTrace {
	const contextLines = 3
	return raven.NewStacktrace(depth+1, contextLines, kwdbPaths)
}

var kwdbPaths = []string{
	"gitee.com/kwbasedb/kwbase",
	"go.etcd.io/etcd/raft",
}

// PrintStackTrace produces a human-readable partial representation of
// the stack trace.
func PrintStackTrace(s *StackTrace) string {
	var buf bytes.Buffer
	for i := len(s.Frames) - 1; i >= 0; i-- {
		f := s.Frames[i]
		fmt.Fprintf(&buf, "%s:%d: in %s()\n", f.Filename, f.Lineno, f.Function)
	}
	return buf.String()
}

// EncodeStackTrace produces a decodable string representation of the
// stack trace.  This never fails.
func EncodeStackTrace(s *StackTrace) (string, error) {
	v, err := json.Marshal(s)
	if err != nil {
		return "<invalid stack trace>", err
	}
	return string(v), nil
}

// DecodeStackTrace produces a stack trace from the encoded string.
// If decoding fails, a boolean false is returned. In that case the
// caller is invited to include the string in the final reportable
// object, as a fallback (instead of discarding the stack trace
// entirely).
func DecodeStackTrace(s string) (*StackTrace, error) {
	var st raven.Stacktrace
	err := json.Unmarshal([]byte(s), &st)
	return &st, err
}
