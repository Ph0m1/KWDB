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

package pgerror

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type withCandidateCode struct {
	cause error
	code  string
}

var _ error = (*withCandidateCode)(nil)
var _ errors.SafeDetailer = (*withCandidateCode)(nil)
var _ fmt.Formatter = (*withCandidateCode)(nil)
var _ errors.Formatter = (*withCandidateCode)(nil)

func (w *withCandidateCode) Error() string         { return w.cause.Error() }
func (w *withCandidateCode) Cause() error          { return w.cause }
func (w *withCandidateCode) Unwrap() error         { return w.cause }
func (w *withCandidateCode) SafeDetails() []string { return []string{w.code} }

func (w *withCandidateCode) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

func (w *withCandidateCode) FormatError(p errors.Printer) (next error) {
	if p.Detail() {
		p.Printf("candidate pg code: %s", w.code)
	}
	return w.cause
}

// decodeWithCandidateCode is a custom decoder that will be used when decoding
// withCandidateCode error objects.
// Note that as the last argument it takes proto.Message (and not
// protoutil.Message which is required by linter) because the latter brings in
// additional dependencies into this package and the former is sufficient here.
func decodeWithCandidateCode(
	_ context.Context, cause error, _ string, details []string, _ proto.Message,
) error {
	code := pgcode.Uncategorized
	if len(details) > 0 {
		code = details[0]
	}
	return &withCandidateCode{cause: cause, code: code}
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*withCandidateCode)(nil)), decodeWithCandidateCode)
}
