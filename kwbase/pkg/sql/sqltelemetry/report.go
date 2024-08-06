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

package sqltelemetry

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RecordError processes a SQL error. This includes both incrementing
// telemetry counters, and sending a sentry report for internal
// (assertion) errors.
func RecordError(ctx context.Context, err error, sv *settings.Values) {
	// In any case, record the counters.
	telemetry.RecordError(err)

	code := pgerror.GetPGCode(err)
	switch {
	case code == pgcode.Uncategorized:
		// For compatibility with 19.1 telemetry, keep track of the number
		// of occurrences of errors without code in telemetry. Over time,
		// we'll want this count to go down (i.e. more errors becoming
		// qualified with a code).
		//
		// TODO(knz): figure out if this telemetry is still useful.
		telemetry.Inc(UncategorizedErrorCounter)

	case code == pgcode.Internal || errors.HasAssertionFailure(err):
		// This is an assertion failure / crash.
		//
		// Note: not all assertion failures end up with code "internal".
		// For example, an assertion failure "underneath" a schema change
		// failure during a COMMIT for a multi-stmt txn will mask the
		// internal code and replace it with
		// TransactionCommittedWithSchemaChangeFailure.
		//
		// Conversely, not all errors with code "internal" are assertion
		// failures, but we still want to log/register them.

		// We want to log the internal error regardless of whether a
		// report is sent to sentry below.
		log.Errorf(ctx, "encountered internal error:\n%+v", err)

		if log.ShouldSendReport(sv) {
			msg, details, extraDetails := errors.BuildSentryReport(err)
			log.SendReport(ctx, msg, log.ReportTypeError, extraDetails, details...)
		}
	}
}
