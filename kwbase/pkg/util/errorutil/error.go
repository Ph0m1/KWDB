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

package errorutil

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// UnexpectedWithIssueErrorf indicates an error with an associated Github issue.
// It's supposed to be used for conditions that would otherwise be checked by
// assertions, except that they fail and we need the public's help for tracking
// it down.
// The error message will invite users to report repros.
func UnexpectedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	err := errors.Newf(format, args...)
	err = errors.Wrap(err, "unexpected error")
	err = errors.WithSafeDetails(err, "issue #%d", errors.Safe(issue))
	err = errors.WithHint(err,
		fmt.Sprintf("We've been trying to track this particular issue down. "+
			"Please report your reproduction at "+
			"https://gitee.com/kwbasedb/kwbase/issues/%d "+
			"unless that issue seems to have been resolved "+
			"(in which case you might want to update kwdb to a newer version).",
			issue))
	return err
}

// SendReport creates a Sentry report about the error, if the settings allow.
// The format string will be reproduced ad litteram in the report; the arguments
// will be sanitized.
func SendReport(ctx context.Context, sv *settings.Values, err error) {
	if !log.ShouldSendReport(sv) {
		return
	}
	msg, details, extraDetails := errors.BuildSentryReport(err)
	log.SendReport(ctx, msg, log.ReportTypeError, extraDetails, details...)
}
