// Copyright 2020 The Cockroach Authors.
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

package skip

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// SkippableTest is a testing.TB with Skip methods.
type SkippableTest interface {
	Skip(...interface{})
	Skipf(string, ...interface{})
}

// WithIssue skips this test, logging the given issue ID as the reason.
func WithIssue(t SkippableTest, githubIssueID int, args ...interface{}) {
	t.Skip(append([]interface{}{
		fmt.Sprintf("https://gitee.com/kwbasedb/kwbase/issue/%d", githubIssueID)},
		args...))
}

// IgnoreLint skips this test, explicitly marking it as not a test that
// should be tracked as a "skipped test" by external tools. You should use this
// if, for example, your test should only be run in Race mode.
func IgnoreLint(t SkippableTest, args ...interface{}) {
	t.Skip(args...)
}

// IgnoreLintf is like IgnoreLint, and it also takes a format string.
func IgnoreLintf(t SkippableTest, format string, args ...interface{}) {
	t.Skipf(format, args...)
}

// UnderRace skips this test if the race detector is enabled.
func UnderRace(t SkippableTest, args ...interface{}) {
	if util.RaceEnabled {
		t.Skip(append([]interface{}{"disabled under race"}, args...))
	}
}

// UnderShort skips this test if the -short flag is specified.
func UnderShort(t SkippableTest, args ...interface{}) {
	if testing.Short() {
		t.Skip(append([]interface{}{"disabled under -short"}, args...))
	}
}
