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

package testshout

import (
	"context"
	"flag"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/log/logflags"
)

// Example_shout_before_log verifies that Shout output emitted after
// the log flags were set, but before the first log message was
// output, properly appears on stderr.
//
// This test needs to occur in its own test package where there is no
// other activity on the log flags, and no other log activity,
// otherwise the test's behavior will break on `make stress`.
func Example_shout_before_log() {
	origStderr := log.OrigStderr
	log.OrigStderr = os.Stdout
	defer func() { log.OrigStderr = origStderr }()

	if err := flag.Set(logflags.LogToStderrName, "WARNING"); err != nil {
		panic(err)
	}
	if err := flag.Set(logflags.NoRedirectStderrName, "false"); err != nil {
		panic(err)
	}

	log.Shout(context.Background(), log.Severity_INFO, "hello world")

	// output:
	// *
	// * INFO: hello world
	// *
}
