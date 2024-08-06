// Copyright 2017 The Cockroach Authors.
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

package log

import "os"

// OrigStderr points to the original stderr stream.
var OrigStderr = func() *os.File {
	fd, err := dupFD(os.Stderr.Fd())
	if err != nil {
		panic(err)
	}

	return os.NewFile(fd, os.Stderr.Name())
}()

// LoggingToStderr returns true if log messages of the given severity
// sent to the main logger are also visible on stderr.
func LoggingToStderr(s Severity) bool {
	return s >= logging.stderrThreshold.get()
}

// stderrRedirected returns true if and only if logging to this logger
// captures stderr output to the log file. This is used e.g. by
// Shout() to determine whether to report to standard error in
// addition to logs.
func (l *loggerT) stderrRedirected() bool {
	return logging.stderrThreshold > Severity_INFO && !l.noStderrRedirect
}

// hijackStderr replaces stderr with the given file descriptor.
//
// A client that wishes to use the original stderr must use
// OrigStderr defined above.
func hijackStderr(f *os.File) error {
	return redirectStderr(f)
}

// restoreStderr cancels the effect of hijackStderr().
func restoreStderr() error {
	return redirectStderr(OrigStderr)
}
