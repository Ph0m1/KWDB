// Copyright 2018 The Cockroach Authors.
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

package log

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/util/caller"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// SecondaryLogger represents a secondary / auxiliary logging channel
// whose logging events go to a different file than the main logging
// facility.
type SecondaryLogger struct {
	logger          loggerT
	msgCount        uint64
	enableMsgCount  bool
	forceSyncWrites bool
}

// SetFileMode set the mode of log file
func (l *SecondaryLogger) SetFileMode(fileMode os.FileMode) {
	l.logger.fileMode = fileMode
}

var secondaryLogRegistry struct {
	mu struct {
		syncutil.Mutex
		loggers []*SecondaryLogger
	}
}

// NewSecondaryLogger creates a secondary logger.
//
// The given directory name can be either nil or empty, in which case
// the global logger's own dirName is used; or non-nil and non-empty,
// in which case it specifies the directory for that new logger.
//
// The logger's GC daemon stops when the provided context is canceled.
//
// The caller is responsible for ensuring the Close() method is
// eventually called.
func NewSecondaryLogger(
	ctx context.Context,
	dirName *DirName,
	fileNamePrefix string,
	enableGc bool,
	forceSyncWrites bool,
	enableMsgCount bool,
) *SecondaryLogger {
	mainLog.mu.Lock()
	defer mainLog.mu.Unlock()
	var dir string
	if dirName != nil {
		dir = dirName.String()
	}
	if dir == "" {
		dir = mainLog.logDir.String()
	}
	l := &SecondaryLogger{
		logger: loggerT{
			logDir:           DirName{name: dir},
			prefix:           program + "-" + fileNamePrefix,
			fileThreshold:    Severity_INFO,
			noStderrRedirect: true,
			gcNotify:         make(chan struct{}, 1),
		},
		forceSyncWrites: forceSyncWrites,
		enableMsgCount:  enableMsgCount,
	}
	l.logger.mu.syncWrites = forceSyncWrites || mainLog.mu.syncWrites

	// Ensure the registry knows about this logger.
	secondaryLogRegistry.mu.Lock()
	secondaryLogRegistry.mu.loggers = append(secondaryLogRegistry.mu.loggers, l)
	secondaryLogRegistry.mu.Unlock()

	if enableGc {
		// Start the log file GC for the secondary logger.
		go l.logger.gcDaemon(ctx)
	}

	return l
}

// Close implements the stopper.Closer interface.
func (l *SecondaryLogger) Close() {
	// Make the registry forget about this logger. This avoids
	// stacking many secondary loggers together when there are
	// subsequent tests starting servers in the same package.
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for i, thatLogger := range secondaryLogRegistry.mu.loggers {
		if thatLogger != l {
			continue
		}
		secondaryLogRegistry.mu.loggers = append(secondaryLogRegistry.mu.loggers[:i], secondaryLogRegistry.mu.loggers[i+1:]...)
		return
	}
}

func (l *SecondaryLogger) output(
	ctx context.Context, depth int, sev Severity, format string, args ...interface{},
) {
	file, line, _ := caller.Lookup(depth + 1)
	var buf strings.Builder
	formatTags(ctx, &buf)

	if l.enableMsgCount {
		// Add a counter. This is important for the SQL audit logs.
		counter := atomic.AddUint64(&l.msgCount, 1)
		fmt.Fprintf(&buf, "%d ", counter)
	}

	if format == "" {
		fmt.Fprint(&buf, args...)
	} else {
		fmt.Fprintf(&buf, format, args...)
	}
	l.logger.outputLogEntry(Severity_INFO, file, line, buf.String())
}

// Logf logs an event on a secondary logger.
func (l *SecondaryLogger) Logf(ctx context.Context, format string, args ...interface{}) {
	l.output(ctx, 1, Severity_INFO, format, args...)
}

// LogfDepth logs an event on a secondary logger, offsetting the caller's stack
// frame by 'depth'
func (l *SecondaryLogger) LogfDepth(
	ctx context.Context, depth int, format string, args ...interface{},
) {
	l.output(ctx, depth+1, Severity_INFO, format, args...)
}

// LogSev logs an event at the specified severity on a secondary logger.
func (l *SecondaryLogger) LogSev(ctx context.Context, sev Severity, args ...interface{}) {
	l.output(ctx, 1, Severity_INFO, "", args...)
}
