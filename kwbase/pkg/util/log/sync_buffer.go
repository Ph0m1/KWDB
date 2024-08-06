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

package log

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/build"
	"gitee.com/kwbasedb/kwbase/pkg/util/caller"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/petermattis/goid"
)

// syncBuffer joins a bufio.Writer to its underlying file, providing access to the
// file's Sync method and providing a wrapper for the Write method that provides log
// file rotation. There are conflicting methods, so the file cannot be embedded.
// l.mu is held for all its methods.
type syncBuffer struct {
	*bufio.Writer

	logger       *loggerT
	file         *os.File
	lastRotation int64
	nbytes       int64 // The number of bytes written to this file so far.
}

// Sync implements the flushSyncWriter interface.
//
// Note: the other methods from flushSyncWriter (Flush, io.Writer) is
// implemented by the embedded *bufio.Writer directly.
func (sb *syncBuffer) Sync() error {
	return sb.file.Sync()
}

func (sb *syncBuffer) Write(p []byte) (n int, err error) {
	if sb.nbytes+int64(len(p)) >= atomic.LoadInt64(&LogFileMaxSize) {
		if err := sb.rotateFile(timeutil.Now(), 0644); err != nil {
			sb.logger.exitLocked(err)
		}
	}
	n, err = sb.Writer.Write(p)
	sb.nbytes += int64(n)
	if err != nil {
		sb.logger.exitLocked(err)
	}
	return
}

// createFile initializes the syncBuffer for a logger, and triggers
// creation of the log file.
// Assumes that l.mu is held by the caller.
func (l *loggerT) createFile() error {
	now := timeutil.Now()
	if l.mu.file == nil {
		sb := &syncBuffer{
			logger: l,
		}
		if err := sb.rotateFile(now, l.fileMode); err != nil {
			return err
		}
		l.mu.file = sb
	}
	return nil
}

// rotateFile closes the syncBuffer's file and starts a new one.
func (sb *syncBuffer) rotateFile(now time.Time, fileMode os.FileMode) error {
	if sb.file != nil {
		if err := sb.Flush(); err != nil {
			return err
		}
		if err := sb.file.Close(); err != nil {
			return err
		}
	}
	var err error
	sb.file, sb.lastRotation, _, err = create(&sb.logger.logDir, sb.logger.prefix, now, sb.lastRotation, fileMode)
	sb.nbytes = 0
	if err != nil {
		return err
	}

	// Redirect stderr to the current INFO log file in order to capture panic
	// stack traces that are written by the Go runtime to stderr. Note that if
	// --logtostderr is true we'll never enter this code path and panic stack
	// traces will go to the original stderr as you would expect.
	if sb.logger.stderrRedirected() {
		// NB: any concurrent output to stderr may straddle the old and new
		// files. This doesn't apply to log messages as we won't reach this code
		// unless we're not logging to stderr.
		if err := hijackStderr(sb.file); err != nil {
			return err
		}
	}

	// bufferSize sizes the buffer associated with each log file. It's large
	// so that log records can accumulate without the logging thread blocking
	// on disk I/O. The flushDaemon will block instead.
	const bufferSize = 256 * 1024

	sb.Writer = bufio.NewWriterSize(sb.file, bufferSize)

	messages := make([]string, 0, 6)
	messages = append(messages,
		fmt.Sprintf("[config] file created at: %s\n", now.Format("2006/01/02 15:04:05")),
		fmt.Sprintf("[config] running on machine: %s\n", host),
		fmt.Sprintf("[config] binary: %s\n", build.GetInfo().Short()),
		fmt.Sprintf("[config] arguments: %s\n", os.Args),
	)

	logging.mu.Lock()
	if logging.mu.clusterID != "" {
		messages = append(messages, fmt.Sprintf("[config] clusterID: %s\n", logging.mu.clusterID))
	}
	logging.mu.Unlock()

	// Including a non-ascii character in the first 1024 bytes of the log helps
	// viewers that attempt to guess the character encoding.
	messages = append(messages, fmt.Sprintf("line format: [IWEF]yymmdd hh:mm:ss.uuuuuu goid file:line msg utf8=\u2713\n"))

	f, l, _ := caller.Lookup(1)
	for _, msg := range messages {
		buf := logging.formatLogEntry(Entry{
			Severity:  Severity_INFO,
			Time:      now.UnixNano(),
			Goroutine: goid.Get(),
			File:      f,
			Line:      int64(l),
			Message:   msg,
		}, nil, nil)
		var n int
		n, err = sb.file.Write(buf.Bytes())
		putBuffer(buf)
		sb.nbytes += int64(n)
		if err != nil {
			return err
		}
	}

	select {
	case sb.logger.gcNotify <- struct{}{}:
	default:
	}
	return nil
}
