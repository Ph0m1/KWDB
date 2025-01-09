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
	"context"
	"fmt"
	"io"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/sysutil"
)

var (
	// LogSyncEnable controls whether the log is synchronized or not.
	LogSyncEnable = settings.RegisterPublicBoolSetting(
		"log.sync.enabled",
		"control whether the log is synchronized or not",
		true)
)

// flushSyncWriter is the interface satisfied by logging destinations.
type flushSyncWriter interface {
	Flush() error
	Sync() error
	io.Writer
}

// Flush explicitly flushes all pending log I/O.
// See also flushDaemon() that manages background (asynchronous)
// flushes, and signalFlusher() that manages flushes in reaction to a
// user signal.
func Flush() {
	mainLog.lockAndFlushAndSync(true /*doSync*/)
	secondaryLogRegistry.mu.Lock()
	defer secondaryLogRegistry.mu.Unlock()
	for _, l := range secondaryLogRegistry.mu.loggers {
		// Some loggers (e.g. the audit log) want to keep all the files.
		l.logger.lockAndFlushAndSync(true /*doSync*/)
	}
}

func init() {
	go flushDaemon()
	go signalFlusher()
}

// flushInterval is the delay between periodic flushes of the buffered log data.
const flushInterval = time.Second

// syncInterval is the multiple of flushInterval where the log is also synced to disk.
const syncInterval = 30

// maxSyncDuration is set to a conservative value since this is a new mechanism.
// In practice, even a fraction of that would indicate a problem.
var maxSyncDuration = envutil.EnvOrDefaultDuration("KWBASE_LOG_MAX_SYNC_DURATION", 30*time.Second)

// flushDaemon periodically flushes and syncs the log file buffers.
// This manages both the primary and secondary loggers.
//
// Flush propagates the in-memory buffer inside CockroachDB to the
// in-memory buffer(s) of the OS. The flush is relatively frequent so
// that a human operator can see "up to date" logging data in the log
// file.
//
// Syncs ensure that the OS commits the data to disk. Syncs are less
// frequent because they can incur more significant I/O costs.
func flushDaemon() {
	syncCounter := 1

	// This doesn't need to be Stop()'d as the loop never escapes.
	for range time.Tick(flushInterval) {
		doSync := syncCounter == syncInterval
		syncCounter = (syncCounter + 1) % syncInterval

		// Is flushing disabled?
		logging.mu.Lock()
		disableDaemons := logging.mu.disableDaemons
		logging.mu.Unlock()

		// Flush the main log.
		if !disableDaemons {
			mainLog.lockAndFlushAndSync(doSync)

			// Flush the secondary logs.
			secondaryLogRegistry.mu.Lock()
			for _, l := range secondaryLogRegistry.mu.loggers {
				l.logger.lockAndFlushAndSync(doSync)
			}
			secondaryLogRegistry.mu.Unlock()
		}
	}
}

// signalFlusher flushes the log(s) every time SIGHUP is received.
// This handles both the primary and secondary loggers.
func signalFlusher() {
	ch := sysutil.RefreshSignaledChan()
	for sig := range ch {
		Infof(context.Background(), "%s received, flushing logs", sig)
		Flush()
	}
}

// lockAndFlushAndSync is like flushAndSync but locks l.mu first.
func (l *loggerT) lockAndFlushAndSync(doSync bool) {
	l.mu.Lock()
	l.flushAndSync(doSync)
	l.mu.Unlock()
}

// SetSync configures whether logging synchronizes all writes.
// This overrides the synchronization setting for both primary
// and secondary loggers.
// This is used e.g. in `kwbase start` when an error occurs,
// to ensure that all log writes from the point the error
// occurs are flushed to logs (in case the error degenerates
// into a panic / segfault on the way out).
func SetSync(sync bool) {
	mainLog.lockAndSetSync(sync)
	func() {
		secondaryLogRegistry.mu.Lock()
		defer secondaryLogRegistry.mu.Unlock()
		for _, l := range secondaryLogRegistry.mu.loggers {
			if !sync && l.forceSyncWrites {
				// We're not changing this.
				continue
			}
			l.logger.lockAndSetSync(sync)
		}
	}()
	if sync {
		// There may be something in the buffers already; flush it.
		Flush()
	}
}

// lockAndSetSync configures syncWrites.
func (l *loggerT) lockAndSetSync(sync bool) {
	l.mu.Lock()
	l.mu.syncWrites = sync
	l.mu.Unlock()
}

// flushAndSync flushes the current log and, if doSync is set,
// attempts to sync its data to disk.
//
// l.mu is held.
func (l *loggerT) flushAndSync(doSync bool) {
	if l.mu.file == nil {
		return
	}

	logSync := true
	if sv := settings.TODO(); sv != nil {
		logSync = LogSyncEnable.Get(sv)
	}

	// If we can't sync within this duration, exit the process.
	t := time.AfterFunc(maxSyncDuration, func() {
		// NB: the disk-stall-detected roachtest matches on this message.
		if logSync {
			Shout(context.Background(), Severity_FATAL, fmt.Sprintf(
				"disk stall detected: unable to sync log files within %s", maxSyncDuration,
			))
		} else {
			Shout(context.Background(), Severity_WARNING, fmt.Sprintf(
				"disk stall detected: unable to sync log files within %s", maxSyncDuration,
			))
		}
	})
	defer t.Stop()

	_ = l.mu.file.Flush() // ignore error
	if doSync && logSync {
		_ = l.mu.file.Sync() // ignore error
	}
}
