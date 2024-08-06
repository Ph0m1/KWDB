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
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
)

func init() {
	mainLog.gcNotify = make(chan struct{}, 1)
}

// StartGCDaemon starts the log file GC -- this must be called after
// command-line parsing has completed so that no data is lost when the
// user configures larger max sizes than the defaults.
//
// The logger's GC daemon stops when the provided context is canceled.
//
// Note that secondary logger get their GC daemon started when
// they are allocated (NewSecondaryLogger). This assumes that
// secondary loggers are only allocated after command line parsing
// has completed too.
func StartGCDaemon(ctx context.Context) {
	go mainLog.gcDaemon(ctx)
}

// gcDaemon runs the GC loop for the given logger.
func (l *loggerT) gcDaemon(ctx context.Context) {
	l.gcOldFiles()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.gcNotify:
		}

		logging.mu.Lock()
		doGC := !logging.mu.disableDaemons
		logging.mu.Unlock()

		if doGC {
			l.gcOldFiles()
		}
	}
}

// gcOldFiles removes the "old" files that do not match
// the configured size and number threshold.
func (l *loggerT) gcOldFiles() {
	dir, isSet := l.logDir.get()
	if !isSet {
		// No log directory configured. Nothing to do.
		return
	}

	// This only lists the log files for the current logger (sharing the
	// prefix).
	allFiles, err := l.listLogFiles()
	if err != nil {
		fmt.Fprintf(OrigStderr, "unable to GC log files: %s\n", err)
		return
	}

	logFilesCombinedMaxSize := atomic.LoadInt64(&LogFilesCombinedMaxSize)
	files := selectFiles(allFiles, math.MaxInt64)
	if len(files) == 0 {
		return
	}
	// files is sorted with the newest log files first (which we want
	// to keep). Note that we always keep the most recent log file.
	sum := files[0].SizeBytes
	for _, f := range files[1:] {
		sum += f.SizeBytes
		if sum < logFilesCombinedMaxSize {
			continue
		}
		path := filepath.Join(dir, f.Name)
		if err := os.Remove(path); err != nil {
			fmt.Fprintln(OrigStderr, err)
		}
	}
}
