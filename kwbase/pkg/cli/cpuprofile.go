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

package cli

import (
	"context"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/server/dumpstore"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

var maxCombinedCPUProfFileSize = settings.RegisterByteSizeSetting(
	"server.cpu_profile.total_dump_size_limit",
	"maximum combined disk size of preserved CPU profiles",
	128<<20, // 128MiB
)

const cpuProfTimeFormat = "2006-01-02T15_04_05.000"
const cpuProfFileNamePrefix = "cpuprof."

type cpuProfiler struct{}

// PreFilter is part of the dumpstore.Dumper interface.
func (s cpuProfiler) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	// Always keep at least the last profile.
	for i := len(files) - 1; i >= 0; i-- {
		if s.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (s cpuProfiler) CheckOwnsFile(_ context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), cpuProfFileNamePrefix)
}

func initCPUProfile(ctx context.Context, dir string, st *cluster.Settings) {
	cpuProfileInterval := envutil.EnvOrDefaultDuration("KWBASE_CPUPROF_INTERVAL", -1)
	if cpuProfileInterval <= 0 {
		return
	}
	if min := time.Second; cpuProfileInterval < min {
		log.Infof(ctx, "fixing excessively short cpu profiling interval: %s -> %s",
			cpuProfileInterval, min)
		cpuProfileInterval = min
	}

	profilestore := dumpstore.NewStore(dir, maxCombinedCPUProfFileSize, st)
	profiler := dumpstore.Dumper(cpuProfiler{})

	// TODO(knz,tbg): The caller of initCPUProfile() also defines a stopper;
	// arguably this code would be better served by stopper.RunAsyncTask().
	go func() {
		defer log.RecoverAndReportPanic(ctx, &serverCfg.Settings.SV)

		ctx := context.Background()

		t := time.NewTicker(cpuProfileInterval)
		defer t.Stop()

		var currentProfileTime time.Time
		var currentProfile *os.File
		defer func() {
			if currentProfile != nil {
				pprof.StopCPUProfile()
				currentProfile.Close()
				profilestore.GC(ctx, currentProfileTime, profiler)
			}
		}()

		for {
			if err := func() error {
				currentProfileTime = timeutil.Now()
				name := cpuProfFileNamePrefix + currentProfileTime.Format(cpuProfTimeFormat)
				path := profilestore.GetFullPath(name)
				f, err := os.Create(path)
				if err != nil {
					return err

				}

				// Stop the current profile if it exists.
				if currentProfile != nil {
					pprof.StopCPUProfile()
					currentProfile.Close()
					currentProfile = nil
					profilestore.GC(ctx, currentProfileTime, profiler)
				}

				// Start the new profile.
				if err := pprof.StartCPUProfile(f); err != nil {
					f.Close()
					return err
				}

				currentProfile = f
				return nil
			}(); err != nil {
				// Log errors, but continue. There's always next time.
				log.Infof(ctx, "error during CPU profile: %s", err)
			}
		}
	}()
}
