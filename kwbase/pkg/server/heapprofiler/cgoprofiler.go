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

package heapprofiler

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/server/dumpstore"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// NonGoAllocProfiler is used to take heap profiles for allocations
// performed outside of Go.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time Non-Go allocated bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type NonGoAllocProfiler struct {
	profiler
}

const jemallocFileNamePrefix = "jeprof"

// NewNonGoAllocProfiler creates a NonGoAllocProfiler. dir is the
// directory in which profiles are to be stored.
func NewNonGoAllocProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*NonGoAllocProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewHeapProfiler")
	}

	if jemallocHeapDump != nil {
		log.Infof(ctx, "writing jemalloc profiles to %s at last every %s", dir, resetHighWaterMarkInterval)
	} else {
		log.Infof(ctx, `to enable jmalloc profiling: "export MALLOC_CONF=prof:true" or "ln -s prof:true /etc/malloc.conf"`)
	}

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	hp := &NonGoAllocProfiler{
		profiler{
			store: newProfileStore(dumpStore, jemallocFileNamePrefix, st),
		},
	}
	return hp, nil
}

// MaybeTakeProfile takes a profile if the non-go size is big enough.
func (o *NonGoAllocProfiler) MaybeTakeProfile(ctx context.Context, curNonGoAlloc int64) {
	o.maybeTakeProfile(ctx, curNonGoAlloc, takeJemallocProfile)
}

// takeJemallocProfile returns true if and only if the jemalloc dump was taken successfully or jemalloc was not enabled.
func takeJemallocProfile(ctx context.Context, path string) (success bool) {
	if jemallocHeapDump == nil {
		return true
	}
	if err := jemallocHeapDump(path); err != nil {
		log.Warningf(ctx, "error writing jemalloc heap %s: %v", path, err)
		return false
	}
	return true
}

// jemallocHeapDump is an optional function to be called at heap dump time.
// This will be non-nil when jemalloc is linked in with profiling enabled.
// The function takes a filename to write the profile to.
var jemallocHeapDump func(string) error

// SetJemallocHeapDumpFn is used by the CLI package to inject the
// jemalloc heap collection function as a dependency. This is done
// here and not here so as to not impair the ability to run `go test`
// on the server package and sub-packages (Jemalloc needs custom link
// flags).
func SetJemallocHeapDumpFn(fn func(filename string) error) {
	if jemallocHeapDump != nil {
		panic("jemallocHeapDump is already set")
	}
	jemallocHeapDump = fn
}
