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

package heapprofiler

import (
	"context"
	"os"
	"runtime/pprof"

	"gitee.com/kwbasedb/kwbase/pkg/server/dumpstore"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// HeapProfiler is used to take Go heap profiles.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time Go heap allocated bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type HeapProfiler struct {
	profiler
}

const heapFileNamePrefix = "memprof"

// NewHeapProfiler creates a HeapProfiler. dir is the directory in which
// profiles are to be stored.
func NewHeapProfiler(ctx context.Context, dir string, st *cluster.Settings) (*HeapProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewHeapProfiler")
	}

	log.Infof(ctx, "writing go heap profiles to %s at least every %s", dir, resetHighWaterMarkInterval)

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	hp := &HeapProfiler{
		profiler{
			store: newProfileStore(dumpStore, heapFileNamePrefix, st),
		},
	}
	return hp, nil
}

// MaybeTakeProfile takes a heap profile if the heap is big enough.
func (o *HeapProfiler) MaybeTakeProfile(ctx context.Context, curHeap int64) {
	o.maybeTakeProfile(ctx, curHeap, takeHeapProfile)
}

// takeHeapProfile returns true if and only if the profile dump was
// taken successfully.
func takeHeapProfile(ctx context.Context, path string) (success bool) {
	// Try writing a go heap profile.
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating go heap profile %s: %v", path, err)
		return false
	}
	defer f.Close()
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Warningf(ctx, "error writing go heap profile %s: %v", path, err)
		return false
	}
	return true
}
