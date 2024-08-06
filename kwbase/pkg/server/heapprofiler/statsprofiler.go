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
	"encoding/json"
	"fmt"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/server/dumpstore"
	"gitee.com/kwbasedb/kwbase/pkg/server/status"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// StatsProfiler is used to take snapshots of the overall memory statistics
// to break down overall RSS memory usage.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time RSS bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type StatsProfiler struct {
	profiler
}

const statsFileNamePrefix = "memstats"

// NewStatsProfiler creates a StatsProfiler. dir is the
// directory in which profiles are to be stored.
func NewStatsProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*StatsProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewStatsProfiler")
	}

	log.Infof(ctx, "writing memory stats to %s at last every %s", dir, resetHighWaterMarkInterval)

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	hp := &StatsProfiler{
		profiler{
			store: newProfileStore(dumpStore, statsFileNamePrefix, st),
		},
	}
	return hp, nil
}

// MaybeTakeProfile takes a profile if the non-go size is big enough.
func (o *StatsProfiler) MaybeTakeProfile(
	ctx context.Context, curRSS int64, ms *status.GoMemStats, cs *status.CGoMemStats,
) {
	o.maybeTakeProfile(ctx, curRSS, func(ctx context.Context, path string) bool { return saveStats(ctx, path, ms, cs) })
}

func saveStats(
	ctx context.Context, path string, ms *status.GoMemStats, cs *status.CGoMemStats,
) bool {
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating stats profile %s: %v", path, err)
		return false
	}
	defer f.Close()
	msJ, err := json.MarshalIndent(&ms.MemStats, "", "  ")
	if err != nil {
		log.Warningf(ctx, "error marshaling stats profile %s: %v", path, err)
		return false
	}
	csJ, err := json.MarshalIndent(cs, "", "  ")
	if err != nil {
		log.Warningf(ctx, "error marshaling stats profile %s: %v", path, err)
		return false
	}
	_, err = fmt.Fprintf(f, "Go memory stats:\n%s\n----\nNon-Go stats:\n%s\n", msJ, csJ)
	if err != nil {
		log.Warningf(ctx, "error writing stats profile %s: %v", path, err)
		return false
	}
	return true
}
