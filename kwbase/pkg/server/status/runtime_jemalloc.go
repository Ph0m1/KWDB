// Copyright 2016 The Cockroach Authors.
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

// +build !stdmalloc

package status

// #cgo CPPFLAGS: -DJEMALLOC_NO_DEMANGLE
// #cgo LDFLAGS: -ljemalloc
// #cgo dragonfly freebsd LDFLAGS: -lm
// #cgo linux LDFLAGS: -lrt -lm -lpthread
//
// #include <jemalloc/jemalloc.h>
//
// // https://github.com/jemalloc/jemalloc/wiki/Use-Case:-Introspection-Via-mallctl*()
// // https://github.com/jemalloc/jemalloc/blob/4.5.0/src/stats.c#L960:L969
//
// typedef struct {
//   size_t Allocated;
//   size_t Active;
//   size_t Metadata;
//   size_t Resident;
//   size_t Mapped;
//   size_t Retained;
// } JemallocStats;
//
// int jemalloc_get_stats(JemallocStats *stats) {
//   // Update the statistics cached by je_mallctl.
//   uint64_t epoch = 1;
//   size_t sz = sizeof(epoch);
//   je_mallctl("epoch", &epoch, &sz, &epoch, sz);
//
//   int err;
//
//   sz = sizeof(&stats->Allocated);
//   err = je_mallctl("stats.allocated", &stats->Allocated, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Active);
//   err = je_mallctl("stats.active", &stats->Active, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Metadata);
//   err = je_mallctl("stats.metadata", &stats->Metadata, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Resident);
//   err = je_mallctl("stats.resident", &stats->Resident, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Mapped);
//   err = je_mallctl("stats.mapped", &stats->Mapped, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Retained);
//   err = je_mallctl("stats.retained", &stats->Retained, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   return err;
// }
import "C"

import (
	"context"
	"math"
	"reflect"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/dustin/go-humanize"
)

func init() {
	if getCgoMemStats != nil {
		panic("getCgoMemStats is already set")
	}
	getCgoMemStats = getJemallocStats
}

func getJemallocStats(ctx context.Context) (uint, uint, error) {
	var js C.JemallocStats
	// TODO(marc): should we panic here? Failure on fetching the stats may be a problem.
	if _, err := C.jemalloc_get_stats(&js); err != nil {
		return 0, 0, err
	}

	if log.V(2) {
		// Summary of jemalloc stats:
		v := reflect.ValueOf(js)
		t := v.Type()
		stats := make([]string, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			stats[i] = t.Field(i).Name + ": " + humanize.IBytes(uint64(v.Field(i).Interface().(C.size_t)))
		}
		log.Infof(ctx, "jemalloc stats: %s", strings.Join(stats, " "))
	}

	// NB: the `!V(MaxInt32)` condition is a workaround to not spew this to the
	// logs whenever log interception (log spy) is active. If we refactored
	// je_malloc_stats_print to return a string that we can `log.Infof` instead,
	// we wouldn't need this.
	if log.V(3) && !log.V(math.MaxInt32) {
		// Detailed jemalloc stats (very verbose, includes per-arena stats).
		C.je_malloc_stats_print(nil, nil, nil)
	}

	return uint(js.Allocated), uint(js.Resident), nil
}

// Used to force allocation in tests. 'import "C"' is not supported in tests.
func allocateMemory() {
	// Empirically, 8KiB is not enough, but 16KiB is. arm need 512KiB.
	C.malloc(512 << 10)
}
