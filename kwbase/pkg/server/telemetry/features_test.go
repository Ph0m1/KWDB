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

package telemetry_test

import (
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"github.com/stretchr/testify/require"
)

// TestGetCounterDoesNotRace ensures that concurrent calls to GetCounter for
// the same feature always returns the same pointer. Even when this race was
// possible this test would fail on every run but would fail rapidly under
// stressrace.
func TestGetCounterDoesNotRace(t *testing.T) {
	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	counters := make([]telemetry.Counter, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			counters[i] = telemetry.GetCounter("test.foo")
			wg.Done()
		}(i)
	}
	wg.Wait()
	counterSet := make(map[telemetry.Counter]struct{})
	for _, c := range counters {
		counterSet[c] = struct{}{}
	}
	require.Len(t, counterSet, 1)
}
