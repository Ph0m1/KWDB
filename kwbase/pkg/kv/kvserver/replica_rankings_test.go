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

package kvserver

import (
	"math/rand"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestReplicaRankings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rr := newReplicaRankings()

	testCases := []struct {
		replicasByQPS []float64
	}{
		{replicasByQPS: []float64{}},
		{replicasByQPS: []float64{0}},
		{replicasByQPS: []float64{1, 0}},
		{replicasByQPS: []float64{3, 2, 1, 0}},
		{replicasByQPS: []float64{3, 3, 2, 2, 1, 1, 0, 0}},
		{replicasByQPS: []float64{1.1, 1.0, 0.9, -0.9, -1.0, -1.1}},
	}

	for _, tc := range testCases {
		acc := rr.newAccumulator()

		// Randomize the order of the inputs each time the test is run.
		want := make([]float64, len(tc.replicasByQPS))
		copy(want, tc.replicasByQPS)
		rand.Shuffle(len(tc.replicasByQPS), func(i, j int) {
			tc.replicasByQPS[i], tc.replicasByQPS[j] = tc.replicasByQPS[j], tc.replicasByQPS[i]
		})

		for i, replQPS := range tc.replicasByQPS {
			acc.addReplica(replicaWithStats{
				repl: &Replica{RangeID: roachpb.RangeID(i)},
				qps:  replQPS,
			})
		}
		rr.update(acc)

		// Make sure we can read off all expected replicas in the correct order.
		repls := rr.topQPS()
		if len(repls) != len(want) {
			t.Errorf("wrong number of replicas in output; got: %v; want: %v", repls, tc.replicasByQPS)
			continue
		}
		for i := range want {
			if repls[i].qps != want[i] {
				t.Errorf("got %f for %d'th element; want %f (input: %v)", repls[i].qps, i, want, tc.replicasByQPS)
				break
			}
		}
		replsCopy := rr.topQPS()
		if !reflect.DeepEqual(repls, replsCopy) {
			t.Errorf("got different replicas on second call to topQPS; first call: %v, second call: %v", repls, replsCopy)
		}
	}
}
