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

package kvserver

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCalcRangeCounterIsLiveMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Regression test for a bug, see:
	// https://gitee.com/kwbasedb/kwbase/pull/39936#pullrequestreview-359059629

	desc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaDescriptors([]roachpb.ReplicaDescriptor{
			{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 100, StoreID: 110, ReplicaID: 120, Type: roachpb.ReplicaTypeVoterFull()},
			{NodeID: 1000, StoreID: 1100, ReplicaID: 1200, Type: roachpb.ReplicaTypeVoterFull()},
		}))

	{
		ctr, down, under, over := calcRangeCounter(1100 /* storeID */, desc, IsLiveMap{
			1000: IsLiveMapEntry{IsLive: true}, // by NodeID
		}, 3, 3)

		require.True(t, ctr)
		require.True(t, down)
		require.True(t, under)
		require.False(t, over)
	}

	{
		ctr, down, under, over := calcRangeCounter(1000, desc, IsLiveMap{
			1000: IsLiveMapEntry{IsLive: false},
		}, 3, 3)

		// Does not confuse a non-live entry for a live one. In other words,
		// does not think that the liveness map has only entries for live nodes.
		require.False(t, ctr)
		require.False(t, down)
		require.False(t, under)
		require.False(t, over)
	}
}
