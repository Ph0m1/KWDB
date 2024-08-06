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

package kvserver

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/tracker"
)

func TestLastUpdateTimesMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := make(lastUpdateTimesMap)
	t1 := time.Time{}.Add(time.Second)
	t2 := t1.Add(time.Second)
	m.update(3, t1)
	m.update(1, t2)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t2, 3: t1}, m)
	descs := []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}, {ReplicaID: 4}}

	t3 := t2.Add(time.Second)
	m.updateOnBecomeLeader(descs, t3)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{1: t3, 2: t3, 3: t3, 4: t3}, m)

	t4 := t3.Add(time.Second)
	descs = append(descs, []roachpb.ReplicaDescriptor{{ReplicaID: 5}, {ReplicaID: 6}}...)
	prs := map[uint64]tracker.Progress{
		1: {State: tracker.StateReplicate}, // should be updated
		// 2 is missing because why not
		3: {State: tracker.StateProbe},     // should be ignored
		4: {State: tracker.StateSnapshot},  // should be ignored
		5: {State: tracker.StateProbe},     // should be ignored
		6: {State: tracker.StateReplicate}, // should be added
		7: {State: tracker.StateReplicate}, // ignored, not in descs
	}
	m.updateOnUnquiesce(descs, prs, t4)
	assert.EqualValues(t, map[roachpb.ReplicaID]time.Time{
		1: t4,
		2: t3,
		3: t3,
		4: t3,
		6: t4,
	}, m)
}
