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
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

type testSplitDelayHelper struct {
	numAttempts int

	rangeID    roachpb.RangeID
	raftStatus *raft.Status
	sleep      func()

	slept, emptyProposed int
}

func (h *testSplitDelayHelper) RaftStatus(context.Context) (roachpb.RangeID, *raft.Status) {
	return h.rangeID, h.raftStatus
}
func (h *testSplitDelayHelper) ProposeEmptyCommand(ctx context.Context) {
	h.emptyProposed++
}
func (h *testSplitDelayHelper) NumAttempts() int {
	return h.numAttempts
}
func (h *testSplitDelayHelper) Sleep(context.Context) time.Duration {
	if h.sleep != nil {
		h.sleep()
	}
	h.slept++
	return time.Second
}

var _ splitDelayHelperI = (*testSplitDelayHelper)(nil)

func TestSplitDelayToAvoidSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("disabled", func(t *testing.T) {
		// Should immediately bail out if told to run zero attempts.
		h := &testSplitDelayHelper{
			numAttempts: 0,
			rangeID:     1,
			raftStatus:  nil,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.Equal(t, "", s)
		assert.Equal(t, 0, h.slept)
	})

	t.Run("follower", func(t *testing.T) {
		// Should immediately bail out if run on non-leader.
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  nil,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.Equal(t, "; not Raft leader", s)
		assert.Equal(t, 0, h.slept)
	})

	t.Run("inactive", func(t *testing.T) {
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus: &raft.Status{
				Progress: map[uint64]tracker.Progress{
					2: {State: tracker.StateProbe},
				},
			},
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		// We try to wake up the follower once, but then give up on it.
		assert.Equal(t, "; r1/2 inactive; delayed split for 1.0s to avoid Raft snapshot", s)
		assert.Equal(t, 1, h.slept)
		assert.Equal(t, 1, h.emptyProposed)
	})

	for _, state := range []tracker.StateType{tracker.StateProbe, tracker.StateSnapshot} {
		t.Run(state.String(), func(t *testing.T) {
			h := &testSplitDelayHelper{
				numAttempts: 5,
				rangeID:     1,
				raftStatus: &raft.Status{
					Progress: map[uint64]tracker.Progress{
						2: {
							State:        state,
							RecentActive: true,
							ProbeSent:    true, // Unifies string output below.
							Inflights:    &tracker.Inflights{},
						},
						// Healthy follower just for kicks.
						3: {State: tracker.StateReplicate},
					},
				},
			}
			s := maybeDelaySplitToAvoidSnapshot(ctx, h)
			assert.Equal(t, "; replica r1/2 not caught up: "+state.String()+
				" match=0 next=0 paused; delayed split for 5.0s to avoid Raft snapshot (without success)", s)
			assert.Equal(t, 5, h.slept)
			assert.Equal(t, 5, h.emptyProposed)
		})
	}

	t.Run("immediately-replicating", func(t *testing.T) {
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus: &raft.Status{
				Progress: map[uint64]tracker.Progress{
					2: {State: tracker.StateReplicate}, // intentionally not recently active
				},
			},
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.Equal(t, "", s)
		assert.Equal(t, 0, h.slept)
		assert.Equal(t, 0, h.emptyProposed)
	})

	t.Run("becomes-replicating", func(t *testing.T) {
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus: &raft.Status{
				Progress: map[uint64]tracker.Progress{
					2: {State: tracker.StateProbe, RecentActive: true, Inflights: &tracker.Inflights{}},
				},
			},
		}
		// The fourth attempt will see the follower catch up.
		h.sleep = func() {
			if h.slept == 2 {
				pr := h.raftStatus.Progress[2]
				pr.State = tracker.StateReplicate
				h.raftStatus.Progress[2] = pr
			}
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.Equal(t, "; delayed split for 3.0s to avoid Raft snapshot", s)
		assert.Equal(t, 3, h.slept)
		assert.Equal(t, 3, h.emptyProposed)
	})
}
