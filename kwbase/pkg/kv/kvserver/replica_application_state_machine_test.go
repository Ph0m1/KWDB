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
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/apply"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
)

// TestReplicaStateMachineChangeReplicas tests the behavior of applying a
// replicated command with a ChangeReplicas trigger in a replicaAppBatch.
// The test exercises the logic of applying both old-style and new-style
// ChangeReplicas triggers.
func TestReplicaStateMachineChangeReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunTrueAndFalse(t, "add replica", func(t *testing.T, add bool) {
		testutils.RunTrueAndFalse(t, "deprecated", func(t *testing.T, deprecated bool) {
			tc := testContext{}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(t, stopper)

			// Lock the replica for the entire test.
			r := tc.repl
			r.raftMu.Lock()
			defer r.raftMu.Unlock()
			sm := r.getStateMachine()

			desc := r.Desc()
			replDesc, ok := desc.GetReplicaDescriptor(r.store.StoreID())
			require.True(t, ok)

			newDesc := *desc
			newDesc.InternalReplicas = append([]roachpb.ReplicaDescriptor(nil), desc.InternalReplicas...)
			var trigger roachpb.ChangeReplicasTrigger
			var confChange raftpb.ConfChange
			if add {
				// Add a new replica to the Range.
				addedReplDesc := newDesc.AddReplica(replDesc.NodeID+1, replDesc.StoreID+1, roachpb.VOTER_FULL)

				if deprecated {
					trigger = roachpb.ChangeReplicasTrigger{
						DeprecatedChangeType: roachpb.ADD_REPLICA,
						DeprecatedReplica:    addedReplDesc,
						DeprecatedUpdatedReplicas: []roachpb.ReplicaDescriptor{
							replDesc,
							addedReplDesc,
						},
						DeprecatedNextReplicaID: addedReplDesc.ReplicaID + 1,
					}
				} else {
					trigger = roachpb.ChangeReplicasTrigger{
						Desc:                  &newDesc,
						InternalAddedReplicas: []roachpb.ReplicaDescriptor{addedReplDesc},
					}
				}

				confChange = raftpb.ConfChange{
					Type:   raftpb.ConfChangeAddNode,
					NodeID: uint64(addedReplDesc.NodeID),
				}
			} else {
				// Remove ourselves from the Range.
				removedReplDesc, ok := newDesc.RemoveReplica(replDesc.NodeID, replDesc.StoreID)
				require.True(t, ok)

				if deprecated {
					trigger = roachpb.ChangeReplicasTrigger{
						DeprecatedChangeType:      roachpb.REMOVE_REPLICA,
						DeprecatedReplica:         removedReplDesc,
						DeprecatedUpdatedReplicas: []roachpb.ReplicaDescriptor{},
						DeprecatedNextReplicaID:   replDesc.ReplicaID + 1,
					}
				} else {
					trigger = roachpb.ChangeReplicasTrigger{
						Desc:                    &newDesc,
						InternalRemovedReplicas: []roachpb.ReplicaDescriptor{removedReplDesc},
					}
				}

				confChange = raftpb.ConfChange{
					Type:   raftpb.ConfChangeRemoveNode,
					NodeID: uint64(removedReplDesc.NodeID),
				}
			}

			// Create a new application batch.
			b := sm.NewBatch(false /* ephemeral */).(*replicaAppBatch)
			defer b.Close()

			// Stage a command with the ChangeReplicas trigger.
			cmd := &replicatedCmd{
				ctx: ctx,
				ent: &raftpb.Entry{
					Index: r.mu.state.RaftAppliedIndex + 1,
					Type:  raftpb.EntryConfChange,
				},
				decodedRaftEntry: decodedRaftEntry{
					idKey: makeIDKey(),
					raftCmd: storagepb.RaftCommand{
						ProposerLeaseSequence: r.mu.state.Lease.Sequence,
						MaxLeaseIndex:         r.mu.state.LeaseAppliedIndex + 1,
						ReplicatedEvalResult: storagepb.ReplicatedEvalResult{
							State:          &storagepb.ReplicaState{Desc: &newDesc},
							ChangeReplicas: &storagepb.ChangeReplicas{ChangeReplicasTrigger: trigger},
							Timestamp:      r.mu.state.GCThreshold.Add(1, 0),
						},
					},
					confChange: &decodedConfChange{
						ConfChangeI: confChange,
					},
				},
			}

			checkedCmd, err := b.Stage(cmd)
			require.NoError(t, err)
			require.Equal(t, !add, b.changeRemovesReplica)
			require.Equal(t, b.state.RaftAppliedIndex, cmd.ent.Index)
			require.Equal(t, b.state.LeaseAppliedIndex, cmd.raftCmd.MaxLeaseIndex)

			// Check the replica's destroy status.
			reason, _ := r.IsDestroyed()
			if add {
				require.Equal(t, destroyReasonAlive, reason)
			} else {
				require.Equal(t, destroyReasonRemoved, reason)
			}

			// Apply the batch to the StateMachine.
			err = b.ApplyToStateMachine(ctx)
			require.NoError(t, err)

			// Apply the side effects of the command to the StateMachine.
			_, err = sm.ApplySideEffects(checkedCmd)
			if add {
				require.NoError(t, err)
			} else {
				require.Equal(t, apply.ErrRemoved, err)
			}

			// Check whether the Replica still exists in the Store.
			_, err = tc.store.GetReplica(r.RangeID)
			if add {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.IsType(t, &roachpb.RangeNotFoundError{}, err)
			}
		})
	})
}
