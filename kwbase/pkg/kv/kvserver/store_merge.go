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

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

// MergeRange expands the left-hand replica, leftRepl, to absorb the right-hand
// replica, identified by rightDesc. freezeStart specifies the time at which the
// right-hand replica promised to stop serving traffic and is used to initialize
// the timestamp cache's low water mark for the right-hand keyspace. The
// right-hand replica must exist on this store and the raftMus for both the
// left-hand and right-hand replicas must be held.
func (s *Store) MergeRange(
	ctx context.Context,
	leftRepl *Replica,
	newLeftDesc, rightDesc roachpb.RangeDescriptor,
	freezeStart hlc.Timestamp,
) error {
	if oldLeftDesc := leftRepl.Desc(); !oldLeftDesc.EndKey.Less(newLeftDesc.EndKey) {
		return errors.Errorf("the new end key is not greater than the current one: %+v <= %+v",
			newLeftDesc.EndKey, oldLeftDesc.EndKey)
	}

	rightRepl, err := s.GetReplica(rightDesc.RangeID)
	if err != nil {
		return err
	}

	leftRepl.raftMu.AssertHeld()
	rightRepl.raftMu.AssertHeld()

	// Shut down rangefeed processors on either side of the merge.
	//
	// It isn't strictly necessary to shut-down a rangefeed processor on the
	// surviving replica in a merge, but we choose to in order to avoid clients
	// who were monitoring both sides of the merge from establishing multiple
	// partial rangefeeds to the surviving range.
	// TODO(nvanbenschoten): does this make sense? We could just adjust the
	// bounds of the leftRepl.Processor.
	//
	// NB: removeInitializedReplicaRaftMuLocked also disconnects any initialized
	// rangefeeds with REASON_REPLICA_REMOVED. That's ok because we will have
	// already disconnected the rangefeed here.

	log.VEventf(context.Background(), 3, "xxxx registry disconnect store merge 1, rangeid %d, %p, rangeid %d, span: [%s - %s], %p",
		leftRepl.RangeID, leftRepl, rightRepl.RangeID,
		roachpb.KeyValue{Key: roachpb.Key(leftRepl.Desc().StartKey)}, roachpb.KeyValue{Key: roachpb.Key(leftRepl.Desc().EndKey)},
		rightRepl)

	leftRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_MERGED, rightRepl,
	)

	log.VEventf(context.Background(), 3, "xxxx registry disconnect store merge 2, rangeid %d, %p, rangeid %d, span: [%s - %s], %p",
		leftRepl.RangeID, leftRepl, rightRepl.RangeID,
		roachpb.KeyValue{Key: roachpb.Key(leftRepl.Desc().StartKey)}, roachpb.KeyValue{Key: roachpb.Key(leftRepl.Desc().EndKey)},
		rightRepl)

	rightRepl.disconnectRangefeedWithReason(
		roachpb.RangeFeedRetryError_REASON_RANGE_MERGED, nil,
	)

	if err := rightRepl.postDestroyRaftMuLocked(ctx, rightRepl.GetMVCCStats()); err != nil {
		return err
	}

	// Note that we were called (indirectly) from raft processing so we must
	// call removeInitializedReplicaRaftMuLocked directly to avoid deadlocking
	// on the right-hand replica's raftMu.
	if err := s.removeInitializedReplicaRaftMuLocked(ctx, rightRepl, rightDesc.NextReplicaID, RemoveOptions{
		DestroyData: false, // the replica was destroyed when the merge commit applied
	}); err != nil {
		return errors.Errorf("cannot remove range: %s", err)
	}

	if leftRepl.leaseholderStats != nil {
		leftRepl.leaseholderStats.resetRequestCounts()
	}
	if leftRepl.writeStats != nil {
		// Note: this could be drastically improved by adding a replicaStats method
		// that merges stats. Resetting stats is typically bad for the rebalancing
		// logic that depends on them.
		leftRepl.writeStats.resetRequestCounts()
	}

	// Clear the concurrency manager's lock and txn wait-queues to redirect the
	// queued transactions to the left-hand replica, if necessary.
	rightRepl.concMgr.OnRangeMerge()

	leftLease, _ := leftRepl.GetLease()
	rightLease, _ := rightRepl.GetLease()
	if leftLease.OwnedBy(s.Ident.StoreID) && !rightLease.OwnedBy(s.Ident.StoreID) {
		// We hold the lease for the LHS, but do not hold the lease for the RHS.
		// That means we don't have up-to-date timestamp cache entries for the
		// keyspace previously owned by the RHS. Bump the low water mark for the RHS
		// keyspace to freezeStart, the time at which the RHS promised to stop
		// serving traffic, as freezeStart is guaranteed to be greater than any
		// entry in the RHS's timestamp cache.
		//
		// Note that we need to update our clock with freezeStart to preserve the
		// invariant that our clock is always greater than or equal to any
		// timestamps in the timestamp cache. For a full discussion, see the comment
		// on TestStoreRangeMergeTimestampCacheCausality.
		s.Clock().Update(freezeStart)
		setTimestampCacheLowWaterMark(s.tsCache, &rightDesc, freezeStart)
	}

	// Update the subsuming range's descriptor.
	leftRepl.setDescRaftMuLocked(ctx, &newLeftDesc)
	return nil
}
