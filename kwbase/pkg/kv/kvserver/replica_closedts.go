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

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
)

// EmitMLAI registers the replica's last assigned max lease index with the
// closed timestamp tracker. This is called to emit an update about this
// replica in the absence of write activity.
func (r *Replica) EmitMLAI() {
	r.mu.RLock()
	lai := r.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
	if r.mu.state.LeaseAppliedIndex > lai {
		lai = r.mu.state.LeaseAppliedIndex
	}
	epoch := r.mu.state.Lease.Epoch
	isLeaseholder := r.mu.state.Lease.Replica.ReplicaID == r.mu.replicaID
	r.mu.RUnlock()

	// If we're the leaseholder of an epoch-based lease, notify the minPropTracker
	// of the current LAI to trigger a re-broadcast of this range's LAI.
	if isLeaseholder && epoch > 0 {
		ctx := r.AnnotateCtx(context.Background())
		_, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
		untrack(ctx, ctpb.Epoch(epoch), r.RangeID, ctpb.LAI(lai))
	}
}
