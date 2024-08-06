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
	"fmt"
	"io/ioutil"
	"os"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// setCorruptRaftMuLocked is a stand-in for proper handling of failing replicas.
// Such a failure is indicated by a call to setCorruptRaftMuLocked with a
// ReplicaCorruptionError. Currently any error is passed through, but
// prospectively it should stop the range from participating in progress,
// trigger a rebalance operation and decide on an error-by-error basis whether
// the corruption is limited to the range, store, node or cluster with
// corresponding actions taken.
//
// Despite the fatal log call below this message we still return for the
// sake of testing.
//
// TODO(d4l3k): when marking a Replica corrupt, must subtract its stats from
// r.store.metrics. Errors which happen between committing a batch and sending
// a stats delta from the store are going to be particularly tricky and the
// best bet is to not have any of those.
// @bdarnell remarks: Corruption errors should be rare so we may want the store
// to just recompute its stats in the background when one occurs.
func (r *Replica) setCorruptRaftMuLocked(
	ctx context.Context, cErr *roachpb.ReplicaCorruptionError,
) *roachpb.Error {
	r.readOnlyCmdMu.Lock()
	defer r.readOnlyCmdMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	log.ErrorfDepth(ctx, 1, "stalling replica due to: %s", cErr.ErrorMsg)
	cErr.Processed = true
	r.mu.destroyStatus.Set(cErr, destroyReasonRemoved)

	auxDir := r.store.engine.GetAuxiliaryDir()
	_ = os.MkdirAll(auxDir, 0755)
	path := base.PreventedStartupFile(auxDir)

	preventStartupMsg := fmt.Sprintf(`ATTENTION:

this node is terminating because replica %s detected an inconsistent state.
Please contact the CockroachDB support team. It is not necessarily safe
to replace this node; cluster data may still be at risk of corruption.

A file preventing this node from restarting was placed at:
%s
`, r, path)

	if err := ioutil.WriteFile(path, []byte(preventStartupMsg), 0644); err != nil {
		log.Warning(ctx, err)
	}

	log.FatalfDepth(ctx, 1, "replica is corrupted: %s", cErr)
	return roachpb.NewError(cErr)
}
