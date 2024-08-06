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

package kvserver_test

import (
	"context"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/confchange"
	"go.etcd.io/etcd/raft/tracker"
)

// TestAtomicReplicationChange is a simple smoke test for atomic membership
// changes.
func TestAtomicReplicationChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{},
			},
		},
		ReplicationMode: base.ReplicationManual,
	}
	tc := testcluster.StartTestCluster(t, 6, args)
	defer tc.Stopper().Stop(ctx)

	_, err := tc.ServerConn(0).Exec(`SET CLUSTER SETTING kv.atomic_replication_changes.enabled = true`)
	require.NoError(t, err)

	// Create a range and put it on n1, n2, n3. Intentionally do this one at a
	// time so we're not using atomic replication changes yet.
	k := tc.ScratchRange(t)
	desc, err := tc.AddReplicas(k, tc.Target(1))
	require.NoError(t, err)
	desc, err = tc.AddReplicas(k, tc.Target(2))
	require.NoError(t, err)

	runChange := func(expDesc roachpb.RangeDescriptor, chgs []roachpb.ReplicationChange) roachpb.RangeDescriptor {
		t.Helper()
		desc, err := tc.Servers[0].DB().AdminChangeReplicas(ctx, k, expDesc, chgs, false)
		require.NoError(t, err)

		return *desc
	}

	checkDesc := func(desc roachpb.RangeDescriptor, expStores ...roachpb.StoreID) {
		testutils.SucceedsSoon(t, func() error {
			var sawStores []roachpb.StoreID
			for _, s := range tc.Servers {
				r, _, _ := s.Stores().GetReplicaForRangeID(desc.RangeID)
				if r == nil {
					continue
				}
				if _, found := desc.GetReplicaDescriptor(r.StoreID()); !found {
					// There's a replica but it's not in the new descriptor, so
					// it should be replicaGC'ed soon.
					return errors.Errorf("%s should have been removed", r)
				}
				sawStores = append(sawStores, r.StoreID())
				// Check that in-mem descriptor of repl is up-to-date.
				if diff := pretty.Diff(&desc, r.Desc()); len(diff) > 0 {
					return errors.Errorf("diff(want, have):\n%s", strings.Join(diff, "\n"))
				}
				// Check that conf state is up to date. This can fail even though
				// the descriptor already matches since the descriptor is updated
				// a hair earlier.
				cfg, _, err := confchange.Restore(confchange.Changer{
					Tracker:   tracker.MakeProgressTracker(1),
					LastIndex: 1,
				}, desc.Replicas().ConfState())
				require.NoError(t, err)
				act := r.RaftStatus().Config.Voters
				if diff := pretty.Diff(cfg.Voters, act); len(diff) > 0 {
					return errors.Errorf("diff(exp,act):\n%s", strings.Join(diff, "\n"))
				}
			}
			assert.Equal(t, expStores, sawStores)
			return nil
		})
	}

	// Run a fairly general change.
	desc = runChange(desc, []roachpb.ReplicationChange{
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(3)},
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(5)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(2)},
		{ChangeType: roachpb.ADD_REPLICA, Target: tc.Target(4)},
	})

	// Replicas should now live on all stores except s3.
	checkDesc(desc, 1, 2, 4, 5, 6)

	// Transfer the lease to s5.
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(4)))

	// Rebalance back down all the way.
	desc = runChange(desc, []roachpb.ReplicationChange{
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(0)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(1)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(3)},
		{ChangeType: roachpb.REMOVE_REPLICA, Target: tc.Target(5)},
	})

	// Only a lone voter on s5 should be left over.
	checkDesc(desc, 5)
}
