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

package tscoord_test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// setClusterArgs set cluster args
func setClusterArgs(nodes int, baseDir string) base.TestClusterArgs {
	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{},
	}

	for i := 0; i < nodes; i++ {
		args := base.TestServerArgs{}
		storeID := roachpb.StoreID(i + 1)
		path := filepath.Join(baseDir, fmt.Sprintf("s%d", storeID))
		args.StoreSpecs = []base.StoreSpec{{Path: path}}
		clusterArgs.ServerArgsPerNode[i] = args
	}
	return clusterArgs
}

// prepareTsTable create timeseries table return ts info
func prepareTsTable(t *testing.T, tc *testcluster.TestCluster) uint32 {
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TS DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.ts (ts timestamp not null, e1 int) tags(attr1 int not null) primary tags (attr1)`)
	//for i := 0; i <= 100; i++ {
	//	sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO d1.ts(ts, e1, attr1) VALUES (now(), %d, %d)`, i, i))
	//}

	var tableID, dbID uint32
	const tableIDQuery = `
SELECT tables.id, tables."parentID" FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	sqlDB.QueryRow(t, tableIDQuery, "d1", "ts").Scan(&tableID, &dbID)

	return tableID
}

// getPointInfo return point info
func getPointInfo(
	t *testing.T, ctx context.Context, tableID uint32,
) (*api.EntityRangeGroup, api.HashPartition, roachpb.RKey) {
	hashPoint := api.HashPoint(rand.Intn(api.HashParam))
	hashPointKey := roachpb.RKey(sqlbase.MakeTsHashPointKey(sqlbase.ID(tableID), uint64(hashPoint)))

	mgr, _ := api.GetHashRouterManagerWithTxn(ctx, nil)
	routerCaches, err := mgr.GetAllHashRouterInfo(ctx, nil)
	require.NoError(t, err)

	var hashRouter api.HashRouter
	for id, router := range routerCaches {
		if id == tableID {
			hashRouter = router
		}
	}

	group, err := hashRouter.GetGroupByHashPoint(ctx, hashPoint)
	require.NoError(t, err)

	hashPartition, err := hashRouter.GetPartitionByPoint(ctx, hashPoint)
	require.NoError(t, err)

	return group, hashPartition, hashPointKey
}

// getLeaseServerIdx return serverIdx
func getLeaseServerIdx(
	nodes int, tc *testcluster.TestCluster, lease api.EntityRangeGroupReplica,
) int {
	serverIdx := 0
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		if curServer.NodeID() == lease.NodeID {
			serverIdx = n
			break
		}
	}

	return serverIdx
}

// getLeaseServerIdx return serverIdx
func getReplicaServerIdx(
	nodes int, tc *testcluster.TestCluster, replicas []api.EntityRangeGroupReplica,
) []int {
	var serverIdx []int
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		for _, replica := range replicas {
			if curServer.NodeID() == replica.NodeID {
				serverIdx = append(serverIdx, n)
				break
			}
		}
	}

	return serverIdx
}

func requireDescMembers(
	t *testing.T, desc roachpb.RangeDescriptor, targets []roachpb.ReplicationTarget,
) {
	t.Skip("skip ts UT")
	t.Helper()
	targets = append([]roachpb.ReplicationTarget(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].StoreID < targets[j].StoreID })

	have := make([]roachpb.ReplicationTarget, 0, len(targets))
	for _, rDesc := range desc.Replicas().All() {
		have = append(have, roachpb.ReplicationTarget{
			NodeID:  rDesc.NodeID,
			StoreID: rDesc.StoreID,
		})
	}
	sort.Slice(have, func(i, j int) bool { return have[i].StoreID < have[j].StoreID })
	require.Equal(t, targets, have)
}

func requireLeaseAt(
	t *testing.T,
	tc *testcluster.TestCluster,
	desc roachpb.RangeDescriptor,
	target roachpb.ReplicationTarget,
) {
	t.Helper()
	// NB: under stressrace the lease will sometimes be inactive by the time
	// it's returned here, so don't use FindRangeLeaseHolder which fails when
	// that happens.
	testutils.SucceedsSoon(t, func() error {
		lease, _, err := tc.FindRangeLease(desc, &target)
		if err != nil {
			return err
		}
		if target != (roachpb.ReplicationTarget{
			NodeID:  lease.Replica.NodeID,
			StoreID: lease.Replica.StoreID,
		}) {
			return errors.Errorf("lease %v is not held by %+v", lease, target)
		}
		return nil
	})
}

func replicasCheck(
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	targets []roachpb.ReplicationTarget,
) {
	t.Skip("skip ts UT")
	desc, err := tc.Servers[0].LookupRange(startKey.AsRawKey())
	require.NoError(t, err)
	fmt.Printf("xxxx replicasCheck %v, %v, %v\n", desc.StartKey.String(), targets[0], desc.InternalReplicas)
	requireDescMembers(t, desc, targets)
	//requireLeaseAt(t, tc, desc, targets[0])
}

func transferCheck(
	t *testing.T,
	tc *testcluster.TestCluster,
	startKey roachpb.RKey,
	targets []roachpb.ReplicationTarget,
) {
	desc, err := tc.Servers[0].LookupRange(startKey.AsRawKey())
	require.NoError(t, err)
	requireLeaseAt(t, tc, desc, targets[0])
	fmt.Printf("xxxx %v, %v, %v\n", desc.StartKey.String(), targets[0], desc.InternalReplicas)
}

// TestAddPartitionReplicas test AddPartitionReplicas
func TestAddPartitionReplicas(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	group, hashPartition, hashPointKey := getPointInfo(t, ctx, tableID)

	var srcReplicas, destReplicas, allReplicas []api.EntityRangeGroupReplica
	// src replicas
	srcLeaseHolder := group.LeaseHolder
	srcReplicas = group.InternalReplicas

	// dest replicas
	for i := 0; i < nodes; i++ {
		nodeID := tc.Servers[i].NodeID()
		storeID := tc.Servers[i].GetFirstStoreID()
		allReplicas = append(allReplicas, api.EntityRangeGroupReplica{NodeID: nodeID, StoreID: storeID})
	}
	var addTargets []api.EntityRangeGroupReplica
	for _, t := range allReplicas {
		found := false
		for _, replicaDesc := range srcReplicas {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, api.EntityRangeGroupReplica{NodeID: t.NodeID, StoreID: t.StoreID})
		}
	}

	destReplicas = append(destReplicas, srcLeaseHolder)
	destReplicas = append(destReplicas, addTargets...)

	change := api.EntityRangePartitionMessage{
		Partition:            hashPartition,
		SrcLeaseHolder:       srcLeaseHolder,
		SrcInternalReplicas:  srcReplicas,
		DestLeaseHolder:      srcLeaseHolder,
		DestInternalReplicas: destReplicas,
	}
	fmt.Printf("xxxx src %v\n", srcReplicas)
	fmt.Printf("xxxx dest %v\n", destReplicas)

	// 123
	srcIdx := getReplicaServerIdx(nodes, tc, srcReplicas)
	replicasCheck(t, tc, hashPointKey, tc.Targets(srcIdx...))

	// addReplica: 123 -add45-> 12345
	destIdx := getReplicaServerIdx(nodes, tc, allReplicas)
	require.NoError(t, tc.Servers[0].TseDB().AddPartitionReplicas(ctx, sqlbase.ID(tableID), change))
	replicasCheck(t, tc, hashPointKey, tc.Targets(destIdx...))
}

// TestRemovePartitionReplicas test RemovePartitionReplicas
func TestRemovePartitionReplicas(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	group, hashPartition, hashPointKey := getPointInfo(t, ctx, tableID)

	var srcReplicas, destReplicas []api.EntityRangeGroupReplica
	// src replicas
	srcLeaseHolder := group.LeaseHolder
	srcReplicas = group.InternalReplicas

	// dest replicas
	destReplicas = append(destReplicas, srcLeaseHolder)

	change := api.EntityRangePartitionMessage{
		Partition:            hashPartition,
		SrcLeaseHolder:       srcLeaseHolder,
		SrcInternalReplicas:  srcReplicas,
		DestLeaseHolder:      srcLeaseHolder,
		DestInternalReplicas: destReplicas,
	}

	// preDistribution is 3 replicas if it is relocated
	// preDistribution: 123
	srcIdx := getReplicaServerIdx(nodes, tc, srcReplicas)
	replicasCheck(t, tc, hashPointKey, tc.Targets(srcIdx...))

	// removeReplica: 123 -del23-> 1
	destIdx := getLeaseServerIdx(nodes, tc, srcLeaseHolder)
	require.NoError(t, tc.Servers[0].TseDB().RemovePartitionReplicas(ctx, sqlbase.ID(tableID), change))
	replicasCheck(t, tc, hashPointKey, tc.Targets(destIdx))

	// Verify that the exec can be repeated
	// removeReplica: 123 -del23-> 1
	require.Error(t, tc.Servers[0].TseDB().RemovePartitionReplicas(ctx, sqlbase.ID(tableID), change))
	replicasCheck(t, tc, hashPointKey, tc.Targets(destIdx))
}

// TestTransferPartitionLease test TransferPartitionLease
func TestTransferPartitionLease(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	group, hashPartition, hashPointKey := getPointInfo(t, ctx, tableID)

	srcLeaseHolder := group.LeaseHolder
	destLeaseHolder := group.InternalReplicas[1]
	srcIdx := getLeaseServerIdx(nodes, tc, srcLeaseHolder)
	destIdx := getLeaseServerIdx(nodes, tc, destLeaseHolder)

	change := api.EntityRangePartitionMessage{
		Partition:       hashPartition,
		SrcLeaseHolder:  srcLeaseHolder,
		DestLeaseHolder: destLeaseHolder,
	}

	// 1(LH)
	transferCheck(t, tc, hashPointKey, tc.Targets(srcIdx))

	// 1(LH) -> 2(LH)
	require.NoError(t, tc.Servers[0].TseDB().TransferPartitionLease(ctx, sqlbase.ID(tableID), change))
	transferCheck(t, tc, hashPointKey, tc.Targets(destIdx))
}

// TestRelocatePartition test RelocatePartition
func TestRelocationPartition(t *testing.T) {
	t.Skip()
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	group, hashPartition, hashPointKey := getPointInfo(t, ctx, tableID)

	var srcReplicas, destReplicas, allReplicas []api.EntityRangeGroupReplica
	// src replicas
	srcLeaseHolder := group.LeaseHolder
	srcReplicas = group.InternalReplicas

	// dest replicas
	for i := 0; i < nodes; i++ {
		nodeID := tc.Servers[i].NodeID()
		storeID := tc.Servers[i].GetFirstStoreID()
		allReplicas = append(allReplicas, api.EntityRangeGroupReplica{NodeID: nodeID, StoreID: storeID})
	}
	var addTargets []api.EntityRangeGroupReplica
	for _, t := range allReplicas {
		found := false
		for _, replicaDesc := range srcReplicas {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, api.EntityRangeGroupReplica{NodeID: t.NodeID, StoreID: t.StoreID})
		}
	}

	destReplicas = append(destReplicas, srcLeaseHolder)
	destReplicas = append(destReplicas, addTargets...)

	srcIdx := getReplicaServerIdx(nodes, tc, srcReplicas)
	destIdx := getReplicaServerIdx(nodes, tc, destReplicas)

	// 123
	replicasCheck(t, tc, hashPointKey, tc.Targets(srcIdx...))

	change := api.EntityRangePartitionMessage{
		Partition:            hashPartition,
		SrcLeaseHolder:       srcLeaseHolder,
		SrcInternalReplicas:  srcReplicas,
		DestLeaseHolder:      srcLeaseHolder,
		DestInternalReplicas: destReplicas,
	}

	// relocate: 123 -> 123
	require.NoError(t, tc.Servers[0].TseDB().RelocatePartition(ctx, sqlbase.ID(tableID), change, false))
	replicasCheck(t, tc, hashPointKey, tc.Targets(destIdx...))
}

// TestAdminReplicaStatusConsistent test replica status consistent
func TestAdminReplicaStatusConsistent(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create timeseries table and return info
	tableID := prepareTsTable(t, tc)

	startKey := sqlbase.MakeTsHashPointKey(sqlbase.ID(tableID), 0)
	endKey := sqlbase.MakeTsHashPointKey(sqlbase.ID(tableID), api.HashParam)
	_, err := tc.Servers[0].DB().AdminReplicaStatusConsistent(ctx, startKey, endKey)
	require.NoError(t, err)
}

// TestIsReadyForTransferPartitionLease test SHOW TS PARTITIONS FROM TABLE tsDB.table
func TestShowTsPartitions(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TS DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.ts (ts timestamp not null, e1 int) tags(attr1 int not null) primary tags (attr1)`)

	var tableID uint32
	const tableIDQuery = `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	sqlDB.QueryRow(t, tableIDQuery, "d1", "ts").Scan(&tableID)

	// GetHashRoutingByTableID
	manager := tc.Servers[0].GetHashRouterManger().(*hashrouter.HRManager)
	hashRoutines, err := manager.GetHashRoutingsByTableID(ctx, nil, tableID)
	require.NoError(t, err)
	group := hashRoutines[0].EntityRangeGroup
	var partitionID uint32
	var hashStartPoint, hashEndPoint api.HashPoint
	var hashReplicas []roachpb.NodeID
	for id, part := range group.Partitions {
		if part.StartPoint > 0 && part.EndPoint < api.HashParam {
			partitionID = id
			hashStartPoint = part.StartPoint
			hashEndPoint = part.EndPoint
			break
		}
	}
	hashLeaseHolder := group.LeaseHolder.NodeID
	for _, replica := range group.InternalReplicas {
		hashReplicas = append(hashReplicas, replica.NodeID)
	}

	// SHOW TS PARTITIONS
	const partitionQuery = `
SELECT split_part(start_pretty, '/', 4), split_part(end_pretty, '/', 4), lease_holder, replicas 
FROM [SHOW TS PARTITIONS FROM TABLE d1.ts] 
WHERE partition_id = $1
`
	var showStartPoint, showEndPoint, showLeaseHolder int
	var showReplicasStr string
	showReplicas := make([]roachpb.NodeID, 3)
	sqlDB.QueryRow(t, partitionQuery, partitionID).Scan(&showStartPoint, &showEndPoint, &showLeaseHolder, &showReplicasStr)
	_, err = fmt.Sscanf(showReplicasStr, "{%d,%d,%d}", &showReplicas[0], &showReplicas[1], &showReplicas[2])
	require.NoError(t, err)

	require.Equal(t, hashStartPoint, api.HashPoint(showStartPoint))
	require.Equal(t, hashEndPoint, api.HashPoint(showEndPoint))
	require.Equal(t, hashLeaseHolder, roachpb.NodeID(showLeaseHolder))
	require.Equal(t, hashReplicas, showReplicas)
}

func TestGetPartitionCandidate(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create ts table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	_, hashPartition, _ := getPointInfo(t, ctx, tableID)

	_, err := tc.Servers[0].TseDB().GetPartitionCandidate(ctx, sqlbase.ID(tableID), hashPartition)
	require.NoError(t, err)
}

// TestIsReadyForTransferPartitionLease test is ready for transfer
func TestIsReadyForTransferPartitionLease(t *testing.T) {
	t.Skip("skip ts UT")
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	const nodes = 5
	clusterArgs := setClusterArgs(nodes, baseDir)
	tc := testcluster.StartTestCluster(t, nodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	// create ts table and return info
	tableID := prepareTsTable(t, tc)

	// random point info
	group, hashPartition, _ := getPointInfo(t, ctx, tableID)

	srcLeaseHolder := group.LeaseHolder
	destLeaseHolder := group.InternalReplicas[1]
	change := api.EntityRangePartitionMessage{
		Partition:       hashPartition,
		SrcLeaseHolder:  srcLeaseHolder,
		DestLeaseHolder: destLeaseHolder,
	}

	_, err := tc.Servers[0].TseDB().IsReadyForTransferPartitionLease(ctx, sqlbase.ID(tableID), change, 10)
	require.NoError(t, err)
}
