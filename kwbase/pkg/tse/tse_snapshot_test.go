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

package tse_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
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
func prepareTsTable(
	t *testing.T, ctx context.Context, tc *testcluster.TestCluster,
) (uint32, uint32, uint16, uint16, roachpb.NodeID, []roachpb.NodeID, []byte) {
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE TS DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.ts (ts timestamp not null, e1 int) tags(attr1 int not null) primary tags (attr1)`)
	for i := 0; i <= 100; i++ {
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO d1.ts(ts, e1, attr1) VALUES (now(), %d, %d)`, i, i))
	}

	var tableID, groupID, dbID uint32
	var startHashPoint, endHashPoint uint16
	var leaseHolder roachpb.NodeID
	var replicasStr string
	const tableIDQuery = `
SELECT tables.id, tables."parentID" FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	const partitionQuery = `
SELECT group_id, split_part(start_pretty, '/', 4), split_part(end_pretty, '/', 4), lease_holder, replicas
FROM kwdb_internal.kwdb_ts_partitions
WHERE database_name = $1
  AND table_name = $2
  AND partition_id = $3
`
	sqlDB.QueryRow(t, tableIDQuery, "d1", "ts").Scan(&tableID, &dbID)
	sqlDB.QueryRow(t, partitionQuery, "d1", "ts", 15).Scan(&groupID, &startHashPoint, &endHashPoint, &leaseHolder, &replicasStr)

	replicas := make([]roachpb.NodeID, 3)
	_, err := fmt.Sscanf(replicasStr, "{%d,%d,%d}", &replicas[0], &replicas[1], &replicas[2])
	require.NoError(t, err)

	var meta []byte
	err = tc.Servers[0].DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var table *sqlbase.TableDescriptor
		table, _, err = sqlbase.GetTsTableDescFromID(ctx, txn, sqlbase.ID(tableID))
		require.NoError(t, err)
		meta, err = sql.MakeNormalTSTableMeta(table)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	return tableID, groupID, startHashPoint, endHashPoint, leaseHolder, replicas, meta
}

// getOptServer return opt servers
func getOptServer(
	nodes int, tc *testcluster.TestCluster, leaseHolder roachpb.NodeID, replicas []roachpb.NodeID,
) (*server.TestServer, *server.TestServer) {
	var leaseServer, targetServer *server.TestServer
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		if curServer.NodeID() == leaseHolder {
			leaseServer = curServer
			break
		}
	}
	for n := 0; n < nodes; n++ {
		curServer := tc.Servers[n]
		found := false
		for _, replica := range replicas {
			if curServer.NodeID() == replica {
				found = true
				break
			}
		}
		if !found {
			targetServer = curServer
			break
		}
	}
	return leaseServer, targetServer
}

// TestSnapshotMultiNode test same group on diff nodes
// n1(g1) -> n4(g1)
func TestSnapshotMultiNode(t *testing.T) {
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
	tableID, groupID, startHashPoint, endHashPoint, leaseHolder, replicas, _ := prepareTsTable(t, ctx, tc)

	// get opt server
	leaseServer, targetServer := getOptServer(nodes, tc, leaseHolder, replicas)

	// create snapshot
	snapshotID, err := leaseServer.TSEngine().CreateSnapshot(uint64(tableID), uint64(groupID), uint64(startHashPoint), uint64(endHashPoint))
	require.NoError(t, err)

	limit := 1024
	totalSize := 0
	for offset := 0; offset <= totalSize; offset = offset + limit {
		// GetSnapshotData
		var data []byte
		data, totalSize, err = leaseServer.TSEngine().GetSnapshotData(uint64(tableID), uint64(groupID), snapshotID, offset, limit)
		require.NoError(t, err)

		// InitSnapshotForWrite
		if offset == 0 {
			err = targetServer.TSEngine().InitSnapshotForWrite(uint64(tableID), uint64(groupID), snapshotID, totalSize)
			require.NoError(t, err)
		}

		// WriteSnapshotData
		if offset+limit < totalSize {
			err = targetServer.TSEngine().WriteSnapshotData(uint64(tableID), uint64(groupID), snapshotID, offset, data, false)
			fmt.Printf("write rang: %d to %d of %d\n", offset, len(data), totalSize)
			require.NoError(t, err)
		} else {
			err = targetServer.TSEngine().WriteSnapshotData(uint64(tableID), uint64(groupID), snapshotID, offset, data, true)
			fmt.Printf("write rang: %d to %d of %d\n", offset, len(data), totalSize)
			require.NoError(t, err)
		}
	}

	// ApplySnapshot
	err = targetServer.TSEngine().ApplySnapshot(uint64(tableID), uint64(groupID), snapshotID)
	require.NoError(t, err)

	// DropSnapshot target
	err = targetServer.TSEngine().DropSnapshot(uint64(tableID), uint64(groupID), snapshotID)
	require.NoError(t, err)

	// DropSnapshot lease
	err = leaseServer.TSEngine().DropSnapshot(uint64(tableID), uint64(groupID), snapshotID)
	require.NoError(t, err)
}

// TestSnapshotSingleNode test two diff groups on same node
// n1(g1) -> n1(g2)
func TestSnapshotSingleNode(t *testing.T) {
	t.Skip()
}
