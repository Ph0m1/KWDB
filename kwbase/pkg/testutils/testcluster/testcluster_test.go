// Copyright 2016 The Cockroach Authors.
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

package testcluster

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "t",
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	s0 := sqlutils.MakeSQLRunner(tc.Conns[0])
	s1 := sqlutils.MakeSQLRunner(tc.Conns[1])
	s2 := sqlutils.MakeSQLRunner(tc.Conns[2])

	s0.Exec(t, `CREATE DATABASE t`)
	s0.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY, v INT)`)
	s0.Exec(t, `INSERT INTO test VALUES (5, 1), (4, 2), (1, 2)`)

	if r := s1.Query(t, `SELECT * FROM test WHERE k = 5`); !r.Next() {
		t.Fatal("no rows")
	} else {
		r.Close()
	}

	s2.ExecRowsAffected(t, 3, `DELETE FROM test`)

	// Split the table to a new range.
	kvDB := tc.Servers[0].DB()
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	tableStartKey := keys.MakeTablePrefix(uint32(tableDesc.ID))
	leftRangeDesc, tableRangeDesc, err := tc.SplitRange(tableStartKey)
	if err != nil {
		t.Fatal(err)
	}
	log.Infof(context.Background(), "After split got ranges: %+v and %+v.", leftRangeDesc, tableRangeDesc)
	if len(tableRangeDesc.InternalReplicas) == 0 {
		t.Fatalf(
			"expected replica on node 1, got no replicas: %+v", tableRangeDesc.InternalReplicas)
	}
	if tableRangeDesc.InternalReplicas[0].NodeID != 1 {
		t.Fatalf(
			"expected replica on node 1, got replicas: %+v", tableRangeDesc.InternalReplicas)
	}

	// Replicate the table's range to all the nodes.
	tableRangeDesc, err = tc.AddReplicas(
		tableRangeDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableRangeDesc.InternalReplicas) != 3 {
		t.Fatalf("expected 3 replicas, got %+v", tableRangeDesc.InternalReplicas)
	}
	for i := 0; i < 3; i++ {
		if _, ok := tableRangeDesc.GetReplicaDescriptor(
			tc.Servers[i].GetFirstStoreID()); !ok {
			t.Fatalf("expected replica on store %d, got %+v",
				tc.Servers[i].GetFirstStoreID(), tableRangeDesc.InternalReplicas)
		}
	}

	// Transfer the lease to node 1.
	leaseHolder, err := tc.FindRangeLeaseHolder(
		tableRangeDesc,
		&roachpb.ReplicationTarget{
			NodeID:  tc.Servers[0].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[0].GetFirstStoreID(),
		})
	if err != nil {
		t.Fatal(err)
	}
	if leaseHolder.StoreID != tc.Servers[0].GetFirstStoreID() {
		t.Fatalf("expected initial lease on server idx 0, but is on node: %+v",
			leaseHolder)
	}

	err = tc.TransferRangeLease(tableRangeDesc, tc.Target(1))
	if err != nil {
		t.Fatal(err)
	}

	// Check that the lease holder has changed. We'll use the old lease holder as
	// the hint, since it's guaranteed that the old lease holder has applied the
	// new lease.
	leaseHolder, err = tc.FindRangeLeaseHolder(
		tableRangeDesc,
		&roachpb.ReplicationTarget{
			NodeID:  tc.Servers[0].GetNode().Descriptor.NodeID,
			StoreID: tc.Servers[0].GetFirstStoreID(),
		})
	if err != nil {
		t.Fatal(err)
	}
	if leaseHolder.StoreID != tc.Servers[1].GetFirstStoreID() {
		t.Fatalf("expected lease on server idx 1 (node: %d store: %d), but is on node: %+v",
			tc.Servers[1].GetNode().Descriptor.NodeID,
			tc.Servers[1].GetFirstStoreID(),
			leaseHolder)
	}
}

// A basic test of manual replication that used to fail because we weren't
// waiting for all of the stores to initialize.
func TestBasicManualReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3, base.TestClusterArgs{ReplicationMode: base.ReplicationManual})
	defer tc.Stopper().Stop(context.TODO())

	desc, err := tc.AddReplicas(keys.MinKey, tc.Target(1), tc.Target(2))
	if err != nil {
		t.Fatal(err)
	}
	if expected := 3; expected != len(desc.InternalReplicas) {
		t.Fatalf("expected %d replicas, got %+v", expected, desc.InternalReplicas)
	}

	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}

	// NB: Removing the leaseholder (tc.Target(1)) causes the test to take ~11s
	// vs ~1.5s for removing a non-leaseholder. This is due to needing to wait
	// for the lease to timeout which takes ~9s. Testing leaseholder removal is
	// not necessary because internal rebalancing avoids ever removing the
	// leaseholder for the exact reason that it causes performance hiccups.
	desc, err = tc.RemoveReplicas(desc.StartKey.AsRawKey(), tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	if expected := 2; expected != len(desc.InternalReplicas) {
		t.Fatalf("expected %d replicas, got %+v", expected, desc.InternalReplicas)
	}
}

func TestBasicAutoReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := StartTestCluster(t, 3, base.TestClusterArgs{ReplicationMode: base.ReplicationAuto})
	defer tc.Stopper().Stop(context.TODO())
	// NB: StartTestCluster will wait for full replication.
}

func TestStopServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use insecure mode so our servers listen on util.IsolatedTestAddr
	// and they fail cleanly instead of interfering with other tests.
	// See https://gitee.com/kwbasedb/kwbase/issues/9256
	tc := StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(context.TODO())

	// Connect to server 1, ensure it is answering requests over HTTP and GRPC.
	server1 := tc.Server(1)
	var response serverpb.JSONResponse

	httpClient1, err := server1.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url := server1.AdminURL() + "/_status/metrics/local"
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}

	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: tc.Server(0).ClusterSettings().Tracer},
		tc.Server(1).RPCContext().Config, tc.Server(1).Clock(), tc.Stopper(),
		tc.Server(1).ClusterSettings(),
	)
	conn, err := rpcContext.GRPCDialNode(server1.ServingRPCAddr(), server1.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	statusClient1 := serverpb.NewStatusClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := statusClient1.Metrics(ctx, &serverpb.MetricsRequest{NodeId: "local"}); err != nil {
		t.Fatal(err)
	}

	// Stop server 1.
	tc.StopServer(1)

	// Verify HTTP and GRPC requests to server now fail.
	//
	// On *nix, this error is:
	//
	// dial tcp 127.0.0.1:65054: getsockopt: connection refused
	//
	// On Windows, this error is:
	//
	// dial tcp 127.0.0.1:59951: connectex: No connection could be made because the target machine actively refused it.
	//
	// So we look for the common bit.
	httpErrorText := `dial tcp .*: .* refused`
	if err := httputil.GetJSON(httpClient1, url, &response); err == nil {
		t.Fatal("Expected HTTP Request to fail after server stopped")
	} else if !testutils.IsError(err, httpErrorText) {
		t.Fatalf("Expected error from server with text %q, got error with text %q", httpErrorText, err.Error())
	}

	grpcErrorText := "rpc error"
	if _, err := statusClient1.Metrics(ctx, &serverpb.MetricsRequest{NodeId: "local"}); err == nil {
		t.Fatal("Expected GRPC Request to fail after server stopped")
	} else if !testutils.IsError(err, grpcErrorText) {
		t.Fatalf("Expected error from GRPC with text %q, got error with text %q", grpcErrorText, err.Error())
	}

	// Verify that request to Server 0 still works.
	httpClient1, err = tc.Server(0).GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	url = tc.Server(0).AdminURL() + "/_status/metrics/local"
	if err := httputil.GetJSON(httpClient1, url, &response); err != nil {
		t.Fatal(err)
	}
}

func TestExpirationBasedLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(ctx)

	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	key := tc.ScratchRangeWithExpirationLease(t)
	repl := store.LookupReplica(roachpb.RKey(key))
	lease, _ := repl.GetLease()
	require.NotNil(t, lease.Expiration)

	// Verify idempotence of ScratchRangeWithExpirationLease
	keyAgain := tc.ScratchRangeWithExpirationLease(t)
	replAgain := store.LookupReplica(roachpb.RKey(keyAgain))
	require.Equal(t, repl, replAgain)
}
