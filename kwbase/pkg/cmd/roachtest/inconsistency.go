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

package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func registerInconsistency(r *testRegistry) {
	r.Add(testSpec{
		Name:       fmt.Sprintf("inconsistency"),
		Owner:      OwnerKV,
		Skip:       "Uses RocksDB put command; unskip when that's bypassed",
		MinVersion: "v19.2.2", // https://gitee.com/kwbasedb/kwbase/pull/42149 is new in 19.2.2
		Cluster:    makeClusterSpec(3),
		Run:        runInconsistency,
	})
}

func runInconsistency(ctx context.Context, t *test, c *cluster) {
	// With encryption on, our attempt below to manually introduce an inconsistency
	// will fail.
	c.encryptDefault = false

	nodes := c.Range(1, 3)
	c.Put(ctx, kwbase, "./kwbase", nodes)
	c.Start(ctx, t, nodes)

	{
		db := c.Conn(ctx, 1)
		// Disable consistency checks. We're going to be introducing an inconsistency and wish for it to be detected when
		// we've set up the test to expect it.
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '0'`)
		if err != nil {
			t.Fatal(err)
		}
		waitForFullReplication(t, db)
		_, db = db.Close(), nil
	}

	c.Stop(ctx, nodes)

	// KV pair created via:
	//
	// t.Errorf("0x%x", EncodeKey(MVCCKey{
	// 	Key: keys.TransactionKey(keys.LocalMax, uuid.Nil),
	// }))
	// for i := 0; i < 3; i++ {
	// 	var m enginepb.MVCCMetadata
	// 	var txn enginepb.TxnMeta
	// 	txn.Key = []byte(fmt.Sprintf("fake transaction %d", i))
	// 	var err error
	// 	m.RawBytes, err = protoutil.Marshal(&txn)
	// 	require.NoError(t, err)
	// 	data, err := protoutil.Marshal(&m)
	// 	require.NoError(t, err)
	// 	t.Error(fmt.Sprintf("0x%x", data))
	// }
	//
	// Output:
	// 0x016b1202000174786e2d0000000000000000000000000000000000
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20302a004a00
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20312a004a00
	// 0x120408001000180020002800322a0a10000000000000000000000000000000001a1266616b65207472616e73616374696f6e20322a004a00

	c.Run(ctx, c.Node(1), "./kwbase debug rocksdb put --hex --db={store-dir} "+
		"0x016b1202000174786e2d0000000000000000000000000000000000 "+
		"0x12040800100018002000280032280a10000000000000000000000000000000001a1066616b65207472616e73616374696f6e2a004a00")

	m := newMonitor(ctx, c)
	c.Start(ctx, t, nodes)
	m.Go(func(ctx context.Context) error {
		select {
		case <-time.After(5 * time.Minute):
		case <-ctx.Done():
		}
		return nil
	})

	time.Sleep(10 * time.Second) // wait for n1-n3 to all be known as live to each other

	// set an aggressive consistency check interval, but only now (that we're
	// reasonably sure all nodes are live, etc). This makes sure that the consistency
	// check runs against all three nodes. If it targeted only two nodes, a random
	// one would fatal - not what we want.
	{
		db := c.Conn(ctx, 2)
		_, err := db.ExecContext(ctx, `SET CLUSTER SETTING server.consistency_check.interval = '10ms'`)
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}

	if err := m.WaitE(); err == nil {
		t.Fatal("expected a node to crash")
	}

	time.Sleep(20 * time.Second) // wait for liveness to time out for dead nodes

	db := c.Conn(ctx, 2)
	rows, err := db.Query(`SELECT node_id FROM kwdb_internal.gossip_nodes WHERE is_live = false;`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected one dead NodeID, got %v", ids)
	}
	const expr = "this.node.is.terminating.because.a.replica.inconsistency.was.detected"
	c.Run(ctx, c.Node(1), "grep "+
		expr+" "+"{log-dir}/kwbase.log")

	if err := c.StartE(ctx, c.Node(1)); err == nil {
		// NB: we can't easily verify the error because there's a lot of output
		// which isn't fully included in the error returned from StartE.
		t.Fatalf("node restart should have failed")
	}

	// roachtest checks that no nodes are down when the test finishes, but in this
	// case we have a down node that we can't restart. Remove the data dir, which
	// tells roachtest to ignore this node.
	c.Wipe(ctx, c.Node(1))
}
