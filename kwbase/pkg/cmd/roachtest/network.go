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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/httputil"
	toxiproxy "github.com/Shopify/toxiproxy/client"
	_ "github.com/lib/pq"
)

// runNetworkSanity is just a sanity check to make sure we're setting up toxiproxy
// correctly. It injects latency between the nodes and verifies that we're not
// seeing the latency on the client connection running `SELECT 1` on each node.
func runNetworkSanity(ctx context.Context, t *test, origC *cluster, nodes int) {
	origC.Put(ctx, kwbase, "./kwbase", origC.All())
	c, err := Toxify(ctx, origC, origC.All())
	if err != nil {
		t.Fatal(err)
	}

	c.Start(ctx, t, c.All())

	db := c.Conn(ctx, 1) // unaffected by toxiproxy
	defer db.Close()
	waitForFullReplication(t, db)

	// NB: we're generous with latency in this test because we're checking that
	// the upstream connections aren't affected by latency below, but the fixed
	// cost of starting the binary and processing the query is already close to
	// 100ms.
	const latency = 300 * time.Millisecond
	for i := 1; i <= nodes; i++ {
		// NB: note that these latencies only apply to connections *to* the node
		// on which the toxic is active. That is, if n1 has a (down or upstream)
		// latency toxic of 100ms, then none of its outbound connections are
		// affected but any connections made to it by other nodes will.
		// In particular, it's difficult to simulate intricate network partitions
		// as there's no way to activate toxics only for certain peers.
		proxy := c.Proxy(i)
		if _, err := proxy.AddToxic("", "latency", "downstream", 1, toxiproxy.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := proxy.AddToxic("", "latency", "upstream", 1, toxiproxy.Attributes{
			"latency": latency / (2 * time.Millisecond), // ms
		}); err != nil {
			t.Fatal(err)
		}
	}

	m := newMonitor(ctx, c.cluster, c.All())
	m.Go(func(ctx context.Context) error {
		c.Measure(ctx, 1, `SET CLUSTER SETTING trace.debug.enable = true`)
		c.Measure(ctx, 1, "CREATE DATABASE test")
		c.Measure(ctx, 1, `CREATE TABLE test.commit (a INT, b INT, v INT, PRIMARY KEY (a, b))`)

		for i := 0; i < 10; i++ {
			duration := c.Measure(ctx, 1, fmt.Sprintf(
				"BEGIN; INSERT INTO test.commit VALUES (2, %[1]d), (1, %[1]d), (3, %[1]d); COMMIT",
				i,
			))
			c.l.Printf("%s\n", duration)
		}

		c.Measure(ctx, 1, `
set tracing=on;
insert into test.commit values(3,1000), (1,1000), (2,1000);
select age, message from [ show trace for session ];
`)

		for i := 1; i <= origC.spec.NodeCount; i++ {
			if dur := c.Measure(ctx, i, `SELECT 1`); dur > latency {
				t.Fatalf("node %d unexpectedly affected by latency: select 1 took %.2fs", i, dur.Seconds())
			}
		}

		return nil
	})

	m.Wait()
}

func runNetworkTPCC(ctx context.Context, t *test, origC *cluster, nodes int) {
	n := origC.spec.NodeCount
	serverNodes, workerNode := origC.Range(1, n-1), origC.Node(n)
	origC.Put(ctx, kwbase, "./kwbase", origC.All())
	origC.Put(ctx, workload, "./workload", origC.All())

	c, err := Toxify(ctx, origC, serverNodes)
	if err != nil {
		t.Fatal(err)
	}

	const warehouses = 1
	c.Start(ctx, t, serverNodes)
	c.Run(ctx, c.Node(1), tpccImportCmd(warehouses))

	db := c.Conn(ctx, 1)
	defer db.Close()
	waitForFullReplication(t, db)

	duration := time.Hour
	if local {
		// NB: this is really just testing the test with this duration, it won't
		// be able to detect slow goroutine leaks.
		duration = 5 * time.Minute
	}

	// Run TPCC, but don't give it the first node (or it basically won't do anything).
	m := newMonitor(ctx, c.cluster, serverNodes)

	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")

		cmd := fmt.Sprintf(
			"./workload run tpcc --warehouses=%d --wait=false"+
				" --histograms="+perfArtifactsDir+"/stats.json"+
				" --duration=%s {pgurl:2-%d}",
			warehouses, duration, c.spec.NodeCount-1)
		return c.RunE(ctx, workerNode, cmd)
	})

	checkGoroutines := func(ctx context.Context) int {
		// NB: at the time of writing, the goroutine count would quickly
		// stabilize near 230 when the network is partitioned, and around 270
		// when it isn't. Experimentally a past "slow" goroutine leak leaked ~3
		// goroutines every minute (though it would likely be more with the tpcc
		// workload above), which over the duration of an hour would easily push
		// us over the threshold.
		const thresh = 350

		uiAddrs := c.ExternalAdminUIAddr(ctx, serverNodes)
		var maxSeen int
		// The goroutine dump may take a while to generate, maybe more
		// than the 3 second timeout of the default http client.
		httpClient := httputil.NewClientWithTimeout(15 * time.Second)
		for _, addr := range uiAddrs {
			url := "http://" + addr + "/debug/pprof/goroutine?debug=2"
			resp, err := httpClient.Get(ctx, url)
			if err != nil {
				t.Fatal(err)
			}
			content, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			numGoroutines := bytes.Count(content, []byte("goroutine "))
			if numGoroutines >= thresh {
				t.Fatalf("%s shows %d goroutines (expected <%d)", url, numGoroutines, thresh)
			}
			if maxSeen < numGoroutines {
				maxSeen = numGoroutines
			}
		}
		return maxSeen
	}

	m.Go(func(ctx context.Context) error {
		time.Sleep(10 * time.Second) // give tpcc a head start
		// Give n1 a network partition from the remainder of the cluster. Note that even though it affects
		// both the "upstream" and "downstream" directions, this is in fact an asymmetric partition since
		// it only affects connections *to* the node. n1 itself can connect to the cluster just fine.
		proxy := c.Proxy(1)
		c.l.Printf("letting inbound traffic to first node time out")
		for _, direction := range []string{"upstream", "downstream"} {
			if _, err := proxy.AddToxic("", "timeout", direction, 1, toxiproxy.Attributes{
				"timeout": 0, // forever
			}); err != nil {
				t.Fatal(err)
			}
		}

		t.WorkerStatus("checking goroutines")
		done := time.After(duration)
		var maxSeen int
		for {
			cur := checkGoroutines(ctx)
			if maxSeen < cur {
				c.l.Printf("new goroutine peak: %d", cur)
				maxSeen = cur
			}

			select {
			case <-done:
				c.l.Printf("done checking goroutines, repairing network")
				// Repair the network. Note that the TPCC workload would never
				// finish (despite the duration) without this. In particular,
				// we don't want to m.Wait() before we do this.
				toxics, err := proxy.Toxics()
				if err != nil {
					t.Fatal(err)
				}
				for _, toxic := range toxics {
					if err := proxy.RemoveToxic(toxic.Name); err != nil {
						t.Fatal(err)
					}
				}
				c.l.Printf("network is repaired")

				// Verify that goroutine count doesn't spike.
				for i := 0; i < 20; i++ {
					nowGoroutines := checkGoroutines(ctx)
					c.l.Printf("currently at most %d goroutines per node", nowGoroutines)
					time.Sleep(time.Second)
				}

				return nil
			default:
				time.Sleep(3 * time.Second)
			}
		}
	})

	m.Wait()
}

func registerNetwork(r *testRegistry) {
	const numNodes = 4

	r.Add(testSpec{
		Name:    fmt.Sprintf("network/sanity/nodes=%d", numNodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runNetworkSanity(ctx, t, c, numNodes)
		},
	})
	r.Add(testSpec{
		Name:    fmt.Sprintf("network/tpcc/nodes=%d", numNodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(numNodes),
		Skip:    "https://gitee.com/kwbasedb/kwbase/issues/49901#issuecomment-640666646",
		SkipDetails: `The ordering of steps in the test is:

- install toxiproxy
- start cluster, wait for up-replication
- launch the goroutine that starts the tpcc client command, but do not wait on
it starting
- immediately, cause a network partition
- only then, the goroutine meant to start the tpcc client goes to fetch the
pg URLs and start workload, but of course this fails because network
partition
- tpcc fails to start, so the test tears down before it resolves the network partition
- test tear-down and debug zip fail because the network partition is still active

There are two problems here:

the tpcc client is not actually started yet when the test sets up the
network partition. This is a race condition. there should be a defer in
there to resolve the partition when the test aborts prematurely. (And the
command to resolve the partition should not be sensitive to the test
context's Done() channel, because during a tear-down that is closed already)
`,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runNetworkTPCC(ctx, t, c, numNodes)
		},
	})
}
