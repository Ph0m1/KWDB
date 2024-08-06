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

func runClockMonotonicity(ctx context.Context, t *test, c *cluster, tc clockMonotonicityTestCase) {
	// Test with a single node so that the node does not crash due to MaxOffset
	// violation when introducing offset
	if c.spec.NodeCount != 1 {
		t.Fatalf("Expected num nodes to be 1, got: %d", c.spec.NodeCount)
	}

	t.Status("deploying offset injector")
	offsetInjector := newOffsetInjector(c)
	if err := offsetInjector.deploy(ctx); err != nil {
		t.Fatal(err)
	}

	if err := c.RunE(ctx, c.Node(1), "test -x ./kwbase"); err != nil {
		c.Put(ctx, kwbase, "./kwbase", c.All())
	}
	c.Wipe(ctx)
	c.Start(ctx, t)

	db := c.Conn(ctx, c.spec.NodeCount)
	defer db.Close()
	if _, err := db.Exec(
		fmt.Sprintf(`SET CLUSTER SETTING server.clock.persist_upper_bound_interval = '%v'`,
			tc.persistWallTimeInterval)); err != nil {
		t.Fatal(err)
	}

	// Wait for Cockroach to process the above cluster setting
	time.Sleep(10 * time.Second)

	if !isAlive(db, c.l) {
		t.Fatal("Node unexpectedly crashed")
	}

	preRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	// Recover from the injected clock offset after validation completes.
	defer func() {
		if !isAlive(db, c.l) {
			t.Fatal("Node unexpectedly crashed")
		}
		// Stop kwbase node before recovering from clock offset as this clock
		// jump can crash the node.
		c.Stop(ctx, c.Node(c.spec.NodeCount))
		t.l.Printf("recovering from injected clock offset")

		offsetInjector.recover(ctx, c.spec.NodeCount)

		c.Start(ctx, t, c.Node(c.spec.NodeCount))
		if !isAlive(db, c.l) {
			t.Fatal("Node unexpectedly crashed")
		}
	}()

	// Inject a clock offset after stopping a node
	t.Status("stopping kwbase")
	c.Stop(ctx, c.Node(c.spec.NodeCount))
	t.Status("injecting offset")
	offsetInjector.offset(ctx, c.spec.NodeCount, tc.offset)
	t.Status("starting kwbase post offset")
	c.Start(ctx, t, c.Node(c.spec.NodeCount))

	if !isAlive(db, c.l) {
		t.Fatal("Node unexpectedly crashed")
	}

	postRestartTime, err := dbUnixEpoch(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Status("validating clock monotonicity")
	t.l.Printf("pre-restart time:  %f\n", preRestartTime)
	t.l.Printf("post-restart time: %f\n", postRestartTime)
	difference := postRestartTime - preRestartTime
	t.l.Printf("time-difference: %v\n", time.Duration(difference*float64(time.Second)))

	if tc.expectIncreasingWallTime {
		if preRestartTime > postRestartTime {
			t.Fatalf("Expected pre-restart time %f < post-restart time %f", preRestartTime, postRestartTime)
		}
	} else {
		if preRestartTime < postRestartTime {
			t.Fatalf("Expected pre-restart time %f > post-restart time %f", preRestartTime, postRestartTime)
		}
	}
}

type clockMonotonicityTestCase struct {
	name                     string
	persistWallTimeInterval  time.Duration
	offset                   time.Duration
	expectIncreasingWallTime bool
}

func registerClockMonotonicTests(r *testRegistry) {
	testCases := []clockMonotonicityTestCase{
		{
			name:                     "persistent",
			offset:                   -60 * time.Second,
			persistWallTimeInterval:  500 * time.Millisecond,
			expectIncreasingWallTime: true,
		},
	}

	for i := range testCases {
		tc := testCases[i]
		spec := testSpec{
			Name:  "clock/monotonic/" + tc.name,
			Owner: OwnerKV,
			// These tests muck with NTP, therefor we don't want the cluster reused by
			// others.
			Cluster: makeClusterSpec(1, reuseTagged("offset-injector")),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runClockMonotonicity(ctx, t, c, tc)
			},
		}
		r.Add(spec)
	}
}
