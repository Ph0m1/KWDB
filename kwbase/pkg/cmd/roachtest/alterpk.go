// Copyright 2020 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func registerAlterPK(r *testRegistry) {

	setupTest := func(ctx context.Context, t *test, c *cluster) (nodeListOption, nodeListOption) {
		roachNodes := c.Range(1, c.spec.NodeCount-1)
		loadNode := c.Node(c.spec.NodeCount)
		t.Status("copying binaries")
		c.Put(ctx, kwbase, "./kwbase", roachNodes)
		c.Put(ctx, workload, "./workload", loadNode)

		t.Status("starting kwbase nodes")
		c.Start(ctx, t, roachNodes)
		return roachNodes, loadNode
	}

	// runAlterPKBank runs a primary key change while the bank workload runs.
	runAlterPKBank := func(ctx context.Context, t *test, c *cluster) {
		const numRows = 1000000
		const duration = 1 * time.Minute

		roachNodes, loadNode := setupTest(ctx, t, c)

		initDone := make(chan struct{}, 1)
		pkChangeDone := make(chan struct{}, 1)

		m := newMonitor(ctx, c, roachNodes)
		m.Go(func(ctx context.Context) error {
			// Load up a relatively small dataset to perform a workload on.

			// Init the workload.
			cmd := fmt.Sprintf("./workload init bank --drop --rows %d {pgurl%s}", numRows, roachNodes)
			if err := c.RunE(ctx, loadNode, cmd); err != nil {
				t.Fatal(err)
			}
			initDone <- struct{}{}

			// Run the workload while the primary key change is happening.
			cmd = fmt.Sprintf("./workload run bank --duration=%s {pgurl%s}", duration, roachNodes)
			c.Run(ctx, loadNode, cmd)
			// Wait for the primary key change to finish.
			<-pkChangeDone
			t.Status("starting second run of the workload after primary key change")
			// Run the workload after the primary key change occurs.
			c.Run(ctx, loadNode, cmd)
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Wait for the initialization to finish. Once it's done,
			// sleep for some time, then alter the primary key.
			<-initDone
			time.Sleep(duration / 10)

			t.Status("beginning primary key change")
			db := c.Conn(ctx, roachNodes[0])
			defer db.Close()
			cmd := `
			USE bank;
			ALTER TABLE bank ALTER COLUMN balance SET NOT NULL;
			ALTER TABLE bank ALTER PRIMARY KEY USING COLUMNS (id, balance)
			`
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				t.Fatal(err)
			}
			t.Status("primary key change finished")
			pkChangeDone <- struct{}{}
			return nil
		})
		m.Wait()
	}

	// runAlterPKTPCC runs a primary key change while the TPCC workload runs.
	runAlterPKTPCC := func(ctx context.Context, t *test, c *cluster, warehouses int, expensiveChecks bool) {
		const duration = 10 * time.Minute

		roachNodes, loadNode := setupTest(ctx, t, c)

		cmd := fmt.Sprintf(
			"./kwbase workload fixtures import tpcc --warehouses=%d --db=tpcc",
			warehouses,
		)
		if err := c.RunE(ctx, c.Node(roachNodes[0]), cmd); err != nil {
			t.Fatal(err)
		}

		m := newMonitor(ctx, c, roachNodes)
		m.Go(func(ctx context.Context) error {
			// Start running the workload.
			runCmd := fmt.Sprintf(
				"./workload run tpcc --warehouses=%d --split --scatter --duration=%s {pgurl%s}",
				warehouses,
				duration,
				roachNodes,
			)
			t.Status("beginning workload")
			c.Run(ctx, loadNode, runCmd)
			t.Status("finished running workload")
			return nil
		})
		m.Go(func(ctx context.Context) error {
			// Start a primary key change after some delay.
			time.Sleep(duration / 10)

			// Pick a random table to change the primary key of.
			alterStmts := []string{
				`ALTER TABLE warehouse ALTER PRIMARY KEY USING COLUMNS (w_id)`,
				`ALTER TABLE district ALTER PRIMARY KEY USING COLUMNS (d_w_id, d_id)`,
				`ALTER TABLE history ALTER PRIMARY KEY USING COLUMNS (h_w_id, rowid)`,
				`ALTER TABLE customer ALTER PRIMARY KEY USING COLUMNS (c_w_id, c_d_id, c_id)`,
				`ALTER TABLE "order" ALTER PRIMARY KEY USING COLUMNS (o_w_id, o_d_id, o_id DESC)`,
				`ALTER TABLE new_order ALTER PRIMARY KEY USING COLUMNS (no_w_id, no_d_id, no_o_id)`,
				`ALTER TABLE item ALTER PRIMARY KEY USING COLUMNS (i_id)`,
				`ALTER TABLE stock ALTER PRIMARY KEY USING COLUMNS (s_w_id, s_i_id)`,
				`ALTER TABLE order_line ALTER PRIMARY KEY USING COLUMNS (ol_w_id, ol_d_id, ol_o_id DESC, ol_number)`,
			}

			rand, _ := randutil.NewPseudoRand()
			randStmt := alterStmts[rand.Intn(len(alterStmts))]
			t.Status("Running command: ", randStmt)

			db := c.Conn(ctx, roachNodes[0])
			defer db.Close()
			alterCmd := `USE tpcc; %s;`
			t.Status("beginning primary key change")
			if _, err := db.ExecContext(ctx, fmt.Sprintf(alterCmd, randStmt)); err != nil {
				t.Fatal(err)
			}
			t.Status("primary key change finished")
			return nil
		})

		m.Wait()

		// Run the verification checks of the TPCC workload post primary key change.
		expensiveChecksArg := ""
		if expensiveChecks {
			expensiveChecksArg = "--expensive-checks"
		}
		checkCmd := fmt.Sprintf(
			"./workload check tpcc --warehouses %d %s {pgurl%s}",
			warehouses,
			expensiveChecksArg,
			c.Node(roachNodes[0]),
		)
		t.Status("beginning database verification")
		c.Run(ctx, loadNode, checkCmd)
		t.Status("finished database verification")
	}
	r.Add(testSpec{
		Name:  "alterpk-bank",
		Owner: OwnerSQLSchema,
		// Use a 4 node cluster -- 3 nodes will run kwbase, and the last will be the
		// workload driver node.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run:        runAlterPKBank,
	})
	r.Add(testSpec{
		Name:  "alterpk-tpcc-250",
		Owner: OwnerSQLSchema,
		// Use a 4 node cluster -- 3 nodes will run kwbase, and the last will be the
		// workload driver node.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4, cpu(32)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAlterPKTPCC(ctx, t, c, 250 /* warehouses */, true /* expensiveChecks */)
		},
	})
	r.Add(testSpec{
		Name:  "alterpk-tpcc-500",
		Owner: OwnerSQLSchema,
		// Use a 4 node cluster -- 3 nodes will run kwbase, and the last will be the
		// workload driver node.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4, cpu(16)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runAlterPKTPCC(ctx, t, c, 500 /* warehouses */, false /* expensiveChecks */)
		},
	})
}
