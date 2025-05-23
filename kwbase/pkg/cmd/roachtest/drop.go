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
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	_ "github.com/lib/pq"
)

func registerDrop(r *testRegistry) {
	// TODO(tschottdorf): rearrange all tests so that their synopses are available
	// via godoc and (some variation on) `roachtest run <testname> --help`.

	// This test imports a TPCC dataset and then issues a manual deletion followed
	// by a truncation for the `stock` table (which contains warehouses*100k
	// rows). Next, it issues a `DROP` for the whole database, and sets the GC TTL
	// to one second.
	runDrop := func(ctx context.Context, t *test, c *cluster, warehouses, nodes int) {
		c.Put(ctx, kwbase, "./kwbase", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		c.Start(ctx, t, c.Range(1, nodes), startArgs("-e", "KWBASE_MEMPROF_INTERVAL=15s"))

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			t.WorkerStatus("importing TPCC fixture")
			c.Run(ctx, c.Node(1), tpccImportCmd(warehouses))

			// Don't open the DB connection until after the data has been imported.
			// Otherwise the ALTER TABLE query below might fail to find the
			// tpcc.order_line table that we just imported (!) due to what seems to
			// be a problem with table descriptor leases (#24374).
			db := c.Conn(ctx, 1)
			defer db.Close()

			run := func(maybeExperimental bool, stmtStr string, args ...interface{}) {
				stmt := stmtStr
				// We are removing the EXPERIMENTAL keyword in 2.1. For compatibility
				// with 2.0 clusters we still need to try with it if the
				// syntax without EXPERIMENTAL fails.
				// TODO(knz): Remove this in 2.2.
				if maybeExperimental {
					stmt = fmt.Sprintf(stmtStr, "", "=")
				}
				t.WorkerStatus(stmt)
				_, err := db.ExecContext(ctx, stmt, args...)
				if err != nil && maybeExperimental && strings.Contains(err.Error(), "syntax error") {
					stmt = fmt.Sprintf(stmtStr, "EXPERIMENTAL", "")
					t.WorkerStatus(stmt)
					_, err = db.ExecContext(ctx, stmt, args...)
				}
				if err != nil {
					t.Fatal(err)
				}
			}

			run(false, `SET CLUSTER SETTING trace.debug.enable = true`)

			// Drop a constraint that would get in the way of deleting from tpcc.stock.
			const stmtDropConstraint = "ALTER TABLE tpcc.order_line DROP CONSTRAINT fk_ol_supply_w_id_ref_stock"
			run(false, stmtDropConstraint)

			var rows, minWarehouse, maxWarehouse int
			if err := db.QueryRow("select count(*), min(s_w_id), max(s_w_id) from tpcc.stock").Scan(&rows,
				&minWarehouse, &maxWarehouse); err != nil {
				t.Fatalf("failed to get range count: %v", err)
			}

			for j := 1; j <= nodes; j++ {
				size, err := getDiskUsageInBytes(ctx, c, t.l, j)
				if err != nil {
					return err
				}

				t.l.Printf("Node %d space used: %s\n", j, humanizeutil.IBytes(int64(size)))
			}

			for i := minWarehouse; i <= maxWarehouse; i++ {
				t.Progress(float64(i) / float64(maxWarehouse))
				tBegin := timeutil.Now()
				run(false, "DELETE FROM tpcc.stock WHERE s_w_id = $1", i)
				elapsed := timeutil.Since(tBegin)
				// TODO(tschottdorf): check what's reasonable here and make sure we don't drop below it.
				c.l.Printf("deleted from tpcc.stock for warehouse %d (100k rows) in %s (%.2f rows/sec)\n", i, elapsed, 100000.0/elapsed.Seconds())
			}

			const stmtTruncate = "TRUNCATE TABLE tpcc.stock"
			run(false, stmtTruncate)

			const stmtDrop = "DROP DATABASE tpcc"
			run(false, stmtDrop)
			// The data has already been deleted, but changing the default zone config
			// should take effect retroactively.
			run(true, "ALTER RANGE default %[1]s CONFIGURE ZONE %[2]s '\ngc:\n  ttlseconds: 1\n'")

			var allNodesSpaceCleared bool
			var sizeReport string
			maxSizeBytes := 100 * 1024 * 1024
			if true {
				// TODO(tschottdorf): This test should pass without this large fudge factor. This requires manual reproduction
				// and an investigation of the compactor logs as well as the data directory.
				maxSizeBytes *= 100
			}
			// We're waiting a maximum of 10 minutes to makes sure that the drop operations clear the disk.
			for i := 0; i < 10; i++ {
				sizeReport = ""
				allNodesSpaceCleared = true
				for j := 1; j <= nodes; j++ {
					size, err := getDiskUsageInBytes(ctx, c, t.l, j)
					if err != nil {
						return err
					}

					nodeSpaceUsed := fmt.Sprintf("Node %d space after deletion used: %s\n", j, humanizeutil.IBytes(int64(size)))
					t.l.Printf(nodeSpaceUsed)

					// Return if the size of the directory is less than 100mb
					if size > maxSizeBytes {
						allNodesSpaceCleared = false
						sizeReport += nodeSpaceUsed
					}
				}

				if allNodesSpaceCleared {
					break
				}
				time.Sleep(time.Minute)
			}

			if !allNodesSpaceCleared {
				sizeReport += fmt.Sprintf("disk space usage has not dropped below %s on all nodes.",
					humanizeutil.IBytes(int64(maxSizeBytes)))
				t.Fatalf(sizeReport)
			}

			return nil
		})
		m.Wait()
	}

	warehouses := 100
	numNodes := 9

	r.Add(testSpec{
		Name:       fmt.Sprintf("drop/tpcc/w=%d,nodes=%d", warehouses, numNodes),
		Owner:      OwnerKV,
		MinVersion: `v2.1.0`,
		Cluster:    makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			// NB: this is likely not going to work out in `-local` mode. Edit the
			// numbers during iteration.
			if local {
				numNodes = 4
				warehouses = 1
				fmt.Printf("running with w=%d,nodes=%d in local mode\n", warehouses, numNodes)
			}
			runDrop(ctx, t, c, warehouses, numNodes)
		},
	})
}
