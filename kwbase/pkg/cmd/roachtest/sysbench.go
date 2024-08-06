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
)

type sysbenchWorkload int

const (
	oltpDelete sysbenchWorkload = iota
	oltpInsert
	oltpPointSelect
	oltpUpdateIndex
	oltpUpdateNonIndex
	oltpReadOnly
	oltpReadWrite
	oltpWriteOnly

	numSysbenchWorkloads
)

var sysbenchWorkloadName = map[sysbenchWorkload]string{
	oltpDelete:         "oltp_delete",
	oltpInsert:         "oltp_insert",
	oltpPointSelect:    "oltp_point_select",
	oltpUpdateIndex:    "oltp_update_index",
	oltpUpdateNonIndex: "oltp_update_non_index",
	oltpReadOnly:       "oltp_read_only",
	oltpReadWrite:      "oltp_read_write",
	oltpWriteOnly:      "oltp_write_only",
}

func (w sysbenchWorkload) String() string {
	return sysbenchWorkloadName[w]
}

type sysbenchOptions struct {
	workload     sysbenchWorkload
	duration     time.Duration
	concurrency  int
	tables       int
	rowsPerTable int
}

func (o *sysbenchOptions) cmd(haproxy bool) string {
	pghost := "{pghost:1}"
	if haproxy {
		pghost = "127.0.0.1"
	}
	return fmt.Sprintf(`sysbench \
		--db-driver=pgsql \
		--pgsql-host=%s \
		--pgsql-port=26257 \
		--pgsql-user=root \
		--pgsql-password= \
		--pgsql-db=sysbench \
		--report-interval=1 \
		--time=%d \
		--threads=%d \
		--tables=%d \
		--table_size=%d \
		--auto_inc=false \
		%s`,
		pghost,
		int(o.duration.Seconds()),
		o.concurrency,
		o.tables,
		o.rowsPerTable,
		o.workload,
	)
}

func runSysbench(ctx context.Context, t *test, c *cluster, opts sysbenchOptions) {
	allNodes := c.Range(1, c.spec.NodeCount)
	roachNodes := c.Range(1, c.spec.NodeCount-1)
	loadNode := c.Node(c.spec.NodeCount)

	t.Status("installing kwbase")
	c.Put(ctx, kwbase, "./kwbase", allNodes)
	c.Start(ctx, t, roachNodes)
	waitForFullReplication(t, c.Conn(ctx, allNodes[0]))

	t.Status("installing haproxy")
	if err := c.Install(ctx, t.l, loadNode, "haproxy"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, loadNode, "./kwbase gen haproxy --insecure --url {pgurl:1}")
	c.Run(ctx, loadNode, "haproxy -f haproxy.cfg -D")

	t.Status("installing sysbench")
	if err := c.Install(ctx, t.l, loadNode, "sysbench"); err != nil {
		t.Fatal(err)
	}

	m := newMonitor(ctx, c, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("preparing workload")
		c.Run(ctx, c.Node(1), `./kwbase sql --insecure -e "CREATE DATABASE sysbench"`)
		c.Run(ctx, loadNode, opts.cmd(false /* haproxy */)+" prepare")

		t.Status("running workload")
		err := c.RunE(ctx, loadNode, opts.cmd(true /* haproxy */)+" run")
		// Sysbench occasionally segfaults. When that happens, don't fail the
		// test.
		if err != nil && !strings.Contains(err.Error(), "Segmentation fault") {
			c.l.Printf("sysbench segfaulted; passing test anyway")
			return err
		}
		return nil
	})
	m.Wait()
}

func registerSysbench(r *testRegistry) {
	for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
		const n = 3
		const cpus = 32
		const conc = 4 * cpus
		opts := sysbenchOptions{
			workload:     w,
			duration:     10 * time.Minute,
			concurrency:  conc,
			tables:       10,
			rowsPerTable: 10000000,
		}

		r.Add(testSpec{
			Name:    fmt.Sprintf("sysbench/%s/nodes=%d/cpu=%d/conc=%d", w, n, cpus, conc),
			Owner:   OwnerKV,
			Cluster: makeClusterSpec(n+1, cpu(cpus)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runSysbench(ctx, t, c, opts)
			},
		})
	}
}
