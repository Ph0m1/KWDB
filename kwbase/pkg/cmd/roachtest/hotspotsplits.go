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

	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

func registerHotSpotSplits(r *testRegistry) {
	// This test sets up a cluster and runs kv on it with high concurrency and a large block size
	// to force a large range. We then make sure that the largest range isn't larger than a threshold and
	// that backpressure is working correctly.
	runHotSpot := func(ctx context.Context, t *test, c *cluster, duration time.Duration, concurrency int) {
		roachNodes := c.Range(1, c.spec.NodeCount-1)
		appNode := c.Node(c.spec.NodeCount)

		c.Put(ctx, kwbase, "./kwbase", roachNodes)
		c.Start(ctx, t, roachNodes)

		c.Put(ctx, workload, "./workload", appNode)
		c.Run(ctx, appNode, `./workload init kv --drop {pgurl:1}`)

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)

		m.Go(func() error {
			t.l.Printf("starting load generator\n")

			const blockSize = 1 << 18 // 256 KB
			return c.RunE(ctx, appNode, fmt.Sprintf(
				"./workload run kv --read-percent=0 --tolerate-errors --concurrency=%d "+
					"--min-block-bytes=%d --max-block-bytes=%d --duration=%s {pgurl:1-3}",
				concurrency, blockSize, blockSize, duration.String()))
		})

		m.Go(func() error {
			t.Status(fmt.Sprintf("starting checks for range sizes"))
			const sizeLimit = 3 * (1 << 29) // 3*512 MB (512 mb is default size)

			db := c.Conn(ctx, 1)
			defer db.Close()

			var size = float64(0)
			for tBegin := timeutil.Now(); timeutil.Since(tBegin) <= duration; {
				if err := db.QueryRow(
					`select max(bytes_per_replica->'PMax') from kwdb_internal.kv_store_status;`,
				).Scan(&size); err != nil {
					return err
				}
				if size > sizeLimit {
					return errors.Errorf("range size %s exceeded %s", humanizeutil.IBytes(int64(size)),
						humanizeutil.IBytes(int64(sizeLimit)))
				}

				t.Status(fmt.Sprintf("max range size %s", humanizeutil.IBytes(int64(size))))

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}

			return nil
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	minutes := 10 * time.Minute
	numNodes := 4
	concurrency := 128

	r.Add(testSpec{
		Name:  fmt.Sprintf("hotspotsplits/nodes=%d", numNodes),
		Owner: OwnerKV,
		// Test OOMs below this version because of scans over the large rows.
		// No problem in 20.1 thanks to:
		// https://gitee.com/kwbasedb/kwbase/pull/45323.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				concurrency = 32
				fmt.Printf("lowering concurrency to %d in local testing\n", concurrency)
			}
			runHotSpot(ctx, t, c, minutes, concurrency)
		},
	})
}
