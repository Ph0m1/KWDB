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

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"golang.org/x/exp/rand"
)

func registerEngineSwitch(r *testRegistry) {
	runEngineSwitch := func(ctx context.Context, t *test, c *cluster, additionalArgs ...string) {
		roachNodes := c.Range(1, c.spec.NodeCount-1)
		loadNode := c.Node(c.spec.NodeCount)
		c.Put(ctx, workload, "./workload", loadNode)
		c.Put(ctx, kwbase, "./kwbase", roachNodes)
		pebbleArgs := startArgs(append(additionalArgs, "--args=--storage-engine=pebble")...)
		rocksdbArgs := startArgs(append(additionalArgs, "--args=--storage-engine=rocksdb")...)
		c.Start(ctx, t, roachNodes, rocksdbArgs)
		stageDuration := 1 * time.Minute
		if local {
			t.l.Printf("local mode: speeding up test\n")
			stageDuration = 10 * time.Second
		}
		numIters := 5 * len(roachNodes)

		loadDuration := " --duration=" + (time.Duration(numIters) * stageDuration).String()

		var deprecatedWorkloadsStr string
		if !t.buildVersion.AtLeast(version.MustParse("v20.2.0")) {
			deprecatedWorkloadsStr += " --deprecated-fk-indexes"
		}

		workloads := []string{
			// Currently tpcc is the only one with CheckConsistency. We can add more later.
			"./workload run tpcc --tolerate-errors --wait=false --drop --init" + deprecatedWorkloadsStr + " --warehouses=1 " + loadDuration + " {pgurl:1-%d}",
		}
		checkWorkloads := []string{
			"./workload check tpcc --warehouses=1 --expensive-checks=true {pgurl:1}",
		}
		m := newMonitor(ctx, c, roachNodes)
		for _, cmd := range workloads {
			cmd := cmd // loop-local copy
			m.Go(func(ctx context.Context) error {
				cmd = fmt.Sprintf(cmd, len(roachNodes))
				return c.RunE(ctx, loadNode, cmd)
			})
		}

		usingPebble := make([]bool, len(roachNodes))
		rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))
		m.Go(func(ctx context.Context) error {
			l, err := t.l.ChildLogger("engine-switcher")
			if err != nil {
				return err
			}
			// NB: the number of calls to `sleep` needs to be reflected in `loadDuration`.
			sleepAndCheck := func() error {
				t.WorkerStatus("sleeping")
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(stageDuration):
				}
				// Make sure everyone is still running.
				for i := 1; i <= len(roachNodes); i++ {
					t.WorkerStatus("checking ", i)
					db := c.Conn(ctx, i)
					defer db.Close()
					rows, err := db.Query(`SHOW DATABASES`)
					if err != nil {
						return err
					}
					if err := rows.Close(); err != nil {
						return err
					}
					if err := c.CheckReplicaDivergenceOnDB(ctx, db); err != nil {
						return errors.Wrapf(err, "node %d", i)
					}
				}
				return nil
			}

			for i := 0; i < numIters; i++ {
				// First let the load generators run in the cluster.
				if err := sleepAndCheck(); err != nil {
					return err
				}

				stop := func(node int) error {
					m.ExpectDeath()
					if rng.Intn(2) == 0 {
						l.Printf("stopping node gracefully %d\n", node)
						return c.StopCockroachGracefullyOnNode(ctx, node)
					}
					l.Printf("stopping node %d\n", node)
					c.Stop(ctx, c.Node(node))
					return nil
				}

				i := rng.Intn(len(roachNodes))
				var args option
				usingPebble[i] = !usingPebble[i]
				if usingPebble[i] {
					args = pebbleArgs
				} else {
					args = rocksdbArgs
				}
				t.WorkerStatus("switching ", i+1)
				l.Printf("switching %d\n", i+1)
				if err := stop(i + 1); err != nil {
					return err
				}
				c.Start(ctx, t, c.Node(i+1), args)
			}
			return sleepAndCheck()
		})
		m.Wait()

		for _, cmd := range checkWorkloads {
			c.Run(ctx, loadNode, cmd)
		}
	}

	n := 3
	r.Add(testSpec{
		Name:       fmt.Sprintf("engine/switch/nodes=%d", n),
		Owner:      OwnerStorage,
		Skip:       "rocksdb removed in 21.1",
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(n + 1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runEngineSwitch(ctx, t, c)
		},
	})
	r.Add(testSpec{
		Name:       fmt.Sprintf("engine/switch/encrypted/nodes=%d", n),
		Owner:      OwnerStorage,
		Skip:       "rocksdb removed in 21.1",
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(n + 1),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runEngineSwitch(ctx, t, c, "--encrypt=true")
		},
	})
}
