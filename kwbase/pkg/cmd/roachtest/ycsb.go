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
)

func registerYCSB(r *testRegistry) {
	workloads := []string{"A", "B", "C", "D", "E", "F"}
	cpusConfigs := []int{8, 32}

	// concurrencyConfigs contains near-optimal concurrency levels for each
	// (workload, cpu count) combination. All of these figures were tuned on GCP
	// n1-standard instance types. We should consider implementing a search for
	// the optimal concurrency level in the roachtest itself (see kvbench).
	concurrencyConfigs := map[string] /* workload */ map[int] /* cpus */ int{
		"A": {8: 96, 32: 144},
		"B": {8: 144, 32: 192},
		"C": {8: 144, 32: 192},
		"D": {8: 96, 32: 144},
		"E": {8: 96, 32: 144},
		"F": {8: 96, 32: 144},
	}

	runYCSB := func(ctx context.Context, t *test, c *cluster, wl string, cpus int) {
		nodes := c.spec.NodeCount - 1

		conc, ok := concurrencyConfigs[wl][cpus]
		if !ok {
			t.Fatalf("missing concurrency for (workload, cpus) = (%s, %d)", wl, cpus)
		}

		c.Put(ctx, kwbase, "./kwbase", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes))
		waitForFullReplication(t, c.Conn(ctx, 1))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			sfu := fmt.Sprintf(" --select-for-update=%t", t.IsBuildVersion("v19.2.0"))
			ramp := " --ramp=" + ifLocal("0s", "1m")
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --insert-count=1000000 --workload=%s --concurrency=%d"+
					" --splits=%d --histograms="+perfArtifactsDir+"/stats.json"+sfu+ramp+duration+
					" {pgurl:1-%d}",
				wl, conc, nodes, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	for _, wl := range workloads {
		for _, cpus := range cpusConfigs {
			var name string
			if cpus == 8 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", wl)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=3/cpu=%d", wl, cpus)
			}
			wl, cpus := wl, cpus
			r.Add(testSpec{
				Name:    name,
				Owner:   OwnerKV,
				Cluster: makeClusterSpec(4, cpu(cpus)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runYCSB(ctx, t, c, wl, cpus)
				},
			})
		}
	}
}
