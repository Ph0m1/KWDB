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
	"strconv"
	"strings"
)

func registerNIndexes(r *testRegistry, secondaryIndexes int) {
	const nodes = 6
	geoZones := []string{"us-west1-b", "us-east1-b", "us-central1-a"}
	geoZonesStr := strings.Join(geoZones, ",")
	r.Add(testSpec{
		Name:    fmt.Sprintf("indexes/%d/nodes=%d/multi-region", secondaryIndexes, nodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(nodes+1, cpu(16), geo(), zones(geoZonesStr)),
		// Uses CONFIGURE ZONE USING ... COPY FROM PARENT syntax.
		MinVersion: `v19.1.0`,
		Run: func(ctx context.Context, t *test, c *cluster) {
			firstAZ := geoZones[0]
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, kwbase, "./kwbase", roachNodes)
			c.Put(ctx, workload, "./workload", loadNode)
			c.Start(ctx, t, roachNodes)

			t.Status("running workload")
			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				secondary := " --secondary-indexes=" + strconv.Itoa(secondaryIndexes)
				initCmd := "./workload init indexes" + secondary + " {pgurl:1}"
				c.Run(ctx, loadNode, initCmd)

				// Set lease preferences so that all leases for the table are
				// located in the availability zone with the load generator.
				if !local {
					leasePrefs := fmt.Sprintf(`ALTER TABLE indexes.indexes
						                       CONFIGURE ZONE USING
						                       constraints = COPY FROM PARENT,
						                       lease_preferences = '[[+zone=%s]]'`, firstAZ)
					c.Run(ctx, c.Node(1), `./kwbase sql --insecure -e "`+leasePrefs+`"`)
				}

				payload := " --payload=256"
				concurrency := ifLocal("", " --concurrency="+strconv.Itoa(nodes*32))
				duration := " --duration=" + ifLocal("10s", "10m")
				runCmd := fmt.Sprintf("./workload run indexes --histograms="+perfArtifactsDir+"/stats.json"+
					payload+concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, runCmd)
				return nil
			})
			m.Wait()
		},
	})
}

func registerIndexes(r *testRegistry) {
	registerNIndexes(r, 2)
}

func registerIndexesBench(r *testRegistry) {
	for i := 0; i <= 10; i++ {
		registerNIndexes(r, i)
	}
}
