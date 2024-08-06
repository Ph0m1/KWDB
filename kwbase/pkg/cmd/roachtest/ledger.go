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

func registerLedger(r *testRegistry) {
	const nodes = 6
	const azs = "us-central1-a,us-central1-b,us-central1-c"
	r.Add(testSpec{
		Name:    fmt.Sprintf("ledger/nodes=%d/multi-az", nodes),
		Owner:   OwnerKV,
		Cluster: makeClusterSpec(nodes+1, cpu(16), geo(), zones(azs)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			roachNodes := c.Range(1, nodes)
			gatewayNodes := c.Range(1, nodes/3)
			loadNode := c.Node(nodes + 1)

			c.Put(ctx, kwbase, "./kwbase", roachNodes)
			c.Put(ctx, workload, "./workload", loadNode)
			c.Start(ctx, t, roachNodes)

			t.Status("running workload")
			m := newMonitor(ctx, c, roachNodes)
			m.Go(func(ctx context.Context) error {
				concurrency := ifLocal("", " --concurrency="+fmt.Sprint(nodes*32))
				duration := " --duration=" + ifLocal("10s", "10m")

				cmd := fmt.Sprintf("./workload run ledger --init --histograms="+perfArtifactsDir+"/stats.json"+
					concurrency+duration+" {pgurl%s}", gatewayNodes)
				c.Run(ctx, loadNode, cmd)
				return nil
			})
			m.Wait()
		},
	})
}
