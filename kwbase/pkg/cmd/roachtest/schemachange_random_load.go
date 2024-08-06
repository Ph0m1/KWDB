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
	"strings"
)

type randomLoadBenchSpec struct {
	Nodes       int
	Ops         int
	Concurrency int
}

func registerSchemaChangeRandomLoad(r *testRegistry) {
	r.Add(testSpec{
		Name:       "schemachange/random-load",
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(3),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxOps := 5000
			concurrency := 20
			if local {
				maxOps = 1000
				concurrency = 2
			}
			runSchemaChangeRandomLoad(ctx, t, c, maxOps, concurrency)
		},
	})

	// Run a few representative scbench specs in CI.
	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         2000,
		Concurrency: 1,
	})

	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         10000,
		Concurrency: 20,
	})
}

func registerRandomLoadBenchSpec(r *testRegistry, b randomLoadBenchSpec) {
	nameParts := []string{
		"scbench",
		"randomload",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("ops=%d", b.Ops),
		fmt.Sprintf("conc=%d", b.Concurrency),
	}
	name := strings.Join(nameParts, "/")

	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(b.Nodes),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSchemaChangeRandomLoad(ctx, t, c, b.Ops, b.Concurrency)
		},
	})
}

func runSchemaChangeRandomLoad(ctx context.Context, t *test, c *cluster, maxOps, concurrency int) {
	loadNode := c.Node(1)
	roachNodes := c.Range(1, c.spec.NodeCount)
	t.Status("copying binaries")
	c.Put(ctx, kwbase, "./kwbase", roachNodes)
	c.Put(ctx, workload, "./workload", loadNode)

	t.Status("starting kwbase nodes")
	c.Start(ctx, t, roachNodes)
	c.Run(ctx, loadNode, "./workload init schemachange")

	runCmd := []string{
		"./workload run schemachange --verbose=1",
		// The workload is still in development and occasionally discovers schema
		// change errors so for now we don't fail on them but only on panics, server
		// crashes, deadlocks, etc.
		// TODO(spaskob): remove when https://gitee.com/kwbasedb/kwbase/issues/47430
		// is closed.
		"--tolerate-errors=true",
		// Save the histograms so that they can be reported to https://roachperf.kwdb.dev/.
		" --histograms=" + perfArtifactsDir + "/stats.json",
		fmt.Sprintf("--max-ops %d", maxOps),
		fmt.Sprintf("--concurrency %d", concurrency),
	}
	t.Status("running schemachange workload")
	c.Run(ctx, loadNode, runCmd...)
}
