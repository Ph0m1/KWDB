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

	"gitee.com/kwbasedb/kwbase/pkg/util/version"
)

func registerSchemaChangeMixedVersions(r *testRegistry) {
	r.Add(testSpec{
		Name:  "schemachange/mixed-versions",
		Owner: OwnerSQLSchema,
		// This tests the work done for 20.1 that made schema changes jobs and in
		// addition prevented making any new schema changes on a mixed cluster in
		// order to prevent bugs during upgrades.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxOps := 100
			concurrency := 5
			if local {
				maxOps = 10
				concurrency = 2
			}
			runSchemaChangeMixedVersions(ctx, t, c, maxOps, concurrency, r.buildVersion)
		},
	})
}

func uploadAndInitSchemaChangeWorkload() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Stage workload on all nodes as the load node to run workload is chosen
		// randomly.
		u.c.Put(ctx, workload, "./workload", u.c.All())
		u.c.Run(ctx, u.c.All(), "./workload init schemachange")
	}
}

func runSchemaChangeWorkloadStep(loadNode, maxOps, concurrency int) versionStep {
	var numFeatureRuns int
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		numFeatureRuns++
		t.l.Printf("Workload step run: %d", numFeatureRuns)
		runCmd := []string{
			"./workload run schemachange --verbose=1",
			// The workload is still in development and occasionally discovers schema
			// change errors so for now we don't fail on them but only on panics, server
			// crashes, deadlocks, etc.
			// TODO(spaskob): remove when https://gitee.com/kwbasedb/kwbase/issues/47430
			// is closed.
			"--tolerate-errors=true",
			fmt.Sprintf("--max-ops %d", maxOps),
			fmt.Sprintf("--concurrency %d", concurrency),
			fmt.Sprintf("{pgurl:1-%d}", u.c.spec.NodeCount),
		}
		u.c.Run(ctx, u.c.Node(loadNode), runCmd...)
	}
}

func runSchemaChangeMixedVersions(
	ctx context.Context,
	t *test,
	c *cluster,
	maxOps int,
	concurrency int,
	buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	// An empty string will lead to the kwbase binary specified by flag
	// `kwbase` to be used.
	const mainVersion = ""
	schemaChangeStep := runSchemaChangeWorkloadStep(c.All().randNode()[0], maxOps, concurrency)
	if buildVersion.Major() < 20 {
		// Schema change workload is meant to run only on versions 19.2 or higher.
		// If the main version is below 20.1 then then predecessor version will be
		// below 19.2.
		schemaChangeStep = nil
	}

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(c.All(), predecessorVersion),
		uploadAndInitSchemaChangeWorkload(),
		waitForUpgradeStep(c.All()),

		// NB: at this point, cluster and binary version equal predecessorVersion,
		// and auto-upgrades are on.

		preventAutoUpgradeStep(1),
		schemaChangeStep,

		// Roll the nodes into the new version one by one, while repeatedly running
		// schema changes. We use an empty string for the version below, which means
		// use the main ./kwbase binary (i.e. the one being tested in this run).
		binaryUpgradeStep(c.Node(3), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(4), mainVersion),
		schemaChangeStep,

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(4), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(3), predecessorVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), predecessorVersion),
		schemaChangeStep,

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(3), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(1), mainVersion),
		schemaChangeStep,
		binaryUpgradeStep(c.Node(2), mainVersion),
		schemaChangeStep,

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(c.All()),
		schemaChangeStep,
	)

	u.run(ctx, t)
}
