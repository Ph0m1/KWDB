// Copyright 2019 The Cockroach Authors.
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

	"github.com/stretchr/testify/require"
)

// runIndexUpgrade runs a test that creates an index before a version upgrade,
// and modifies it in a mixed version setting. It aims to test the changes made
// to index encodings done to allow secondary indexes to respect column families.
func runIndexUpgrade(ctx context.Context, t *test, c *cluster, predecessorVersion string) {
	firstExpected := [][]int{
		{2, 3, 4},
		{6, 7, 8},
		{10, 11, 12},
		{14, 15, 17},
	}
	secondExpected := [][]int{
		{2, 3, 4},
		{6, 7, 8},
		{10, 11, 12},
		{14, 15, 17},
		{21, 25, 25},
	}

	roachNodes := c.All()
	// An empty string means that the kwbase binary specified by flag
	// `kwbase` will be used.
	const mainVersion = ""
	u := newVersionUpgradeTest(c,
		uploadAndStart(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),

		// Fill the cluster with data.
		createDataStep(),

		// Upgrade one of the nodes.
		binaryUpgradeStep(c.Node(1), mainVersion),

		// Modify index data from that node.
		modifyData(1,
			`INSERT INTO t VALUES (13, 14, 15, 16)`,
			`UPDATE t SET w = 17 WHERE y = 14`,
		),

		// Ensure all nodes see valid index data.
		verifyTableData(1, firstExpected),
		verifyTableData(2, firstExpected),
		verifyTableData(3, firstExpected),

		// Upgrade the rest of the cluster.
		binaryUpgradeStep(c.Node(2), mainVersion),
		binaryUpgradeStep(c.Node(3), mainVersion),

		// Finalize the upgrade.
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),

		// Modify some more data now that the cluster is upgraded.
		modifyData(1,
			`INSERT INTO t VALUES (20, 21, 22, 23)`,
			`UPDATE t SET w = 25, z = 25 WHERE y = 21`,
		),

		// Ensure all nodes see valid index data.
		verifyTableData(1, secondExpected),
		verifyTableData(2, secondExpected),
		verifyTableData(3, secondExpected),
	)

	u.run(ctx, t)
}

func createDataStep() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, 1)
		if _, err := conn.Exec(`
CREATE TABLE t (
	x INT PRIMARY KEY, y INT, z INT, w INT,
	INDEX i (y) STORING (z, w),
	FAMILY (x), FAMILY (y), FAMILY (z), FAMILY (w)
);
INSERT INTO t VALUES (1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12);
`); err != nil {
			t.Fatal(err)
		}
	}
}

func modifyData(node int, sql ...string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Write some data into the table.
		conn := u.conn(ctx, t, node)
		for _, s := range sql {
			if _, err := conn.Exec(s); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func verifyTableData(node int, expected [][]int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		conn := u.conn(ctx, t, node)
		rows, err := conn.Query(`SELECT y, z, w FROM t@i ORDER BY y`)
		if err != nil {
			t.Fatal(err)
		}
		var y, z, w int
		count := 0
		for ; rows.Next(); count++ {
			if err := rows.Scan(&y, &z, &w); err != nil {
				t.Fatal(err)
			}
			found := []int{y, z, w}
			require.Equal(t, found, expected[count])
		}
	}
}

func registerSecondaryIndexesMultiVersionCluster(r *testRegistry) {
	r.Add(testSpec{
		Name:       "schemachange/secondary-index-multi-version",
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(3),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			runIndexUpgrade(ctx, t, c, predV)
		},
	})
}
