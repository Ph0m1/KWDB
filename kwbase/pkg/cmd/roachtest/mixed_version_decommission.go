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
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// runDecommissionMixedVersions runs through randomized
// decommission/recommission processes in mixed-version clusters.
func runDecommissionMixedVersions(
	ctx context.Context, t *test, c *cluster, buildVersion version.Version,
) {
	predecessorVersion, err := PredecessorVersion(buildVersion)
	if err != nil {
		t.Fatal(err)
	}

	h := newDecommTestHelper(t, c)

	// The v20.2 CLI can only be run against servers running v20.2. For this
	// reason, we grab a handle on a specific server slated for an upgrade.
	pinnedUpgrade := h.getRandNode()
	t.l.Printf("pinned n%d for upgrade", pinnedUpgrade)

	// An empty string means that the kwbase binary specified by flag
	// `kwbase` will be used.
	const mainVersion = ""
	allNodes := c.All()
	u := newVersionUpgradeTest(c,
		// We upload both binaries to each node, to be able to vary the binary
		// used when issuing `kwbase node` subcommands.
		uploadVersion(allNodes, predecessorVersion),
		uploadVersion(allNodes, mainVersion),

		startVersion(allNodes, predecessorVersion),
		waitForUpgradeStep(allNodes),
		preventAutoUpgradeStep(h.nodeIDs[0]),

		// We upgrade a subset of the cluster to v20.2.
		binaryUpgradeStep(c.Node(pinnedUpgrade), mainVersion),
		binaryUpgradeStep(c.Node(h.getRandNodeOtherThan(pinnedUpgrade)), mainVersion),
		checkAllMembership(pinnedUpgrade, "active"),

		// 1. Partially decommission a random node from another random node. We
		// use the v20.1 CLI to do so.
		partialDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(pinnedUpgrade, "decommissioning"),

		// 2. Recommission all nodes, including the partially decommissioned
		// one, from a random node. Use the v20.1 CLI to do so.
		recommissionAllStep(h.getRandNode(), predecessorVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(pinnedUpgrade, "active"),
		//
		// 3. Attempt to fully decommission a from a random node, again using
		// the v20.1 CLI.
		fullyDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(pinnedUpgrade, "decommissioning"),

		// Roll back, which should to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(allNodes, predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),

		// Repeat similar recommission/decommission cycles as above. We can no
		// longer assert against the `membership` column as none of the servers
		// are running v20.2.
		recommissionAllStep(h.getRandNode(), predecessorVersion),
		checkNoDecommissioning(h.getRandNode()),

		partialDecommissionStep(h.getRandNode(), h.getRandNode(), predecessorVersion),
		checkOneDecommissioning(h.getRandNode()),

		// Roll all nodes forward, and finalize upgrade.
		binaryUpgradeStep(allNodes, mainVersion),
		allowAutoUpgradeStep(1),
		waitForUpgradeStep(allNodes),

		checkOneMembership(h.getRandNode(), "decommissioning"),

		// Use the v20.2 CLI here on forth. Lets start with recommissioning all
		// the nodes in the cluster.
		recommissionAllStep(h.getRandNode(), mainVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(h.getRandNode(), "active"),

		// We partially decommission a random node.
		partialDecommissionStep(h.getRandNode(), h.getRandNode(), mainVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(h.getRandNode(), "decommissioning"),

		// We check that recommissioning is still functional.
		recommissionAllStep(h.getRandNode(), mainVersion),
		checkNoDecommissioning(h.getRandNode()),
		checkAllMembership(h.getRandNode(), "active"),

		// We fully decommission a random node. We need to use the v20.2 CLI to
		// do so.
		fullyDecommissionStep(h.getRandNode(), h.getRandNode(), mainVersion),
		checkOneDecommissioning(h.getRandNode()),
		checkOneMembership(h.getRandNode(), "decommissioned"),
	)

	u.run(ctx, t)
}

// kwbaseBinaryPath is a shorthand to retrieve the path for a kwbase
// binary of a given version.
func kwbaseBinaryPath(version string) string {
	path := "./kwbase"
	if version != "" {
		path += "-" + version
	}
	return path
}

// partialDecommissionStep runs `kwbase node decommission --wait=none` from a
// given node, targeting another. It uses the specified binary version to run
// the command.
func partialDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), kwbaseBinaryPath(binaryVersion), "node", "decommission",
			"--wait=none", "--insecure", strconv.Itoa(target))
	}
}

// recommissionAllStep runs `kwbase node recommission` from a given node,
// targeting all nodes in the cluster. It uses the specified binary version to
// run the command.
func recommissionAllStep(from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), kwbaseBinaryPath(binaryVersion), "node", "recommission",
			"--insecure", c.All().nodeIDsString())
	}
}

// fullyDecommissionStep is like partialDecommissionStep, except it uses
// `--wait=all`.
func fullyDecommissionStep(target, from int, binaryVersion string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		c := u.c
		c.Run(ctx, c.Node(from), kwbaseBinaryPath(binaryVersion), "node", "decommission",
			"--wait=all", "--insecure", strconv.Itoa(target))
	}
}

// checkOneDecommissioning checks against the `decommissioning` column in
// kwdb_internal.gossip_liveness, asserting that only one node is marked as
// decommissioning. This check can be run against both v20.1 and v20.2 servers.
func checkOneDecommissioning(from int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// We use a retry block here (and elsewhere) because we're consulting
		// kwdb_internal.gossip_liveness, and need to make allowances for gossip
		// propagation delays.
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from kwdb_internal.gossip_liveness where decommissioning = true;`).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 1 {
				return errors.Newf("expected to find 1 node with decommissioning=true, found %d", count)
			}

			var nodeID int
			if err := db.QueryRow(
				`select node_id from kwdb_internal.gossip_liveness where decommissioning = true;`).Scan(&nodeID); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("n%d decommissioning=true", nodeID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkNoDecommissioning checks against the `decommissioning` column in
// kwdb_internal.gossip_liveness, asserting that only no nodes are marked as
// decommissioning. This check can be run against both v20.1 and v20.2 servers.
func checkNoDecommissioning(from int) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from kwdb_internal.gossip_liveness where decommissioning = true;`).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				return errors.Newf("expected to find 0 nodes with decommissioning=false, found %d", count)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkOneMembership checks against the `membership` column in
// kwdb_internal.gossip_liveness, asserting that only one node is marked with
// the specified membership status. This check can be only be run against
// servers running v20.2 and beyond.
func checkOneMembership(from int, membership string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from kwdb_internal.gossip_liveness where membership = $1;`, membership).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 1 {
				return errors.Newf("expected to find 1 node with membership=%s, found %d", membership, count)
			}

			var nodeID int
			if err := db.QueryRow(
				`select node_id from kwdb_internal.gossip_liveness where decommissioning = true;`).Scan(&nodeID); err != nil {
				t.Fatal(err)
			}
			t.l.Printf("n%d membership=%s", nodeID, membership)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// checkAllMembership checks against the `membership` column in
// kwdb_internal.gossip_liveness, asserting that all nodes are marked with
// the specified membership status. This check can be only be run against
// servers running v20.2 and beyond.
func checkAllMembership(from int, membership string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		if err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			db := u.conn(ctx, t, from)
			var count int
			if err := db.QueryRow(
				`select count(*) from kwdb_internal.gossip_liveness where membership != $1;`, membership).Scan(&count); err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				return errors.Newf("expected to find 0 nodes with membership!=%s, found %d", membership, count)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// uploadVersion uploads the specified kwbase binary version on the specified
// nodes.
func uploadVersion(nodes nodeListOption, version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		// Put the binary.
		u.uploadVersion(ctx, t, nodes, version)
	}
}

// startVersion starts the specified kwbase binary version on the specified
// nodes.
func startVersion(nodes nodeListOption, version string) versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		args := startArgs("--binary=" + kwbaseBinaryPath(version))
		u.c.Start(ctx, t, nodes, args, startArgsDontEncrypt)
	}
}
