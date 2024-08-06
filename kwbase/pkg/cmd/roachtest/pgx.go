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
	"fmt"
	"regexp"
)

var pgxReleaseTagRegex = regexp.MustCompile(`^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedPGXTag = "v4.6.0"

// This test runs pgx's full test suite against a single kwbase node.

func registerPgx(r *testRegistry) {
	runPgx := func(
		ctx context.Context,
		t *test,
		c *cluster,
	) {
		if c.isLocal() {
			t.Fatal("cannot be run in local mode")
		}
		node := c.Node(1)
		t.Status("setting up kwbase")
		c.Put(ctx, kwbase, "./kwbase", c.All())
		c.Start(ctx, t, c.All())

		version, err := fetchCockroachVersion(ctx, c, node[0])
		if err != nil {
			t.Fatal(err)
		}

		if err := alterZoneConfigAndClusterSettings(ctx, version, c, node[0]); err != nil {
			t.Fatal(err)
		}

		t.Status("setting up go")
		installLatestGolang(ctx, t, c, node)

		t.Status("getting pgx")
		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/jackc/pgx.git",
			"/mnt/data1/pgx",
			supportedPGXTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		latestTag, err := repeatGetLatestTag(ctx, c, "jackc", "pgx", pgxReleaseTagRegex)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest jackc/pgx release is %s.", latestTag)
		c.l.Printf("Supported release is %s.", supportedPGXTag)

		t.Status("installing go-junit-report")
		if err := repeatRunE(
			ctx, c, node, "install go-junit-report", "go get -u github.com/jstemmer/go-junit-report",
		); err != nil {
			t.Fatal(err)
		}

		t.Status("checking blocklist")
		blocklistName, expectedFailures, ignorelistName, ignorelist := pgxBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No pgx blocklist defined for kwbase version %s", version)
		}
		status := fmt.Sprintf("Running kwbase version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf("Running kwbase version %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		c.l.Printf("%s", status)

		t.Status("setting up test db")
		db, err := c.ConnE(ctx, node[0])
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err = db.ExecContext(
			ctx, `drop database if exists pgx_test; create database pgx_test;`,
		); err != nil {
			t.Fatal(err)
		}

		// This is expected to fail because the feature is unsupported by Cockroach, but pgx expects it.
		// https://gitee.com/kwbasedb/kwbase/issues/27796
		_, _ = db.ExecContext(
			ctx, `create domain uint64 as numeric(20,0);`,
		)

		t.Status("running pgx test suite")
		// Running the test suite is expected to error out, so swallow the error.
		xmlResults, _ := repeatRunWithBuffer(
			ctx, c, t.l, node,
			"run pgx test suite",
			"cd /mnt/data1/pgx && "+
				"PGX_TEST_DATABASE='postgresql://root:@localhost:26257/pgx_test' go test -v 2>&1 | "+
				"`go env GOPATH`/bin/go-junit-report",
		)

		results := newORMTestsResults()
		results.parseJUnitXML(t, expectedFailures, ignorelist, xmlResults)
		results.summarizeAll(
			t, "pgx", blocklistName, expectedFailures, version, supportedPGXTag,
		)
	}

	r.Add(testSpec{
		Name:       "pgx",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		MinVersion: "v20.2.0",
		Tags:       []string{`default`, `driver`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPgx(ctx, t, c)
		},
	})
}
