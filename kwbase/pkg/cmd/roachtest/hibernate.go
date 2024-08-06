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
	"regexp"
)

var hibernateReleaseTagRegex = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedHibernateTag = "5.4.20"

// This test runs hibernate-core's full test suite against a single kwbase
// node.

func registerHibernate(r *testRegistry) {
	runHibernate := func(
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

		t.Status("cloning hibernate and installing prerequisites")
		latestTag, err := repeatGetLatestTag(
			ctx, c, "hibernate", "hibernate-orm", hibernateReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest Hibernate release is %s.", latestTag)
		c.l.Printf("Supported Hibernate release is %s.", supportedHibernateTag)

		if err := repeatRunE(
			ctx, c, node, "update apt-get", `sudo apt-get -qq update`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx,
			c,
			node,
			"install dependencies",
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless gradle`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old Hibernate", `rm -rf /mnt/data1/hibernate`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/hibernate/hibernate-orm.git",
			"/mnt/data1/hibernate",
			supportedHibernateTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building hibernate (without tests)")
		// Build hibernate and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"building hibernate (without tests)",
			`cd /mnt/data1/hibernate/hibernate-core/ && ./../gradlew test -Pdb=kwbasedb `+
				`--tests org.hibernate.jdbc.util.BasicFormatterTest.*`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, _, _ := hibernateBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No hibernate blocklist defined for kwbase version %s", version)
		}
		c.l.Printf("Running kwbase version %s, using blocklist %s", version, blocklistName)

		t.Status("running hibernate test suite, will take at least 3 hours")
		// When testing, it is helpful to run only a subset of the tests. To do so
		// add "--tests org.hibernate.test.annotations.lob.*" to the end of the
		// test run command.
		// Note that this will take upwards of 3 hours.
		// Also note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/hibernate/ && `+
				`HIBERNATE_CONNECTION_LEAK_DETECTION=true ./gradlew test -Pdb=kwbasedb`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the kwbase logs directory to be
		// copied to the artifacts.

		// Copy the html report for the test.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy html report",
			`cp /mnt/data1/hibernate/hibernate-core/target/reports/tests/test ~/logs/report -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/hibernate/hibernate-core/target/test-results/test ~/logs/report/results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/hibernate/hibernate-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/hibernate/*/target/test-results/test/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "hibernate" /* ormName */, output,
			blocklistName, expectedFailures, nil /* ignorelist */, version, supportedHibernateTag,
		)
	}

	r.Add(testSpec{
		Name:    "hibernate",
		Owner:   OwnerAppDev,
		Cluster: makeClusterSpec(1),
		Tags:    []string{`default`, `orm`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runHibernate(ctx, t, c)
		},
	})
}
