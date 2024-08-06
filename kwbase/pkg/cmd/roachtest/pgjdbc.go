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

var pgjdbcReleaseTagRegex = regexp.MustCompile(`^REL(?P<major>\d+)\.(?P<minor>\d+)\.(?P<point>\d+)$`)
var supportedPGJDBCTag = "REL42.2.9"

// This test runs pgjdbc's full test suite against a single kwbase node.

func registerPgjdbc(r *testRegistry) {
	runPgjdbc := func(
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

		t.Status("cloning pgjdbc and installing prerequisites")
		// Report the latest tag, but do not use it. The newest versions produces output that breaks our xml parser,
		// and we want to pin to the working version for now.
		latestTag, err := repeatGetLatestTag(
			ctx, c, "pgjdbc", "pgjdbc", pgjdbcReleaseTagRegex,
		)
		if err != nil {
			t.Fatal(err)
		}
		c.l.Printf("Latest pgjdbc release is %s.", latestTag)

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
			`sudo apt-get -qq install default-jre openjdk-8-jdk-headless maven`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatRunE(
			ctx, c, node, "remove old pgjdbc", `rm -rf /mnt/data1/pgjdbc`,
		); err != nil {
			t.Fatal(err)
		}

		if err := repeatGitCloneE(
			ctx,
			t.l,
			c,
			"https://github.com/pgjdbc/pgjdbc.git",
			"/mnt/data1/pgjdbc",
			supportedPGJDBCTag,
			node,
		); err != nil {
			t.Fatal(err)
		}

		// In order to get pgjdbc's test suite to connect to kwbase, we have
		// to override settings in build.local.properties
		if err := repeatRunE(
			ctx,
			c,
			node,
			"configuring tests for kwbase only",
			fmt.Sprintf(
				"echo \"%s\" > /mnt/data1/pgjdbc/build.local.properties", pgjdbcDatabaseParams,
			),
		); err != nil {
			t.Fatal(err)
		}

		t.Status("building pgjdbc (without tests)")
		// Build pgjdbc and run a single test, this step involves some
		// downloading, so it needs a retry loop as well. Just building was not
		// enough as the test libraries are not downloaded unless at least a
		// single test is invoked.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"building pgjdbc (without tests)",
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn -Dtest=OidToStringTest test`,
		); err != nil {
			t.Fatal(err)
		}

		blocklistName, expectedFailures, ignorelistName, ignorelist := pgjdbcBlocklists.getLists(version)
		if expectedFailures == nil {
			t.Fatalf("No pgjdbc blocklist defined for kwbase version %s", version)
		}
		status := fmt.Sprintf("Running kwbase version %s, using blocklist %s", version, blocklistName)
		if ignorelist != nil {
			status = fmt.Sprintf("Running kwbase version %s, using blocklist %s, using ignorelist %s",
				version, blocklistName, ignorelistName)
		}
		c.l.Printf("%s", status)

		t.Status("running pgjdbc test suite")
		// Note that this is expected to return an error, since the test suite
		// will fail. And it is safe to swallow it here.
		_ = c.RunE(ctx, node,
			`cd /mnt/data1/pgjdbc/pgjdbc/ && mvn test`,
		)

		_ = c.RunE(ctx, node,
			`mkdir -p ~/logs/report/pgjdbc-results`,
		)

		t.Status("collecting the test results")
		// Copy all of the test results to the kwbase logs directory to be
		// copied to the artifacts.

		// Copy the individual test result files.
		if err := repeatRunE(
			ctx,
			c,
			node,
			"copy test result files",
			`cp /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports ~/logs/report/pgjdbc-results -a`,
		); err != nil {
			t.Fatal(err)
		}

		// Load the list of all test results files and parse them individually.
		// Files are here: /mnt/data1/pgjdbc/pgjdbc-core/target/test-results/test
		output, err := repeatRunWithBuffer(
			ctx,
			c,
			t.l,
			node,
			"get list of test files",
			`ls /mnt/data1/pgjdbc/pgjdbc/target/surefire-reports/*.xml`,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(output) == 0 {
			t.Fatal("could not find any test result files")
		}

		parseAndSummarizeJavaORMTestsResults(
			ctx, t, c, node, "pgjdbc" /* ormName */, output,
			blocklistName, expectedFailures, ignorelist, version, supportedPGJDBCTag,
		)
	}

	r.Add(testSpec{
		MinVersion: "v2.1.0",
		Name:       "pgjdbc",
		Owner:      OwnerAppDev,
		Cluster:    makeClusterSpec(1),
		Tags:       []string{`default`, `driver`},
		Run: func(ctx context.Context, t *test, c *cluster) {
			runPgjdbc(ctx, t, c)
		},
	})
}

const pgjdbcDatabaseParams = `
server=localhost
port=26257
secondaryServer=localhost
secondaryPort=5433
secondaryServer2=localhost
secondaryServerPort2=5434
database=defaultdb
username=root
password=
privilegedUser=root
privilegedPassword=
sspiusername=testsspi
preparethreshold=5
loggerLevel=DEBUG
loggerFile=target/pgjdbc-tests.log
protocolVersion=0
sslpassword=sslpwd
`
