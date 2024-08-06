// Copyright 2016 The Cockroach Authors.
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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListFailures(t *testing.T) {
	t.Skip()
	type issue struct {
		testName string
		title    string
		message  string
		author   string
	}
	// Each test case expects a number of issues.
	testCases := []struct {
		pkgEnv    string
		fileName  string
		expPkg    string
		expIssues []issue
	}{
		{
			pkgEnv:   "",
			fileName: "implicit-pkg.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/util/stop",
			expIssues: []issue{{
				testName: "TestStopperWithCancelConcurrent",
				title:    "util/stop: TestStopperWithCancelConcurrent failed",
				message:  "this is just a testing issue",
				author:   "nvanbenschoten@gmail.com",
			}},
		},
		{
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvserver",
			fileName: "stress-failure.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvserver",
			expIssues: []issue{{
				testName: "TestReplicateQueueRebalance",
				title:    "kv/kvserver: TestReplicateQueueRebalance failed",
				message:  "replicate_queue_test.go:88: condition failed to evaluate within 45s: not balanced: [10 1 10 1 8]",
				author:   "petermattis@gmail.com",
			}},
		},
		{
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvserver",
			fileName: "stress-fatal.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvserver",
			expIssues: []issue{{
				testName: "TestGossipHandlesReplacedNode",
				title:    "kv/kvserver: TestGossipHandlesReplacedNode failed",
				message:  "F180711 20:13:15.826193 83 storage/replica.go:1877  [n?,s1,r1/1:/M{in-ax}] on-disk and in-memory state diverged:",
				author:   "alexdwanerobinson@gmail.com",
			}},
		},
		{
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/storage",
			fileName: "stress-unknown.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/storage",
			expIssues: []issue{{
				testName: "(unknown)",
				title:    "storage: package failed",
				message:  "make: *** [bin/.submodules-initialized] Error 1",
				author:   "",
			}},
		},
		{
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/util/json",
			fileName: "stress-subtests.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/util/json",
			expIssues: []issue{{
				testName: "TestPretty",
				title:    "util/json: TestPretty failed",
				message: `=== RUN   TestPretty/["hello",_["world"]]
    --- FAIL: TestPretty/["hello",_["world"]] (0.00s)
    	json_test.go:1656: injected failure`,
				author: "justin@cockroachlabs.com",
			}},
		},
		{
			// A test run where there's a timeout, and the timed out test was the
			// longest running test, so the issue assumes it's the culprit.
			// To spice things up, the test run has another test failure too.
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord",
			fileName: "timeout-culprit-found.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord",
			expIssues: []issue{
				{
					testName: "TestTxnCoordSenderPipelining",
					title:    "kv/kvclient/kvcoord: TestTxnCoordSenderPipelining failed",
					message:  `injected failure`,
					author:   "nikhil.benesch@gmail.com",
				},
				{
					testName: "TestAbortReadOnlyTransaction",
					title:    "kv/kvclient/kvcoord: TestAbortReadOnlyTransaction timed out",
					message: `Slow failing tests:
TestAbortReadOnlyTransaction - 3.99s
TestTxnCoordSenderPipelining - 1.00s

Slow passing tests:
TestAnchorKey - 1.01s
`,
					author: "andrei@cockroachlabs.com",
				},
			},
		},
		{
			// A test run where there's a timeout, but the test that happened to be
			// running when the timeout hit has not been running for very long, and so
			// the issue just names the package.
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			fileName: "timeout-culprit-not-found.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package timed out",
					message: `Slow failing tests:
TestXXX/sub3 - 0.50s

Slow passing tests:
TestXXA - 1.00s
`,
					author: "",
				},
			},
		},
		{
			// Like the above, except this time the output comes from a stress run,
			// not from the test binary directly.
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			fileName: "stress-timeout-culprit-not-found.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package timed out",
					message: `Slow failing tests:
TestXXX/sub1 - 0.49s

Slow passing tests:
TestXXB - 1.01s
TestXXA - 1.00s
`,
					author: "",
				},
			},
		},
		{
			// A stress timeout where the test running when the timeout is hit is the
			// longest.
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			fileName: "stress-timeout-culprit-found.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			expIssues: []issue{
				{
					testName: "TestXXX/sub2",
					title:    "kv: TestXXX/sub2 timed out",
					message: `Slow failing tests:
TestXXX/sub2 - 2.99s

Slow passing tests:
TestXXB - 1.01s
TestXXA - 1.00s
`,
					author: "",
				},
			},
		},
		{
			// A panic in a test.
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			fileName: "stress-panic.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			expIssues: []issue{
				{
					testName: "TestXXX",
					title:    "kv: TestXXX failed",
					message:  `panic: induced panic`,
					author:   "",
				},
			},
		},
		{
			// A panic outside of a test (in this case, in a package init function).
			pkgEnv:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			fileName: "stress-init-panic.json",
			expPkg:   "gitee.com/kwbasedb/kwbase/pkg/kv",
			expIssues: []issue{
				{
					testName: "(unknown)",
					title:    "kv: package failed",
					message:  `panic: induced panic`,
					author:   "",
				},
			},
		},
	}
	for _, c := range testCases {
		t.Run(c.fileName, func(t *testing.T) {
			if err := os.Setenv("PKG", c.pkgEnv); err != nil {
				t.Fatal(err)
			}

			file, err := os.Open(filepath.Join("testdata", c.fileName))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			curIssue := 0

			f := func(_ context.Context, title, packageName, testName, testMessage, author string) error {
				if t.Failed() {
					return nil
				}
				if curIssue >= len(c.expIssues) {
					t.Errorf("unexpected issue filed. title: %s", title)
				}
				if exp := c.expPkg; exp != packageName {
					t.Errorf("expected package %s, but got %s", exp, packageName)
				}
				if exp := c.expIssues[curIssue].testName; exp != testName {
					t.Errorf("expected test name %s, but got %s", exp, testName)
				}
				if exp := c.expIssues[curIssue].author; exp != "" && exp != author {
					t.Errorf("expected author %s, but got %s", exp, author)
				}
				if exp := c.expIssues[curIssue].title; exp != title {
					t.Errorf("expected title %s, but got %s", exp, title)
				}
				if exp := c.expIssues[curIssue].message; !strings.Contains(testMessage, exp) {
					t.Errorf("expected message containing %s, but got:\n%s", exp, testMessage)
				}
				// On next invocation, we'll check the next expected issue.
				curIssue++
				return nil
			}
			if err := listFailures(context.Background(), file, f); err != nil {
				t.Fatal(err)
			}
			if curIssue != len(c.expIssues) {
				t.Fatalf("expected %d issues, got: %d", len(c.expIssues), curIssue)
			}
		})
	}
}
