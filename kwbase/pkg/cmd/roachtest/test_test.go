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
	"bytes"
	"context"
	"io/ioutil"
	"regexp"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
)

const OwnerUnitTest Owner = `unittest`

const defaultParallelism = 10

func TestMatchOrSkip(t *testing.T) {
	testCases := []struct {
		filter       []string
		name         string
		tags         []string
		expected     bool
		expectedSkip string
	}{
		{nil, "foo", nil, true, ""},
		{nil, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"tag:b"}, "foo", nil, true, "[tag:b] does not match [default]"},
		{[]string{"tag:default"}, "foo", nil, true, ""},
		{[]string{"tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
		{[]string{"f"}, "foo", []string{"bar"}, true, "[tag:default] does not match [bar]"},
		{[]string{"f"}, "bar", []string{"bar"}, false, ""},
		{[]string{"f", "tag:b"}, "foo", []string{"bar"}, true, ""},
		{[]string{"f", "tag:f"}, "foo", []string{"bar"}, true, "[tag:f] does not match [bar]"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			f := newFilter(c.filter)
			spec := &testSpec{Name: c.name, Owner: OwnerUnitTest, Tags: c.tags}
			if value := spec.matchOrSkip(f); c.expected != value {
				t.Fatalf("expected %t, but found %t", c.expected, value)
			} else if value && c.expectedSkip != spec.Skip {
				t.Fatalf("expected %s, but found %s", c.expectedSkip, spec.Skip)
			}
		})
	}
}

func nilLogger() *logger {
	lcfg := loggerConfig{
		stdout: ioutil.Discard,
		stderr: ioutil.Discard,
	}
	l, err := lcfg.newLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

// note unit test with git tag
// func TestRunnerRun(t *testing.T) {
// 	ctx := context.Background()
// 	r, err := makeTestRegistry()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	r.Add(testSpec{
// 		Name:    "pass",
// 		Owner:   OwnerUnitTest,
// 		Run:     func(ctx context.Context, t *test, c *cluster) {},
// 		Cluster: makeClusterSpec(0),
// 	})
// 	r.Add(testSpec{
// 		Name:  "fail",
// 		Owner: OwnerUnitTest,
// 		Run: func(ctx context.Context, t *test, c *cluster) {
// 			t.Fatal("failed")
// 		},
// 		Cluster: makeClusterSpec(0),
// 	})
//
// 	testCases := []struct {
// 		filters []string
// 		expErr  string
// 	}{
// 		{nil, "some tests failed"},
// 		{[]string{"pass"}, ""},
// 		{[]string{"fail"}, "some tests failed"},
// 		{[]string{"pass|fail"}, "some tests failed"},
// 		{[]string{"pass", "fail"}, "some tests failed"},
// 		{[]string{"notests"}, "no test"},
// 	}
// 	for _, c := range testCases {
// 		t.Run("", func(t *testing.T) {
// 			tests := testsToRun(ctx, r, newFilter(c.filters))
// 			cr := newClusterRegistry()
// 			runner := newTestRunner(cr, r.buildVersion)
//
// 			lopt := loggingOpt{
// 				l:            nilLogger(),
// 				tee:          noTee,
// 				stdout:       ioutil.Discard,
// 				stderr:       ioutil.Discard,
// 				artifactsDir: "",
// 			}
// 			copt := clustersOpt{
// 				typ:                       roachprodCluster,
// 				user:                      "test_user",
// 				cpuQuota:                  1000,
// 				keepClustersOnTestFailure: false,
// 			}
// 			err := runner.Run(ctx, tests, 1, /* count */
// 				defaultParallelism, copt, "" /* artifactsDir */, lopt)
//
// 			if !testutils.IsError(err, c.expErr) {
// 				t.Fatalf("expected err: %q, but found %v. Filters: %s", c.expErr, err, c.filters)
// 			}
// 		})
// 	}
// }

type syncedBuffer struct {
	mu  syncutil.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunnerTestTimeout(t *testing.T) {
	ctx := context.Background()

	cr := newClusterRegistry()
	runner := newTestRunner(cr, version.Version{})

	var buf syncedBuffer
	lopt := loggingOpt{
		l:            nilLogger(),
		tee:          noTee,
		stdout:       &buf,
		stderr:       &buf,
		artifactsDir: "",
	}
	copt := clustersOpt{
		typ:                       roachprodCluster,
		user:                      "test_user",
		cpuQuota:                  1000,
		keepClustersOnTestFailure: false,
	}
	test := testSpec{
		Name:    `timeout`,
		Owner:   OwnerUnitTest,
		Timeout: 10 * time.Millisecond,
		Cluster: makeClusterSpec(0),
		Run: func(ctx context.Context, t *test, c *cluster) {
			<-ctx.Done()
		},
	}
	err := runner.Run(ctx, []testSpec{test}, 1, /* count */
		defaultParallelism, copt, "" /* artifactsDir */, lopt)
	if !testutils.IsError(err, "some tests failed") {
		t.Fatalf("expected error \"some tests failed\", got: %v", err)
	}

	out := buf.String()
	timeoutRE := regexp.MustCompile(`(?m)^.*test timed out \(.*\)$`)
	if !timeoutRE.MatchString(out) {
		t.Fatalf("unable to find \"timed out\" message:\n%s", out)
	}
}

// note unit test with git tag
// func TestRegistryPrepareSpec(t *testing.T) {
// 	dummyRun := func(context.Context, *test, *cluster) {}
//
// 	var listTests = func(t *testSpec) []string {
// 		return []string{t.Name}
// 	}
//
// 	testCases := []struct {
// 		spec          testSpec
// 		expectedErr   string
// 		expectedTests []string
// 	}{
// 		{
// 			testSpec{
// 				Name:    "a",
// 				Owner:   OwnerUnitTest,
// 				Run:     dummyRun,
// 				Cluster: makeClusterSpec(0),
// 			},
// 			"",
// 			[]string{"a"},
// 		},
// 		{
// 			testSpec{
// 				Name:       "a",
// 				Owner:      OwnerUnitTest,
// 				MinVersion: "v2.1.0",
// 				Run:        dummyRun,
// 				Cluster:    makeClusterSpec(0),
// 			},
// 			"",
// 			[]string{"a"},
// 		},
// 		{
// 			testSpec{
// 				Name:       "a",
// 				Owner:      OwnerUnitTest,
// 				MinVersion: "foo",
// 				Run:        dummyRun,
// 				Cluster:    makeClusterSpec(0),
// 			},
// 			"a: unable to parse min-version: invalid version string 'foo'",
// 			nil,
// 		},
// 	}
// 	for _, c := range testCases {
// 		t.Run("", func(t *testing.T) {
// 			r, err := makeTestRegistry()
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			err = r.prepareSpec(&c.spec)
// 			if !testutils.IsError(err, c.expectedErr) {
// 				t.Fatalf("expected %q, but found %q", c.expectedErr, err.Error())
// 			}
// 			if c.expectedErr == "" {
// 				tests := listTests(&c.spec)
// 				sort.Strings(tests)
// 				if diff := pretty.Diff(c.expectedTests, tests); len(diff) != 0 {
// 					t.Fatalf("unexpected tests:\n%s", strings.Join(diff, "\n"))
// 				}
// 			}
// 		})
// 	}
// }
//
// func TestRegistryMinVersion(t *testing.T) {
// 	ctx := context.Background()
// 	testCases := []struct {
// 		buildVersion string
// 		expectedA    bool
// 		expectedB    bool
// 		expErr       string
// 	}{
// 		{"v1.1.0", false, false, "no test matched filters"},
// 		{"v2.0.0", true, false, ""},
// 		{"v2.1.0", true, true, ""},
// 	}
// 	for _, c := range testCases {
// 		t.Run(c.buildVersion, func(t *testing.T) {
// 			var runA, runB bool
// 			r, err := makeTestRegistry()
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			r.Add(testSpec{
// 				Name:       "a",
// 				Owner:      OwnerUnitTest,
// 				MinVersion: "v2.0.0",
// 				Cluster:    makeClusterSpec(0),
// 				Run: func(ctx context.Context, t *test, c *cluster) {
// 					runA = true
// 				},
// 			})
// 			r.Add(testSpec{
// 				Name:       "b",
// 				Owner:      OwnerUnitTest,
// 				MinVersion: "v2.1.0",
// 				Cluster:    makeClusterSpec(0),
// 				Run: func(ctx context.Context, t *test, c *cluster) {
// 					runB = true
// 				},
// 			})
// 			if err := r.setBuildVersion(c.buildVersion); err != nil {
// 				t.Fatal(err)
// 			}
// 			tests := testsToRun(ctx, r, newFilter(nil))
//
// 			var buf syncedBuffer
// 			lopt := loggingOpt{
// 				l:            nilLogger(),
// 				tee:          noTee,
// 				stdout:       &buf,
// 				stderr:       &buf,
// 				artifactsDir: "",
// 			}
// 			copt := clustersOpt{
// 				typ:                       roachprodCluster,
// 				user:                      "test_user",
// 				cpuQuota:                  1000,
// 				keepClustersOnTestFailure: false,
// 			}
// 			cr := newClusterRegistry()
// 			runner := newTestRunner(cr, r.buildVersion)
// 			err = runner.Run(ctx, tests, 1, /* count */
// 				defaultParallelism, copt, "" /* artifactsDir */, lopt)
// 			if !testutils.IsError(err, c.expErr) {
// 				t.Fatalf("expected err: %q, got: %v", c.expErr, err)
// 			}
//
// 			if c.expectedA != runA || c.expectedB != runB {
// 				t.Fatalf("expected %t,%t, but got %t,%t\n%s",
// 					c.expectedA, c.expectedB, runA, runB, buf.String())
// 			}
// 		})
// 	}
// }
