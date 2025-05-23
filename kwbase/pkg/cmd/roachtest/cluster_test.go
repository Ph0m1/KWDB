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
	"os"
	"regexp"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterNodes(t *testing.T) {
	c := &cluster{spec: makeClusterSpec(10)}
	opts := func(opts ...option) []option {
		return opts
	}
	testCases := []struct {
		opts     []option
		expected string
	}{
		{opts(), ""},
		{opts(c.All()), ":1-10"},
		{opts(c.Range(1, 2)), ":1-2"},
		{opts(c.Range(2, 5)), ":2-5"},
		{opts(c.All(), c.Range(2, 5)), ":1-10"},
		{opts(c.Range(2, 5), c.Range(7, 9)), ":2-5,7-9"},
		{opts(c.Range(2, 5), c.Range(6, 8)), ":2-8"},
		{opts(c.Node(2), c.Node(4), c.Node(6)), ":2,4,6"},
		{opts(c.Node(2), c.Node(3), c.Node(4)), ":2-4"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			nodes := c.makeNodes(tc.opts...)
			if tc.expected != nodes {
				t.Fatalf("expected %s, but found %s", tc.expected, nodes)
			}
		})
	}
}

type testWrapper struct {
	*testing.T
}

func (t testWrapper) ArtifactsDir() string {
	return ""
}

func (t testWrapper) logger() *logger {
	return nil
}

func TestExecCmd(t *testing.T) {
	cfg := &loggerConfig{stdout: os.Stdout, stderr: os.Stderr}
	logger, err := cfg.newLogger("" /* path */)
	if err != nil {
		t.Fatal(err)
	}

	t.Run(`success`, func(t *testing.T) {
		res := execCmdEx(context.Background(), logger, "/bin/bash", "-c", "echo guacamole")
		require.NoError(t, res.err)
		require.Contains(t, res.stdout, "guacamole")
	})

	t.Run(`error`, func(t *testing.T) {
		res := execCmdEx(context.Background(), logger, "/bin/bash", "-c", "echo burrito; false")
		require.Error(t, res.err)
		require.Contains(t, res.stdout, "burrito")
	})

	t.Run(`returns-on-cancel`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		tBegin := timeutil.Now()
		require.Error(t, execCmd(ctx, logger, "/bin/bash", "-c", "sleep 100"))
		if max, act := 99*time.Second, timeutil.Since(tBegin); max < act {
			t.Fatalf("took %s despite cancellation", act)
		}
	})

	t.Run(`returns-on-cancel-subprocess`, func(t *testing.T) {
		// The tricky version of the preceding test. The difference is that the process
		// spawns a stalling subprocess and then waits for it. See execCmdEx for a
		// detailed discussion of how this is made work.
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		tBegin := timeutil.Now()
		require.Error(t, execCmd(ctx, logger, "/bin/bash", "-c", "sleep 100& wait"))
		if max, act := 99*time.Second, timeutil.Since(tBegin); max < act {
			t.Fatalf("took %s despite cancellation", act)
		}
	})
}

func TestClusterMonitor(t *testing.T) {
	cfg := &loggerConfig{stdout: os.Stdout, stderr: os.Stderr}
	logger, err := cfg.newLogger("" /* path */)
	if err != nil {
		t.Fatal(err)
	}

	t.Run(`success`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(context.Context) error { return nil })
		if err := m.wait(`echo`, `1`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run(`dead`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			fmt.Printf("worker done\n")
			return ctx.Err()
		})

		err := m.wait(`echo`, "1: 100\n1: dead")
		expectedErr := `dead`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`worker-fail`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(context.Context) error {
			return errors.New(`worker-fail`)
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		err := m.wait(`sleep`, `100`)
		expectedErr := `worker-fail`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`wait-fail`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		// Returned error should be that from the wait command.
		err := m.wait(`false`)
		expectedErr := `exit status`
		if !testutils.IsError(err, expectedErr) {
			t.Errorf(`expected %s err got: %+v`, expectedErr, err)
		}
	})

	t.Run(`wait-ok`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		m.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})

		// If wait terminates, context gets canceled.
		err := m.wait(`true`)
		if !errors.Is(err, context.Canceled) {
			t.Errorf(`expected context canceled, got: %+v`, err)
		}
	})

	// NB: the forker sleeps in these tests actually get leaked, so it's important to let
	// them finish pretty soon (think stress testing). As a matter of fact, `make test` waits
	// for these child goroutines to finish (so these tests take seconds).
	t.Run(`worker-fd-error`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			defer func() {
				fmt.Println("sleep returns")
			}()
			return execCmd(ctx, logger, "/bin/bash", "-c", "sleep 3& wait")
		})
		m.Go(func(ctx context.Context) error {
			defer func() {
				fmt.Println("failure returns")
			}()
			time.Sleep(30 * time.Millisecond)
			return execCmd(ctx, logger, "/bin/bash", "-c", "echo hi && notthere")
		})
		expectedErr := regexp.QuoteMeta(`exit status 127`)
		if err := m.wait("sleep", "100"); !testutils.IsError(err, expectedErr) {
			t.Logf("error details: %+v", err)
			t.Error(err)
		}
	})
	t.Run(`worker-fd-fatal`, func(t *testing.T) {
		c := &cluster{t: testWrapper{t}, l: logger}
		m := newMonitor(context.Background(), c)
		m.Go(func(ctx context.Context) error {
			err := execCmd(ctx, logger, "/bin/bash", "-c", "echo foo && sleep 3& wait")
			return err
		})
		m.Go(func(ctx context.Context) error {
			time.Sleep(30 * time.Millisecond)
			// Simulate c.t.Fatal for which there isn't enough mocking here.
			// In reality t.Fatal adds text that is returned when the test fails,
			// so the failing goroutine will be referenced (not like in the expected
			// error below, where all you see is the other one being canceled).
			panic(errTestFatal)
		})
		expectedErr := regexp.QuoteMeta(`t.Fatal() was called`)
		if err := m.wait("sleep", "100"); !testutils.IsError(err, expectedErr) {
			t.Logf("error details: %+v", err)
			t.Error(err)
		}
	})
}

func TestClusterMachineType(t *testing.T) {
	testCases := []struct {
		machineType      string
		expectedCPUCount int
	}{
		// AWS machine types
		{"m5.large", 2},
		{"m5.xlarge", 4},
		{"m5.2xlarge", 8},
		{"m5.4xlarge", 16},
		{"m5.12xlarge", 48},
		{"m5.24xlarge", 96},
		{"m5d.large", 2},
		{"m5d.xlarge", 4},
		{"m5d.2xlarge", 8},
		{"m5d.4xlarge", 16},
		{"m5d.12xlarge", 48},
		{"m5d.24xlarge", 96},
		{"c5d.large", 2},
		{"c5d.xlarge", 4},
		{"c5d.2xlarge", 8},
		{"c5d.4xlarge", 16},
		{"c5d.9xlarge", 36},
		{"c5d.18xlarge", 72},
		// GCE machine types
		{"n1-standard-1", 1},
		{"n1-standard-2", 2},
		{"n1-standard-4", 4},
		{"n1-standard-8", 8},
		{"n1-standard-16", 16},
		{"n1-standard-32", 32},
		{"n1-standard-64", 64},
		{"n1-standard-96", 96},
	}
	for _, tc := range testCases {
		t.Run(tc.machineType, func(t *testing.T) {
			cpuCount := MachineTypeToCPUs(tc.machineType)
			if tc.expectedCPUCount != cpuCount {
				t.Fatalf("expected %d CPUs, but found %d", tc.expectedCPUCount, cpuCount)
			}
		})
	}
}

func TestLoadGroups(t *testing.T) {
	cfg := &loggerConfig{stdout: os.Stdout, stderr: os.Stderr}
	logger, err := cfg.newLogger("" /* path */)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		numZones, numRoachNodes, numLoadNodes int
		loadGroups                            loadGroupList
	}{
		{
			3, 9, 3,
			loadGroupList{
				{
					nodeListOption{1, 2, 3},
					nodeListOption{4},
				},
				{
					nodeListOption{5, 6, 7},
					nodeListOption{8},
				},
				{
					nodeListOption{9, 10, 11},
					nodeListOption{12},
				},
			},
		},
		{
			3, 9, 1,
			loadGroupList{
				{
					nodeListOption{1, 2, 3, 4, 5, 6, 7, 8, 9},
					nodeListOption{10},
				},
			},
		},
		{
			4, 8, 2,
			loadGroupList{
				{
					nodeListOption{1, 2, 3, 4},
					nodeListOption{9},
				},
				{
					nodeListOption{5, 6, 7, 8},
					nodeListOption{10},
				},
			},
		},
	} {
		t.Run(fmt.Sprintf("%d/%d/%d", tc.numZones, tc.numRoachNodes, tc.numLoadNodes),
			func(t *testing.T) {
				c := &cluster{t: testWrapper{t}, l: logger, spec: makeClusterSpec(tc.numRoachNodes + tc.numLoadNodes)}
				lg := makeLoadGroups(c, tc.numZones, tc.numRoachNodes, tc.numLoadNodes)
				require.EqualValues(t, lg, tc.loadGroups)
			})
	}
	t.Run("panics with too many load nodes", func(t *testing.T) {
		require.Panics(t, func() {

			numZones, numRoachNodes, numLoadNodes := 2, 4, 3
			makeLoadGroups(nil, numZones, numRoachNodes, numLoadNodes)
		}, "Failed to panic when number of load nodes exceeded number of zones")
	})
	t.Run("panics with unequal zones per load node", func(t *testing.T) {
		require.Panics(t, func() {
			numZones, numRoachNodes, numLoadNodes := 4, 4, 3
			makeLoadGroups(nil, numZones, numRoachNodes, numLoadNodes)
		}, "Failed to panic when number of zones is not divisible by number of load nodes")
	})
}

func TestCmdLogFileName(t *testing.T) {
	ts := time.Date(2000, 1, 1, 15, 4, 12, 0, time.Local)

	const exp = `run_150412.000_n1,3-4,9_kwbase_bla`
	nodes := nodeListOption{1, 3, 4, 9}
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./kwbase", "bla", "--foo", "bar"),
	)
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./kwbase bla --foo bar"),
	)
}
