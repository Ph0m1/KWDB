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
	"math/rand"
	"runtime"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func registerDiskStalledDetection(r *testRegistry) {
	for _, affectsLogDir := range []bool{false, true} {
		for _, affectsDataDir := range []bool{false, true} {
			// Grab copies of the args because we'll pass them into a closure.
			// Everyone's favorite bug to write in Go.
			affectsLogDir := affectsLogDir
			affectsDataDir := affectsDataDir
			r.Add(testSpec{
				Name: fmt.Sprintf(
					"disk-stalled/log=%t,data=%t",
					affectsLogDir, affectsDataDir,
				),
				Owner:      OwnerStorage,
				MinVersion: "v19.2.0",
				Cluster:    makeClusterSpec(1),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runDiskStalledDetection(ctx, t, c, affectsLogDir, affectsDataDir)
				},
			})
		}
	}
}

func runDiskStalledDetection(
	ctx context.Context, t *test, c *cluster, affectsLogDir bool, affectsDataDir bool,
) {
	if local && runtime.GOOS != "linux" {
		t.Fatalf("must run on linux os, found %s", runtime.GOOS)
	}

	n := c.Node(1)

	c.Put(ctx, kwbase, "./kwbase")
	c.Run(ctx, n, "sudo umount -f {store-dir}/faulty || true")
	c.Run(ctx, n, "mkdir -p {store-dir}/{real,faulty} || true")
	// Make sure the actual logs are downloaded as artifacts.
	c.Run(ctx, n, "rm -f logs && ln -s {store-dir}/real/logs logs || true")

	t.Status("setting up charybdefs")

	if err := execCmd(ctx, t.l, roachprod, "install", c.makeNodes(n), "charybdefs"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, n, "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real")
	c.Run(ctx, n, "sudo mkdir -p {store-dir}/real/logs")
	c.Run(ctx, n, "sudo chmod -R 777 {store-dir}/{real,faulty}")
	l, err := t.l.ChildLogger("kwbase")
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		err error
		out string
	}
	errCh := make(chan result)

	// NB: charybdefs' delay nemesis introduces 50ms per syscall. It would
	// be nicer to introduce a longer delay, but this works.
	tooShortSync := 40 * time.Millisecond

	maxLogSync := time.Hour
	logDir := "real/logs"
	if affectsLogDir {
		logDir = "faulty/logs"
		maxLogSync = tooShortSync
	}
	maxDataSync := time.Hour
	dataDir := "real"
	if affectsDataDir {
		maxDataSync = tooShortSync
		dataDir = "faulty"
	}

	tStarted := timeutil.Now()
	dur := 10 * time.Minute
	if !affectsDataDir && !affectsLogDir {
		dur = 30 * time.Second
	}

	go func() {
		t.WorkerStatus("running server")
		out, err := c.RunWithBuffer(ctx, l, n,
			fmt.Sprintf("timeout --signal 9 %ds env KWBASE_ENGINE_MAX_SYNC_DURATION_FATAL=true "+
				"KWBASE_ENGINE_MAX_SYNC_DURATION=%s KWBASE_LOG_MAX_SYNC_DURATION=%s "+
				"./kwbase start-single-node --insecure --logtostderr=INFO --store {store-dir}/%s --log-dir {store-dir}/%s",
				int(dur.Seconds()), maxDataSync, maxLogSync, dataDir, logDir,
			),
		)
		errCh <- result{err, string(out)}
	}()

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	t.Status("blocking storage")
	c.Run(ctx, n, "charybdefs-nemesis --delay")

	res := <-errCh
	if res.err == nil {
		t.Fatalf("expected an error: %s", res.out)
	}

	// This test can also run in sanity check mode to make sure it doesn't fail
	// due to the aggressive env vars above.
	expectMsg := affectsDataDir || affectsLogDir

	if expectMsg != strings.Contains(res.out, "disk stall detected") {
		t.Fatalf("unexpected output: %v %s", res.err, res.out)
	} else if elapsed := timeutil.Since(tStarted); !expectMsg && elapsed < dur {
		t.Fatalf("no disk stall injected, but process terminated too early after %s (expected >= %s)", elapsed, dur)
	}

	c.Run(ctx, n, "charybdefs-nemesis --clear")
	c.Run(ctx, n, "sudo umount {store-dir}/faulty")
}
