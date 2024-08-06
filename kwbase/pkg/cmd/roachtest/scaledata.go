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
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/binfetcher"
	"github.com/cockroachdb/errors"
)

func registerScaleData(r *testRegistry) {
	// apps is a suite of Sqlapp applications designed to be used to check the
	// consistency of a database under load. Each Sqlapp application launches a
	// set of workers who perform database operations while another worker
	// periodically checks invariants to capture any inconsistencies. The
	// application suite has been pulled from:
	// github.com/scaledata/rksql/tree/master/src/go/src/rubrik/sqlapp
	//
	// The map provides a mapping between application name and command-line
	// flags unique to that application.
	apps := map[string]string{
		"distributed-semaphore": "",
		"filesystem-simulator":  "",
		"job-coordinator":       "--num_jobs_per_worker=8 --job_period_scale_millis=100",
	}

	for app, flags := range apps {
		app, flags := app, flags // copy loop iterator vars
		const duration = 10 * time.Minute
		for _, n := range []int{3, 6} {
			var skip, skipDetail string
			if app == "job-coordinator" {
				skip = "skipping flaky scaledata/job-coordinator test"
				skipDetail = "work underway to deflake https://gitee.com/kwbasedb/kwbase/issues/51765"
			}
			r.Add(testSpec{
				Name:        fmt.Sprintf("scaledata/%s/nodes=%d", app, n),
				Owner:       OwnerKV,
				Timeout:     2 * duration,
				Cluster:     makeClusterSpec(n + 1),
				Skip:        skip,
				SkipDetails: skipDetail,
				Run: func(ctx context.Context, t *test, c *cluster) {
					runSqlapp(ctx, t, c, app, flags, duration)
				},
			})
		}
	}
}

func runSqlapp(ctx context.Context, t *test, c *cluster, app, flags string, dur time.Duration) {
	roachNodeCount := c.spec.NodeCount - 1
	roachNodes := c.Range(1, roachNodeCount)
	appNode := c.Node(c.spec.NodeCount)

	if local {
		appBinary, err := findBinary("", app)
		if err != nil {
			err = errors.WithHint(err,
				"place binaries built from kwbasedb/rksql in repo root, or add to $PATH")
			t.Fatal(err)
		}
		c.Put(ctx, appBinary, app, appNode)
	} else {
		b, err := binfetcher.Download(ctx, binfetcher.Options{
			Component: "rubrik",
			Binary:    app,
			Version:   "LATEST",
			GOOS:      "linux",
			GOARCH:    "amd64",
		})
		if err != nil {
			t.Fatal(err)
		}

		c.Put(ctx, b, app, appNode)
	}
	c.Put(ctx, kwbase, "./kwbase", roachNodes)
	c.Start(ctx, t, roachNodes)

	// TODO(nvanbenschoten): We are currently running these consistency checks with
	// basic chaos. We should also run them in more chaotic environments which
	// could introduce network partitions, ENOSPACE, clock issues, etc.

	// Sqlapps each take a `--kwbase_ip_addresses_csv` flag, which is a
	// comma-separated list of node IP addresses with optional port specifiers.
	addrStr := strings.Join(c.InternalAddr(ctx, c.Range(1, roachNodeCount)), ",")

	m := newMonitor(ctx, c, roachNodes)
	{
		// Kill one node at a time, with a minute of healthy cluster and thirty
		// seconds of down node.
		ch := Chaos{
			Timer:   Periodic{Period: 90 * time.Second, DownTime: 30 * time.Second},
			Target:  roachNodes.randNode,
			Stopper: time.After(dur),
		}
		m.Go(ch.Runner(c, m))
	}
	m.Go(func(ctx context.Context) error {
		t.Status("installing schema")
		err := c.RunE(ctx, appNode, fmt.Sprintf("./%s --install_schema "+
			"--kwbase_ip_addresses_csv='%s' %s", app, addrStr, flags))
		if err != nil {
			return err
		}

		t.Status("running consistency checker")
		const workers = 16
		return c.RunE(ctx, appNode, fmt.Sprintf("./%s  --duration_secs=%d "+
			"--num_workers=%d --kwbase_ip_addresses_csv='%s' %s",
			app, int(dur.Seconds()), workers, addrStr, flags))
	})
	m.Wait()
}
