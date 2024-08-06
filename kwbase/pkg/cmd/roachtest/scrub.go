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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func registerScrubIndexOnlyTPCC(r *testRegistry) {
	// numScrubRuns is set assuming a single SCRUB run (index only) takes ~1 min
	r.Add(makeScrubTPCCTest(5, 100, 30*time.Minute, "index-only", 20))
}

func registerScrubAllChecksTPCC(r *testRegistry) {
	// numScrubRuns is set assuming a single SCRUB run (all checks) takes ~2 min
	r.Add(makeScrubTPCCTest(5, 100, 30*time.Minute, "all-checks", 10))
}

func makeScrubTPCCTest(
	numNodes, warehouses int, length time.Duration, optionName string, numScrubRuns int,
) testSpec {
	var stmtOptions string
	// SCRUB checks are run at -1m to avoid contention with TPCC traffic.
	// By the time the SCRUB queries start, the tables will have been loaded for
	// some time (since it takes some time to run the TPCC consistency checks),
	// so using a timestamp in the past is fine.
	switch optionName {
	case "index-only":
		stmtOptions = `AS OF SYSTEM TIME '-1m' WITH OPTIONS INDEX ALL`
	case "all-checks":
		stmtOptions = `AS OF SYSTEM TIME '-1m'`
	default:
		panic(fmt.Sprintf("Not a valid option: %s", optionName))
	}

	return testSpec{
		Name:    fmt.Sprintf("scrub/%s/tpcc/w=%d", optionName, warehouses),
		Owner:   OwnerSQLExec,
		Cluster: makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:   warehouses,
				ExtraRunArgs: "--wait=false --tolerate-errors",
				During: func(ctx context.Context) error {
					if !c.isLocal() {
						// Wait until tpcc has been running for a few minutes to start SCRUB checks
						sleepInterval := time.Minute * 10
						maxSleep := length / 2
						if sleepInterval > maxSleep {
							sleepInterval = maxSleep
						}
						time.Sleep(sleepInterval)
					}

					conn := c.Conn(ctx, 1)
					defer conn.Close()

					c.l.Printf("Starting %d SCRUB checks", numScrubRuns)
					for i := 0; i < numScrubRuns; i++ {
						c.l.Printf("Running SCRUB check %d\n", i+1)
						before := timeutil.Now()
						err := sqlutils.RunScrubWithOptions(conn, "tpcc", "order", stmtOptions)
						c.l.Printf("SCRUB check %d took %v\n", i+1, timeutil.Since(before))

						if err != nil {
							t.Fatal(err)
						}
					}
					return nil
				},
				Duration:  length,
				SetupType: usingImport,
			})
		},
		MinVersion: "v19.1.0",
	}
}
