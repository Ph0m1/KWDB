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
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/ts/tspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// tpccOlapQuery is a contrived query that seems to do serious damage to a
// cluster. The query itself is a hash join with a selective filter and a
// limited sort.
const tpccOlapQuery = `SELECT
    i_id, s_w_id, s_quantity, i_price
FROM
    stock JOIN item ON s_i_id = i_id
WHERE
    s_quantity < 100 AND i_price > 90
ORDER BY
    i_price DESC, s_quantity ASC
LIMIT
    100;`

type tpccOLAPSpec struct {
	Nodes       int
	CPUs        int
	Warehouses  int
	Concurrency int
}

func (s tpccOLAPSpec) run(ctx context.Context, t *test, c *cluster) {
	kwdbNodes, workloadNode := setupTPCC(
		ctx, t, c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport,
		})
	const queryFileName = "queries.sql"
	// querybench expects the entire query to be on a single line.
	queryLine := `"` + strings.Replace(tpccOlapQuery, "\n", " ", -1) + `"`
	c.Run(ctx, workloadNode, "echo", queryLine, "> "+queryFileName)
	t.Status("waiting")
	m := newMonitor(ctx, c, kwdbNodes)
	rampDuration := 2 * time.Minute
	duration := 3 * time.Minute
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running querybench")
		cmd := fmt.Sprintf(
			"./workload run querybench --db tpcc"+
				" --tolerate-errors=t"+
				" --concurrency=%d"+
				" --query-file %s"+
				" --histograms="+perfArtifactsDir+"/stats.json "+
				" --ramp=%s --duration=%s {pgurl:1-%d}",
			s.Concurrency, queryFileName, rampDuration, duration, c.spec.NodeCount-1)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()
	verifyNodeLiveness(ctx, c, t, duration)
}

// Check that node liveness did not fail more than maxFailures times across
// all of the nodes.
func verifyNodeLiveness(ctx context.Context, c *cluster, t *test, runDuration time.Duration) {
	const maxFailures = 10
	adminURLs := c.ExternalAdminUIAddr(ctx, c.Node(1))
	now := timeutil.Now()
	var response tspb.TimeSeriesQueryResponse
	// Retry because timeseries queries can fail if the underlying inter-node
	// connections are in a failed state which can happen due to overload.
	// Now that the load has stopped, this should resolve itself soon.
	// Even with 60 retries we'll at most spend 30s attempting to fetch
	// the metrics.
	if err := retry.WithMaxAttempts(ctx, retry.Options{
		MaxBackoff: 500 * time.Millisecond,
	}, 60, func() (err error) {
		response, err = getMetrics(adminURLs[0], now.Add(-runDuration), now, []tsQuery{
			{
				name:      "cr.node.liveness.heartbeatfailures",
				queryType: total,
			},
		})
		return err
	}); err != nil {
		t.Fatalf("failed to fetch liveness metrics: %v", err)
	}
	if len(response.Results[0].Datapoints) <= 1 {
		t.Fatalf("not enough datapoints in timeseries query response: %+v", response)
	}
	datapoints := response.Results[0].Datapoints
	finalCount := int(datapoints[len(datapoints)-1].Value)
	initialCount := int(datapoints[0].Value)
	if failures := finalCount - initialCount; failures > maxFailures {
		t.Fatalf("Node liveness failed %d times, expected no more than %d",
			failures, maxFailures)
	} else {
		t.logger().Printf("Node liveness failed %d times which is fewer than %d",
			failures, maxFailures)
	}
}

func registerTPCCOverloadSpec(r *testRegistry, s tpccOLAPSpec) {
	name := fmt.Sprintf("overload/tpcc_olap/nodes=%d/cpu=%d/w=%d/c=%d",
		s.Nodes, s.CPUs, s.Warehouses, s.Concurrency)
	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerKV,
		Cluster:    makeClusterSpec(s.Nodes+1, cpu(s.CPUs)),
		Run:        s.run,
		MinVersion: "v19.2.0",
		Timeout:    20 * time.Minute,
	})
}

func registerOverload(r *testRegistry) {
	specs := []tpccOLAPSpec{
		{
			CPUs:        8,
			Concurrency: 96,
			Nodes:       3,
			Warehouses:  50,
		},
	}
	for _, s := range specs {
		registerTPCCOverloadSpec(r, s)
	}
}
