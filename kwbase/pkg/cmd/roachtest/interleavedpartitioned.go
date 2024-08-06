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
)

func registerInterleaved(r *testRegistry) {
	type config struct {
		eastName        string
		westName        string
		centralName     string
		initSessions    int
		insertPercent   int
		retrievePercent int
		updatePercent   int
		localPercent    int
		rowsPerDelete   int
	}

	runInterleaved := func(
		ctx context.Context,
		t *test,
		c *cluster,
		config config,
	) {
		numZones, numRoachNodes, numLoadNodes := 3, 9, 3
		loadGroups := makeLoadGroups(c, numZones, numRoachNodes, numLoadNodes)
		kwbaseWest := loadGroups[0].roachNodes
		workloadWest := loadGroups[0].loadNodes
		kwbaseEast := loadGroups[1].roachNodes
		workloadEast := loadGroups[1].loadNodes
		kwbaseCentral := loadGroups[2].roachNodes
		workloadCentral := loadGroups[2].loadNodes
		kwbaseNodes := loadGroups.roachNodes()
		workloadNodes := loadGroups.loadNodes()

		c.l.Printf("kwbase nodes: %s", kwbaseNodes.String()[1:])
		c.l.Printf("workload nodes: %s", workloadNodes.String()[1:])

		c.Put(ctx, kwbase, "./kwbase", c.All())
		c.Put(ctx, workload, "./workload", c.All())
		c.Start(ctx, t, kwbaseNodes)

		zones := fmt.Sprintf("--east-zone-name %s --west-zone-name %s --central-zone-name %s",
			config.eastName, config.westName, config.centralName)

		cmdInit := fmt.Sprintf("./workload init interleavedpartitioned %s --drop "+
			"--locality east --init-sessions %d",
			zones,
			config.initSessions,
		)

		t.Status("initializing workload")

		// Always init on an east node.
		c.Run(ctx, kwbaseEast.randNode(), cmdInit)

		duration := " --duration " + ifLocal("10s", "10m")
		histograms := " --histograms=" + perfArtifactsDir + "/stats.json"

		createCmd := func(locality string, kwbaseNodes nodeListOption) string {
			return fmt.Sprintf(
				"./workload run interleavedpartitioned %s --locality %s "+
					"--insert-percent %d --insert-local-percent %d "+
					"--retrieve-percent %d --retrieve-local-percent %d "+
					"--update-percent %d --update-local-percent %d "+
					"%s %s {pgurl%s}",
				zones,
				locality,
				config.insertPercent,
				config.localPercent,
				config.retrievePercent,
				config.localPercent,
				config.updatePercent,
				config.localPercent,
				duration,
				histograms,
				kwbaseNodes,
			)
		}

		cmdCentral := fmt.Sprintf(
			"./workload run interleavedpartitioned %s "+
				"--locality central --rows-per-delete %d "+
				"%s %s {pgurl%s}",
			zones,
			config.rowsPerDelete,
			duration,
			histograms,
			kwbaseCentral,
		)

		t.Status("running workload")
		m := newMonitor(ctx, c, kwbaseNodes)

		runLocality := func(node nodeListOption, cmd string) {
			m.Go(func(ctx context.Context) error {
				return c.RunE(ctx, node, cmd)
			})
		}

		runLocality(workloadWest, createCmd("west", kwbaseWest))
		runLocality(workloadEast, createCmd("east", kwbaseEast))
		runLocality(workloadCentral, cmdCentral)

		m.Wait()
	}

	r.Add(testSpec{
		Name:    "interleavedpartitioned",
		Owner:   OwnerPartitioning,
		Cluster: makeClusterSpec(12, geo(), zones("us-east1-b,us-west1-b,europe-west2-b")),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runInterleaved(ctx, t, c,
				config{
					eastName:        `europe-west2-b`,
					westName:        `us-west1-b`,
					centralName:     `us-east1-b`, // us-east is central between us-west and eu-west
					initSessions:    1000,
					insertPercent:   80,
					retrievePercent: 10,
					updatePercent:   10,
					rowsPerDelete:   20,
				},
			)
		},
	})
}
