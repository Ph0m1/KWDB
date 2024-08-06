// Copyright 2014 The Cockroach Authors.
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

package gossip_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip/simulation"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// The tests in this package have fairly small cluster sizes for the sake of
// not taking too long to run when run as part of the normal unit tests. If
// you're testing out gossip network behavior, you may find it useful to
// increase the network size for these tests (adjusting the max thresholds
// accordingly) and see how things behave.
const (
	testConvergenceSize        = 10
	testReachesEquilibriumSize = 24
)

func connectionsRefused(network *simulation.Network) int64 {
	var connsRefused int64
	for _, node := range network.Nodes {
		connsRefused += node.Gossip.GetNodeMetrics().ConnectionsRefused.Count()
	}
	return connsRefused
}

// TestConvergence verifies that a node gossip network converges within
// a fixed number of simulation cycles. It's really difficult to
// determine the right number for cycles because different things can
// happen during a single cycle, depending on how much CPU time is
// available. Eliminating this variability by getting more
// synchronization primitives in place for the simulation is possible,
// though two attempts so far have introduced more complexity into the
// actual production gossip code than seems worthwhile for a unittest.
// As such, the thresholds are drastically higher than is normally needed.
//
// As of Jan 2017, this normally takes ~12 cycles and 8-12 refused connections.
func TestConvergence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testutils.NightlyStress() {
		t.Skip()
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	network := simulation.NewNetwork(stopper, testConvergenceSize, true, zonepb.DefaultZoneConfigRef())

	const maxCycles = 100
	if connectedCycle := network.RunUntilFullyConnected(); connectedCycle > maxCycles {
		log.Warningf(context.Background(), "expected a fully-connected network within %d cycles; took %d",
			maxCycles, connectedCycle)
	}

	const maxConnsRefused = 50
	if connsRefused := connectionsRefused(network); connsRefused > maxConnsRefused {
		log.Warningf(context.Background(),
			"expected network to fully connect with <= %d connections refused; took %d",
			maxConnsRefused, connsRefused)
	}
}

// TestNetworkReachesEquilibrium ensures that the gossip network stops bouncing
// refused connections around after a while and settles down.
// As mentioned in the comment for TestConvergence, there is a large amount of
// variability in how much gets done in each network cycle, and thus we have
// to set thresholds that are drastically higher than is needed in the normal
// case.
//
// As of Jan 2017, this normally takes 8-9 cycles and 50-60 refused connections.
func TestNetworkReachesEquilibrium(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if testutils.NightlyStress() {
		t.Skip()
	}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	network := simulation.NewNetwork(stopper, testReachesEquilibriumSize, true, zonepb.DefaultZoneConfigRef())

	var connsRefused int64
	var cyclesWithoutChange int
	var numCycles int
	network.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
		numCycles = cycle
		newConnsRefused := connectionsRefused(network)
		if newConnsRefused > connsRefused {
			connsRefused = newConnsRefused
			cyclesWithoutChange = 0
		} else {
			cyclesWithoutChange++
		}
		if cycle%5 == 0 {
			log.Infof(context.Background(), "cycle: %d, cyclesWithoutChange: %d, fullyConnected: %v",
				cycle, cyclesWithoutChange, network.IsNetworkConnected())
		}
		return cyclesWithoutChange < 5
	})

	const maxCycles = 200
	if numCycles > maxCycles {
		log.Warningf(context.Background(), "expected a non-thrashing network within %d cycles; took %d",
			maxCycles, numCycles)
	}

	const maxConnsRefused = 500
	if connsRefused > maxConnsRefused {
		log.Warningf(context.Background(),
			"expected thrashing to die down with <= %d connections refused; took %d",
			maxConnsRefused, connsRefused)
	}
}
