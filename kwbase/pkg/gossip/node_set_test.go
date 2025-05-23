// Copyright 2015 The Cockroach Authors.
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

package gossip

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

func TestNodeSetMaxSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(1, metric.NewGauge(metric.Metadata{Name: ""}))
	if !nodes.hasSpace() {
		t.Error("set should have space")
	}
	nodes.addNode(roachpb.NodeID(1))
	if nodes.hasSpace() {
		t.Error("set should have no space")
	}
}

func TestNodeSetHasNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node := roachpb.NodeID(1)
	if nodes.hasNode(node) {
		t.Error("node wasn't added and should not be valid")
	}
	// Add node and verify it's valid.
	nodes.addNode(node)
	if !nodes.hasNode(node) {
		t.Error("empty node wasn't added and should not be valid")
	}
}

func TestNodeSetAddAndRemoveNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes.addNode(node0)
	nodes.addNode(node1)
	if !nodes.hasNode(node0) || !nodes.hasNode(node1) {
		t.Error("failed to locate added nodes")
	}
	nodes.removeNode(node0)
	if nodes.hasNode(node0) || !nodes.hasNode(node1) {
		t.Error("failed to remove node0", nodes)
	}
	nodes.removeNode(node1)
	if nodes.hasNode(node0) || nodes.hasNode(node1) {
		t.Error("failed to remove node1", nodes)
	}
}

func TestNodeSetFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes1 := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes1.addNode(node0)
	nodes1.addNode(node1)

	nodes2 := makeNodeSet(1, metric.NewGauge(metric.Metadata{Name: ""}))
	nodes2.addNode(node1)

	filtered := nodes1.filter(func(a roachpb.NodeID) bool {
		return !nodes2.hasNode(a)
	})
	if filtered.len() != 1 || filtered.hasNode(node1) || !filtered.hasNode(node0) {
		t.Errorf("expected filter to leave node0: %+v", filtered)
	}
}

func TestNodeSetAsSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := makeNodeSet(2, metric.NewGauge(metric.Metadata{Name: ""}))
	node0 := roachpb.NodeID(1)
	node1 := roachpb.NodeID(2)
	nodes.addNode(node0)
	nodes.addNode(node1)

	nodeArr := nodes.asSlice()
	if len(nodeArr) != 2 {
		t.Error("expected slice of length 2:", nodeArr)
	}
	if (nodeArr[0] != node0 && nodeArr[0] != node1) ||
		(nodeArr[1] != node1 && nodeArr[1] != node0) {
		t.Error("expected slice to contain both node0 and node1:", nodeArr)
	}
}
