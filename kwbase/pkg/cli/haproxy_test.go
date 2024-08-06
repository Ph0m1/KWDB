// Copyright 2017 The Cockroach Authors.
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

package cli

import (
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/status/statuspb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestNodeStatusToNodeInfoConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input    serverpb.NodesResponse
		expected []haProxyNodeInfo
	}{
		{
			serverpb.NodesResponse{Nodes: []statuspb.NodeStatus{
				{
					Desc: roachpb.NodeDescriptor{
						NodeID: 1,
						Address: util.UnresolvedAddr{
							AddressField: "addr",
						},
					},
					// Flags but no http port.
					Args: []string{"--unwanted", "-unwanted"},
				},
			}},
			[]haProxyNodeInfo{
				{
					NodeID:    1,
					NodeAddr:  "addr",
					CheckPort: base.DefaultHTTPPort,
				},
			},
		},
		{
			serverpb.NodesResponse{Nodes: []statuspb.NodeStatus{
				{
					Desc: roachpb.NodeDescriptor{NodeID: 1},
					Args: []string{"--unwanted", "--http-port=1234"},
				},
				{
					Desc: roachpb.NodeDescriptor{NodeID: 2},
					Args: nil,
				},
				{
					Desc: roachpb.NodeDescriptor{NodeID: 3},
					Args: []string{"--http-addr=foo:4567"},
				},
			}},
			[]haProxyNodeInfo{
				{
					NodeID:    1,
					CheckPort: "1234",
				},
				{
					NodeID:    2,
					CheckPort: base.DefaultHTTPPort,
				},
				{
					NodeID:    3,
					CheckPort: "4567",
				},
			},
		},
		{
			serverpb.NodesResponse{Nodes: []statuspb.NodeStatus{
				{
					Desc: roachpb.NodeDescriptor{NodeID: 1},
					Args: []string{"--http-port", "5678", "--unwanted"},
				},
			}},
			[]haProxyNodeInfo{
				{
					NodeID:    1,
					CheckPort: "5678",
				},
			},
		},
		{
			serverpb.NodesResponse{Nodes: []statuspb.NodeStatus{
				{
					Desc: roachpb.NodeDescriptor{NodeID: 1},
					// We shouldn't see this, because the flag needs an argument on startup,
					// but check that we fall back to the default port.
					Args: []string{"-http-port"},
				},
			}},
			[]haProxyNodeInfo{
				{
					NodeID:    1,
					CheckPort: base.DefaultHTTPPort,
				},
			},
		},
		// Check that decommission{ing,ed} nodes are not considered for
		// generating the configuration.
		{
			serverpb.NodesResponse{
				Nodes: []statuspb.NodeStatus{
					{Desc: roachpb.NodeDescriptor{NodeID: 1}},
					{Desc: roachpb.NodeDescriptor{NodeID: 2}},
					{Desc: roachpb.NodeDescriptor{NodeID: 3}},
					{Desc: roachpb.NodeDescriptor{NodeID: 4}},
					{Desc: roachpb.NodeDescriptor{NodeID: 5}},
					{Desc: roachpb.NodeDescriptor{NodeID: 6}},
				},
				LivenessByNodeID: map[roachpb.NodeID]storagepb.NodeLivenessStatus{
					1: storagepb.NodeLivenessStatus_DEAD,
					2: storagepb.NodeLivenessStatus_DECOMMISSIONING,
					3: storagepb.NodeLivenessStatus_UNKNOWN,
					4: storagepb.NodeLivenessStatus_UNAVAILABLE,
					5: storagepb.NodeLivenessStatus_LIVE,
					6: storagepb.NodeLivenessStatus_DECOMMISSIONED,
				},
			},
			[]haProxyNodeInfo{
				{NodeID: 1, CheckPort: base.DefaultHTTPPort},
				// Node 2 is decommissioning.
				{NodeID: 3, CheckPort: base.DefaultHTTPPort},
				{NodeID: 4, CheckPort: base.DefaultHTTPPort},
				{NodeID: 5, CheckPort: base.DefaultHTTPPort},
				// Node 6 is decommissioned.
			},
		},
	}

	for i, testCase := range testCases {
		output := nodeStatusesToNodeInfos(&testCase.input)
		if !reflect.DeepEqual(output, testCase.expected) {
			t.Fatalf("test %d: unexpected output %v, expected %v", i, output, testCase.expected)
		}
	}
}

func TestMatchLocalityRegexp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		locality string // The locality as passed to `start --locality=xx`
		desired  string // The desired locality as passed to `gen haproxy --locality=xx`
		matches  bool
	}{
		{"", "", true},
		{"region=us-east1", "", true},
		{"country=us,region=us-east1,datacenter=blah", "", true},
		{"country=us,region=us-east1,datacenter=blah", "country=us", true},
		{"country=us,region=us-east1,datacenter=blah", "count.*=us", false},
		{"country=us,region=us-east1,datacenter=blah", "country=u", false},
		{"country=us,region=us-east1,datacenter=blah", "try=us", false},
		{"country=us,region=us-east1,datacenter=blah", "region=us-east1", true},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*", true},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*,country=us", true},
		{"country=us,region=us-east1,datacenter=blah", "region=notus", false},
		{"country=us,region=us-east1,datacenter=blah", "something=else", false},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*,zone=foo", false},
	}

	for testNum, testCase := range testCases {
		// We're not testing locality parsing: fail on error.
		var locality, desired roachpb.Locality
		if testCase.locality != "" {
			if err := locality.Set(testCase.locality); err != nil {
				t.Fatal(err)
			}
		}
		if testCase.desired != "" {
			if err := desired.Set(testCase.desired); err != nil {
				t.Fatal(err)
			}
		}
		matches, _ := localityMatches(locality, desired)
		if matches != testCase.matches {
			t.Errorf("#%d: expected match %t, got %t", testNum, testCase.matches, matches)
		}
	}
}
