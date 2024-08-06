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

package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/status/statuspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	yaml "gopkg.in/yaml.v2"
)

func TestValidateNoRepeatKeysInZone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		constraint    string
		expectSuccess bool
	}{
		{`constraints: ["+region=us-east-1"]`, true},
		{`constraints: ["+region=us-east-1", "+zone=pa"]`, true},
		{`constraints: ["+region=us-east-1", "-region=us-west-1"]`, true},
		{`constraints: ["+region=us-east-1", "+region=us-east-2"]`, false},
		{`constraints: ["+region=us-east-1", "+zone=pa", "+region=us-west-1"]`, false},
		{`constraints: ["+region=us-east-1", "-region=us-east-1"]`, false},
		{`constraints: ["-region=us-east-1", "+region=us-east-1"]`, false},
		{`constraints: {"+region=us-east-1":2, "+region=us-east-2":2}`, true},
		{`constraints: {"+region=us-east-1,+region=us-west-1":2, "+region=us-east-2":2}`, false},
		{`constraints: ["+x1", "+x2", "+x3"]`, true},
		{`constraints: ["+x1", "+x1"]`, false},
		{`constraints: ["+x1", "-x1"]`, false},
		{`constraints: ["-x1", "+x1"]`, false},
	}

	for _, tc := range testCases {
		var zone zonepb.ZoneConfig
		err := yaml.UnmarshalStrict([]byte(tc.constraint), &zone)
		if err != nil {
			t.Fatal(err)
		}
		err = validateNoRepeatKeysInZone(&zone)
		if err != nil && tc.expectSuccess {
			t.Errorf("expected success for %q; got %v", tc.constraint, err)
		} else if err == nil && !tc.expectSuccess {
			t.Errorf("expected err for %q; got success", tc.constraint)
		}
	}
}

func TestValidateZoneAttrsAndLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stores := []struct {
		nodeAttrs     []string
		storeAttrs    []string
		storeLocality []roachpb.Tier
	}{
		{
			nodeAttrs:  []string{"highcpu", "highmem"},
			storeAttrs: []string{"ssd"},
			storeLocality: []roachpb.Tier{
				{Key: "geo", Value: "us"},
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "us-east1-b"},
			},
		},
		{
			nodeAttrs:  []string{"lowcpu", "lowmem"},
			storeAttrs: []string{"hdd"},
			storeLocality: []roachpb.Tier{
				{Key: "geo", Value: "eu"},
				{Key: "region", Value: "eu-west1"},
				{Key: "zone", Value: "eu-west1-b"},
				{Key: "rack", Value: "17"},
			},
		},
	}

	genStatusFromStore := func(nodeAttrs []string, storeAttrs []string, storeLocality []roachpb.Tier) statuspb.NodeStatus {
		return statuspb.NodeStatus{
			StoreStatuses: []statuspb.StoreStatus{
				{
					Desc: roachpb.StoreDescriptor{
						Attrs: roachpb.Attributes{
							Attrs: storeAttrs,
						},
						Node: roachpb.NodeDescriptor{
							Attrs: roachpb.Attributes{
								Attrs: nodeAttrs,
							},
							Locality: roachpb.Locality{
								Tiers: storeLocality,
							},
						},
					},
				},
			},
		}
	}

	nodes := &serverpb.NodesResponse{}
	for _, store := range stores {
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore(store.nodeAttrs, store.storeAttrs, store.storeLocality))
	}

	// Different cluster settings to test validation behavior.
	getNodes := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		return nodes, nil
	}

	// Regressions for negative constraint validation
	singleAttrNode := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		nodes := &serverpb.NodesResponse{}
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore([]string{}, []string{"ssd"}, []roachpb.Tier{}))
		return nodes, nil
	}
	singleLocalityNode := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		nodes := &serverpb.NodesResponse{}
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore([]string{}, []string{}, []roachpb.Tier{{Key: "region", Value: "us-east1"}}))
		return nodes, nil
	}

	const expectSuccess = 0
	const expectParseErr = 1
	const expectValidateErr = 2
	for i, tc := range []struct {
		cfg       string
		expectErr int
		nodes     nodeGetter
	}{
		{`nonsense`, expectParseErr, getNodes},
		{`range_max_bytes: 100`, expectSuccess, getNodes},
		{`range_max_byte: 100`, expectParseErr, getNodes},
		{`constraints: ["+region=us-east1"]`, expectSuccess, getNodes},
		{`constraints: {"+region=us-east1": 2, "+region=eu-west1": 1}`, expectSuccess, getNodes},
		{`constraints: ["+region=us-eas1"]`, expectValidateErr, getNodes},
		{`constraints: {"+region=us-eas1": 2, "+region=eu-west1": 1}`, expectValidateErr, getNodes},
		{`constraints: {"+region=us-east1": 2, "+region=eu-wes1": 1}`, expectValidateErr, getNodes},
		{`constraints: ["+regio=us-east1"]`, expectValidateErr, getNodes},
		{`constraints: ["+rack=17"]`, expectSuccess, getNodes},
		{`constraints: ["+rack=18"]`, expectValidateErr, getNodes},
		{`constraints: ["+rach=17"]`, expectValidateErr, getNodes},
		{`constraints: ["+highcpu"]`, expectSuccess, getNodes},
		{`constraints: ["+lowmem"]`, expectSuccess, getNodes},
		{`constraints: ["+ssd"]`, expectSuccess, getNodes},
		{`constraints: ["+highcp"]`, expectValidateErr, getNodes},
		{`constraints: ["+owmem"]`, expectValidateErr, getNodes},
		{`constraints: ["+sssd"]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectSuccess, getNodes},
		{`lease_preferences: [["+region=us-eat1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+foo"], ["+geo=us", "+highcpu"]]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+bar"]]`, expectValidateErr, getNodes},
		{`constraints: ["-region=us-east1"]`, expectSuccess, singleLocalityNode},
		{`constraints: ["-ssd"]`, expectSuccess, singleAttrNode},
		{`constraints: ["-regio=us-eas1"]`, expectSuccess, getNodes},
		{`constraints: {"-region=us-eas1": 2, "-region=eu-wes1": 1}`, expectSuccess, getNodes},
		{`constraints: ["-foo=bar"]`, expectSuccess, getNodes},
		{`constraints: ["-highcpu"]`, expectSuccess, getNodes},
		{`constraints: ["-ssd"]`, expectSuccess, getNodes},
		{`constraints: ["-fake"]`, expectSuccess, getNodes},
	} {
		var zone zonepb.ZoneConfig
		err := yaml.UnmarshalStrict([]byte(tc.cfg), &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Fatalf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectParseErr {
			t.Fatalf("#%d: expected parse err for %q; got success", i, tc.cfg)
		}

		err = validateZoneAttrsAndLocalities(context.Background(), tc.nodes, &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Errorf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectValidateErr {
			t.Errorf("#%d: expected err for %q; got success", i, tc.cfg)
		}
	}
}
