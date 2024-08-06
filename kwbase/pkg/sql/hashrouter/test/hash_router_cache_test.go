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

package test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHashInfoOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		ModeFlag:   3,
		StoreSpecs: []base.StoreSpec{{Path: dir}},
	})
	defer s.Stopper().Stop(context.TODO())

	ctx := context.Background()

	mr := s.GetHashRouterManger().(*hashrouter.HRManager)

	partition1 := make(map[uint32]api.HashPartition)
	partition1[1] = api.HashPartition{
		StartPoint: 1,
		EndPoint:   100,
	}
	hashRouting1 := &api.KWDBHashRouting{
		EntityRangeGroupId: 114,
		TableID:            114514,
		EntityRangeGroup: api.EntityRangeGroup{
			GroupID:             1,
			Partitions:          partition1,
			LeaseHolder:         api.EntityRangeGroupReplica{NodeID: 1},
			InternalReplicas:    []api.EntityRangeGroupReplica{{NodeID: 1}, {NodeID: 2}},
			GroupChanges:        nil,
			LeaseHolderChange:   api.EntityRangeGroupReplica{},
			PreviousLeaseHolder: api.EntityRangeGroupReplica{},
			PartitionChanges:    nil,
			Status:              api.EntityRangeGroupStatus_Available,
		},
		TsPartitionSize: 100,
	}

	partition2 := make(map[uint32]api.HashPartition)
	partition2[1] = api.HashPartition{
		StartPoint: 100,
		EndPoint:   200,
	}
	hashRouting2 := &api.KWDBHashRouting{
		EntityRangeGroupId: 514,
		TableID:            114514,
		EntityRangeGroup: api.EntityRangeGroup{
			GroupID:             2,
			Partitions:          partition2,
			LeaseHolder:         api.EntityRangeGroupReplica{NodeID: 2},
			InternalReplicas:    []api.EntityRangeGroupReplica{{NodeID: 1}, {NodeID: 2}},
			GroupChanges:        nil,
			LeaseHolderChange:   api.EntityRangeGroupReplica{},
			PreviousLeaseHolder: api.EntityRangeGroupReplica{},
			PartitionChanges:    nil,
			Status:              api.EntityRangeGroupStatus_Available,
		},
		TsPartitionSize: 100,
	}

	hashRoutings := []api.KWDBHashRouting{*hashRouting1, *hashRouting2}
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return mr.PutHashInfo(ctx, txn, hashRoutings)
	}); err != nil {
		t.Error(err)
	}
	// GetHashRoutingsByTableID
	routings, err := mr.GetHashRoutingsByTableID(ctx, nil, 114514)
	if err != nil {
		t.Error(err)
	}
	require.Equal(t, 2, len(routings))
	if routings[0].EntityRangeGroupId == hashRouting1.EntityRangeGroupId {
		require.True(t, hashRouting1.Equal(routings[0]))
		require.True(t, hashRouting2.Equal(routings[1]))
	} else {
		require.True(t, hashRouting1.Equal(routings[1]))
		require.True(t, hashRouting2.Equal(routings[0]))
	}

	// GetHashRoutingByID
	routing, err := mr.GetHashRoutingByID(ctx, nil, 114)
	if err != nil {
		t.Error(err)
	}
	require.True(t, hashRouting1.Equal(routing))

	// GetAllHashRoutings
	routings, err = mr.GetAllHashRoutings(ctx, nil)
	if err != nil {
		t.Error(err)
	}
	require.Equal(t, 2, len(routings))
	if routings[0].EntityRangeGroupId == hashRouting1.EntityRangeGroupId {
		require.True(t, hashRouting1.Equal(routings[0]))
		require.True(t, hashRouting2.Equal(routings[1]))
	} else {
		require.True(t, hashRouting1.Equal(routings[1]))
		require.True(t, hashRouting2.Equal(routings[0]))
	}
	// DeleteHashRoutingByID
	err = mr.DeleteHashRoutingByTableID(ctx, nil, 114514)
	if err != nil {
		t.Error(err)
	}
	// GetHashRoutingsByTableID
	routings, err = mr.GetHashRoutingsByTableID(ctx, nil, 114514)
	if err != nil {
		t.Error(err)
	}
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return mr.PutHashInfo(ctx, txn, hashRoutings)
	}); err != nil {
		t.Error(err)
	}

	// DeleteHashRoutingByID
	err = mr.DeleteHashRoutingByID(ctx, nil, 114)
	if err != nil {
		t.Error(err)
	}
	routings, err = mr.GetAllHashRoutings(ctx, nil)
	if err != nil {
		t.Error(err)
	}
	require.Equal(t, 1, len(routings))
	require.Equal(t, true, hashRouting2.Equal(routings[0]))

	// DeleteHashRoutingByID
	err = mr.DeleteHashRoutingByTableID(ctx, nil, 114514)
	if err != nil {
		t.Error(err)
	}
	if err != nil {
		t.Error(err)
	}
	routings, err = mr.GetAllHashRoutings(ctx, nil)
	if err != nil {
		t.Error(err)
	}
	require.Equal(t, 0, len(routings))
}
