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

package api

import (
	"context"
	"hash/fnv"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// GetHashRouterManagerWithTxn get the HashRouterManager object with txn
var GetHashRouterManagerWithTxn func(ctx context.Context, txn *kv.Txn) (HashRouterManager, error)

// GetHashRouterManagerWithCache get the HashRouterManager object from cache
var GetHashRouterManagerWithCache func() HashRouterManager

// EntityRangeGroupChange has a routing and ha message,
// todo: Design a better structure in the future and
//       remove the built-in GroupChange field.
type EntityRangeGroupChange struct {
	// Routing is the calculated routing
	Routing KWDBHashRouting
	// Messages is the calculated change messages
	Messages []EntityRangePartitionMessage
}

// GetDistributeInfo get the partitions on create table
var GetDistributeInfo func(ctx context.Context, tableID uint32) ([]HashPartition, error)

// PreDistributeBySingleReplica is used on StartSingleReplica to get pre distribute
var PreDistributeBySingleReplica func(ctx context.Context, txn *kv.Txn, tableID uint32, partitions []HashPartition) ([]roachpb.ReplicaDescriptor, error)

// HashRouterManager the interface of ha manager
type HashRouterManager interface {
	// IsNodeUpgrading the replica change when node upgrading
	IsNodeUpgrading(ctx context.Context, nodeID roachpb.NodeID, tableID uint32) ([]EntityRangeGroupChange, error)
	// NodeRecover the replica change when unhealthy node rejoin
	NodeRecover(ctx context.Context, nodeID roachpb.NodeID, tableID uint32) ([]EntityRangeGroupChange, error)
	// RefreshHashRouterWithSingleGroup calculate the new group distribute from groupChange and write to disk
	RefreshHashRouterWithSingleGroup(ctx context.Context, tableID uint32, txn *kv.Txn, msg string, groupChange EntityRangeGroupChange) error
	// RefreshHashRouterForGroups calculate the new groups distribute and write to disk without groupChange
	RefreshHashRouterForGroups(ctx context.Context, tableID uint32, txn *kv.Txn, msg string, nodeStatus storagepb.NodeLivenessStatus, groups map[EntityRangeGroupID]struct{}) error
	// DropTableHashInfo drop the table distribute from the disk
	DropTableHashInfo(ctx context.Context, txn *kv.Txn, tableID uint32) error
	// GetAllHashRouterInfo get all table distribute from disk
	GetAllHashRouterInfo(ctx context.Context, txn *kv.Txn) (map[uint32]HashRouter, error)
	// GetHashInfoByTableID get the table groups distribute
	GetHashInfoByTableID(ctx context.Context, tableID uint32) HashRouter

	PutSingleHashInfoWithLock(ctx context.Context, tableID uint32, txn *kv.Txn, routing KWDBHashRouting) (err error)
}

// GetHashRouterWithTable read the table distribute from disk return the hashrouter object
var GetHashRouterWithTable func(databaseID uint32, tableID uint32, isCreateTable bool, txn *kv.Txn) (HashRouter, error)

// HashRouter the interface of hashrouter
type HashRouter interface {
	// GetNodeIDByPrimaryTag get the leaseHolder node by primaryTag
	GetNodeIDByPrimaryTag(ctx context.Context, primaryTags ...[]byte) ([]roachpb.NodeID, error)
	// GetPartitionByPoint get hashPartition by hashPoint
	GetPartitionByPoint(ctx context.Context, point HashPoint) (HashPartition, error)
	// GetGroupIDByPrimaryTag get the leaseHolder groupID by primaryTag
	GetGroupIDByPrimaryTag(ctx context.Context, primaryTag []byte) (EntityRangeGroupID, error)
	// GetLeaseHolderNodeIDs get this table leaseHolder NodeID
	GetLeaseHolderNodeIDs(ctx context.Context, isMppMode bool) ([]roachpb.NodeID, error)
	// GetHashPartitions get all group distribute
	GetHashPartitions(ctx context.Context) map[EntityRangeGroupID]*EntityRangeGroup
	// GetGroupIDAndRoleOnNode get groupID and role on nodeID
	GetGroupIDAndRoleOnNode(ctx context.Context, nodeID roachpb.NodeID) []RangeGroup
	// GetGroupsOnNode get all groups on nodeID
	GetGroupsOnNode(ctx context.Context, nodeID roachpb.NodeID) []EntityRangeGroup
	// GetNodeDistributeByHashPoint get the HashPoint leaseHolder and follower
	GetNodeDistributeByHashPoint(ctx context.Context, point HashPoint) (roachpb.NodeID, []roachpb.NodeID, error)
	// GetGroupByHashPoint get the group by HashPoint
	GetGroupByHashPoint(ctx context.Context, point HashPoint) (*EntityRangeGroup, error)
	// GetAllGroups get all groups on this table
	GetAllGroups(ctx context.Context) []EntityRangeGroup
	// RebalancedReplica rebalance this table replica
	RebalancedReplica(ctx context.Context, txn *kv.Txn, tableID uint32) ([]EntityRangePartitionMessage, error)
}

// GetHashPointByPrimaryTag get hashPoint by primaryTag
func GetHashPointByPrimaryTag(primaryTags ...[]byte) ([]HashPoint, error) {
	fnv32 := fnv.New32()
	var hashPoints []HashPoint
	for _, primaryTag := range primaryTags {
		_, err := fnv32.Write(primaryTag)
		if err != nil {
			return nil, err
		}
		hashPoints = append(hashPoints, HashPoint(fnv32.Sum32()%HashParamV2))
		log.Eventf(context.TODO(), "hashID: +%v , primaryTag: +%v", HashPoint(fnv32.Sum32()%HashParamV2), primaryTag)
		fnv32.Reset()
	}

	return hashPoints, nil
}

// GetHealthyNodeIDs get all healthy nodes
var GetHealthyNodeIDs func(ctx context.Context) ([]roachpb.NodeID, error)

// GetTableNodeIDs get all healthy nodes
var GetTableNodeIDs func(ctx context.Context, txn *kv.Txn, tableID uint32) ([]roachpb.NodeID, error)

// RemoveUnusedTSRangeGroups remove unused range groups of target table from the target node.
var RemoveUnusedTSRangeGroups func(ctx context.Context, tableID uint32, nodeID roachpb.NodeID, rangeGroups []RangeGroup) error

// HRManagerWLock HashRouterCache write lock
var HRManagerWLock func()

// HRManagerWUnLock HashRouterCache unwrite lock
var HRManagerWUnLock func()

// GetHashInfoByTableID query kwdb_hash_routing by specific table id
var GetHashInfoByTableID func(ctx context.Context, txn *kv.Txn, tableID uint32) ([]*KWDBHashRouting, error)

// GetHashInfoByIDInTxn query kwdb_hash_routing by specific entity group id, in an active txn.
var GetHashInfoByIDInTxn func(ctx context.Context, entitiGroupID uint64, sender kv.Sender, header *roachpb.Header) (*KWDBHashRouting, error)

// GetAllHashRoutings  query kwdb_hash_routing and fetch all rows
var GetAllHashRoutings func(ctx context.Context, txn *kv.Txn) ([]*KWDBHashRouting, error)

// CreateTSTable ...
var CreateTSTable func(ctx context.Context, tableID uint32, nodeID roachpb.NodeID, tsMeta []byte) error

// AvailableReplicaCnt return count of available status replicas.
func (g *EntityRangeGroup) AvailableReplicaCnt() int {
	cnt := 0
	for _, r := range g.InternalReplicas {
		if r.Status == EntityRangeGroupReplicaStatus_available {
			cnt++
		}
	}
	return cnt
}

// HasReplicaOnNode return true if the node have any replica.
func (g *EntityRangeGroup) HasReplicaOnNode(nodeID roachpb.NodeID) bool {
	for _, r := range g.InternalReplicas {
		if r.NodeID == nodeID {
			return true
		}
	}
	return false
}
