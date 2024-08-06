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

package hashrouter

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// GenerateUniqueEntityRangeGroupID ...
func GenerateUniqueEntityRangeGroupID(
	ctx context.Context, db *kv.DB,
) (api.EntityRangeGroupID, error) {
	newVal, err := kv.IncrementValRetryable(ctx, db, keys.EntityRangeGroupIDGenerator, 1)
	if err != nil {
		return 0, err
	}
	return api.EntityRangeGroupID(newVal), nil
}

// GenerateUniqueEntityRangeReplicaID ...
func GenerateUniqueEntityRangeReplicaID(ctx context.Context, db *kv.DB) (uint64, error) {
	newVal, err := kv.IncrementValRetryable(ctx, db, keys.EntityRangeReplicaIDGenerator, 1)
	if err != nil {
		return 0, err
	}
	return uint64(newVal), nil
}

func maxPartitionEntityRangeGroup(
	partitionSize map[api.EntityRangeGroupID]int,
) (groupID api.EntityRangeGroupID, size int) {
	for k, v := range partitionSize {
		if v > size {
			size = v
			groupID = k
		}
	}
	return groupID, size
}

func minPartitionEntityRangeGroup(
	partitionSize map[api.EntityRangeGroupID]int,
) (groupID api.EntityRangeGroupID, size int) {
	size = int(^uint(0) >> 1)
	for key, v := range partitionSize {
		if v < size {
			size = v
			groupID = key
		}
	}
	return groupID, size
}

func maxLeaseHolderRangeGroupNode(nodes map[roachpb.NodeID]int) roachpb.NodeID {
	var nodeID roachpb.NodeID
	nodeAllLeaseHolders := make(map[roachpb.NodeID]int)
	nodeAllReplicas := make(map[roachpb.NodeID]int)
	for _, hashInfo := range hrMgr.routerCaches {
		for _, group := range hashInfo.groupsMap {
			for _, replica := range group.InternalReplicas {
				if replica.NodeID == group.LeaseHolder.NodeID {
					nodeAllLeaseHolders[replica.NodeID]++
				}
				nodeAllReplicas[replica.NodeID]++
			}
		}
	}
	for k, v := range nodes {
		if nodeID == 0 {
			nodeID = k
		} else {
			if v > nodes[nodeID] {
				nodeID = k
			}
			if v == nodes[nodeID] {
				if nodeAllLeaseHolders[k] > nodeAllLeaseHolders[nodeID] {
					nodeID = k
				} else if nodeAllLeaseHolders[k] == nodeAllLeaseHolders[nodeID] {
					if nodeAllReplicas[k] > nodeAllReplicas[nodeID] {
						nodeID = k
					}
				}
			}
		}
	}

	return nodeID
}

func minLeaseHolderRangeGroupNode(nodes map[roachpb.NodeID]int) roachpb.NodeID {
	var nodeID roachpb.NodeID
	nodeAllLeaseHolders := make(map[roachpb.NodeID]int)
	nodeAllReplicas := make(map[roachpb.NodeID]int)
	for _, hashInfo := range hrMgr.routerCaches {
		for _, group := range hashInfo.groupsMap {
			for _, replica := range group.InternalReplicas {
				if replica.NodeID == group.LeaseHolder.NodeID {
					nodeAllLeaseHolders[replica.NodeID]++
				}
				nodeAllReplicas[replica.NodeID]++
			}
		}
	}
	for k, v := range nodes {
		if nodeID == 0 {
			nodeID = k
		} else {
			if v < nodes[nodeID] {
				nodeID = k
			}
			if v == nodes[nodeID] {
				if nodeAllLeaseHolders[k] < nodeAllLeaseHolders[nodeID] {
					nodeID = k
				} else if nodeAllLeaseHolders[k] == nodeAllLeaseHolders[nodeID] {
					if nodeAllReplicas[k] < nodeAllReplicas[nodeID] {
						nodeID = k
					}
				}
			}
		}
	}
	return nodeID
}

func getChangeGroupID(
	srcNode roachpb.NodeID,
	destNode roachpb.NodeID,
	nodeGroups map[roachpb.NodeID][]api.EntityRangeGroupID,
	groupsMap map[api.EntityRangeGroupID]*api.EntityRangeGroup,
) api.EntityRangeGroupID {
	var id api.EntityRangeGroupID
	for _, groupID := range nodeGroups[srcNode] {
		if srcNode == groupsMap[groupID].LeaseHolder.NodeID && groupsMap[groupID].LeaseHolderChange.ReplicaID == 0 {
			id = groupID
			for _, replica := range groupsMap[groupID].InternalReplicas {
				if replica.NodeID == destNode {
					return groupID
				}
			}
		}
	}
	return id
}

func rebalanceLeaseHolder(
	nodeLeasHolders map[roachpb.NodeID]int,
	nodeReplicas map[roachpb.NodeID]int,
	nodeGroups map[roachpb.NodeID][]api.EntityRangeGroupID,
	groupsMap map[api.EntityRangeGroupID]*api.EntityRangeGroup,
	nodeLeaseHolderGroups map[roachpb.NodeID][]api.EntityRangeGroupID,
) (bool, []api.EntityRangePartitionMessage) {
	var message []api.EntityRangePartitionMessage
	// leaseHolder rebalance
	var isChanged bool
	minLeaseHolderNode := minLeaseHolderRangeGroupNode(nodeLeasHolders)
	maxLeaseHolderNode := maxLeaseHolderRangeGroupNode(nodeLeasHolders)
	for nodeLeasHolders[maxLeaseHolderNode]-nodeLeasHolders[minLeaseHolderNode] > 1 {
		isChanged = true
		changeGroupID := getChangeGroupID(maxLeaseHolderNode, minLeaseHolderNode, nodeGroups, groupsMap)
		groupsMap[changeGroupID].Status = api.EntityRangeGroupStatus_relocating
		var changeReplica api.EntityRangeGroupReplica
		var transferReplica api.EntityRangeGroupReplica
		for _, replica := range groupsMap[changeGroupID].InternalReplicas {
			if replica.NodeID == minLeaseHolderNode {
				transferReplica = replica
				break
			}
		}
		if transferReplica.ReplicaID != 0 {
			groupsMap[changeGroupID].LeaseHolderChange = transferReplica
			for _, partition := range groupsMap[changeGroupID].Partitions {
				message = append(message, api.EntityRangePartitionMessage{
					GroupID:              changeGroupID,
					Partition:            partition,
					SrcLeaseHolder:       groupsMap[changeGroupID].LeaseHolder,
					SrcInternalReplicas:  groupsMap[changeGroupID].InternalReplicas,
					DestLeaseHolder:      transferReplica,
					DestInternalReplicas: groupsMap[changeGroupID].InternalReplicas,
				})
			}
		} else {
			changeReplica = api.EntityRangeGroupReplica{
				ReplicaID: groupsMap[changeGroupID].LeaseHolder.ReplicaID,
				NodeID:    minLeaseHolderNode,
				StoreID:   getStoreIDByNodeID(minLeaseHolderNode, hrMgr.storePool.GetStores()),
			}
			groupsMap[changeGroupID].GroupChanges = append(groupsMap[changeGroupID].GroupChanges, changeReplica)
			var destInternalReplicas []api.EntityRangeGroupReplica
			for _, replica := range groupsMap[changeGroupID].InternalReplicas {
				if replica.ReplicaID == changeReplica.ReplicaID {
					destInternalReplicas = append(destInternalReplicas, changeReplica)
				} else {
					destInternalReplicas = append(destInternalReplicas, replica)
				}
			}
			for _, partition := range groupsMap[changeGroupID].Partitions {
				message = append(message, api.EntityRangePartitionMessage{
					GroupID:              changeGroupID,
					Partition:            partition,
					SrcLeaseHolder:       groupsMap[changeGroupID].LeaseHolder,
					SrcInternalReplicas:  groupsMap[changeGroupID].InternalReplicas,
					DestLeaseHolder:      changeReplica,
					DestInternalReplicas: destInternalReplicas,
				})
			}
			for index, groupID := range nodeGroups[maxLeaseHolderNode] {
				if groupID == changeGroupID {
					nodeGroups[maxLeaseHolderNode] = append(nodeGroups[maxLeaseHolderNode][:index], nodeGroups[maxLeaseHolderNode][index+1:]...)
				}
			}
			nodeGroups[minLeaseHolderNode] = append(nodeGroups[minLeaseHolderNode], changeGroupID)
			nodeReplicas[minLeaseHolderNode]++
			nodeReplicas[maxLeaseHolderNode]--
		}
		for index, groupID := range nodeLeaseHolderGroups[maxLeaseHolderNode] {
			if groupID == changeGroupID {
				nodeLeaseHolderGroups[maxLeaseHolderNode] = append(nodeLeaseHolderGroups[maxLeaseHolderNode][:index], nodeLeaseHolderGroups[maxLeaseHolderNode][index+1:]...)
			}
		}
		nodeLeaseHolderGroups[minLeaseHolderNode] = append(nodeLeaseHolderGroups[minLeaseHolderNode], changeGroupID)
		nodeLeasHolders[minLeaseHolderNode]++
		nodeLeasHolders[maxLeaseHolderNode]--
		minLeaseHolderNode = minLeaseHolderRangeGroupNode(nodeLeasHolders)
		maxLeaseHolderNode = maxLeaseHolderRangeGroupNode(nodeLeasHolders)
	}
	return isChanged, message
}

func rebalanceReplica(
	nodeReplicas map[roachpb.NodeID]int,
	nodeGroups map[roachpb.NodeID][]api.EntityRangeGroupID,
	groupsMap map[api.EntityRangeGroupID]*api.EntityRangeGroup,
	nodeLeaseHolderGroups map[roachpb.NodeID][]api.EntityRangeGroupID,
) (bool, []api.EntityRangePartitionMessage) {
	// leaseHolder rebalance
	var isChanged bool
	var message []api.EntityRangePartitionMessage
	// replica rebalacne
	minReplicaNode := minRangeGroupNode(nodeReplicas)
	maxReplicaNode := maxRangeGroupNode(nodeReplicas)
	for nodeReplicas[maxReplicaNode]-nodeReplicas[minReplicaNode] > 1 {
		isChanged = true
		freeGroup := make(map[api.EntityRangeGroupID]struct{})
		for _, groupID := range nodeGroups[maxReplicaNode] {
			freeGroup[groupID] = struct{}{}
		}
		for _, groupID := range nodeGroups[minReplicaNode] {
			delete(freeGroup, groupID)
		}
		var changeGroupID api.EntityRangeGroupID
		var changeReplicaID uint64
		for groupID := range freeGroup {
			groups, ok := nodeLeaseHolderGroups[maxReplicaNode]
			if ok {
				var isLeaseHolder bool
				for _, id := range groups {
					if id == groupID {
						isLeaseHolder = true
						break
					}
				}
				if isLeaseHolder {
					continue
				}
			}
			for _, replica := range groupsMap[groupID].InternalReplicas {
				if replica.NodeID == maxReplicaNode {
					changeGroupID = groupID
					changeReplicaID = replica.ReplicaID
				}
			}
			if changeReplicaID != 0 {
				break
			}
		}
		changeReplica := api.EntityRangeGroupReplica{
			ReplicaID: changeReplicaID,
			NodeID:    minReplicaNode,
			StoreID:   getStoreIDByNodeID(minReplicaNode, hrMgr.storePool.GetStores()),
		}
		groupsMap[changeGroupID].Status = api.EntityRangeGroupStatus_relocating
		groupsMap[changeGroupID].GroupChanges = append(groupsMap[changeGroupID].GroupChanges, changeReplica)
		var destInternalReplicas []api.EntityRangeGroupReplica
		for _, replica := range groupsMap[changeGroupID].InternalReplicas {
			if replica.ReplicaID == changeReplica.ReplicaID {
				destInternalReplicas = append(destInternalReplicas, changeReplica)
			} else {
				destInternalReplicas = append(destInternalReplicas, replica)
			}
		}
		for _, partition := range groupsMap[changeGroupID].Partitions {
			message = append(message, api.EntityRangePartitionMessage{
				GroupID:              changeGroupID,
				Partition:            partition,
				SrcLeaseHolder:       groupsMap[changeGroupID].LeaseHolder,
				SrcInternalReplicas:  groupsMap[changeGroupID].InternalReplicas,
				DestLeaseHolder:      groupsMap[changeGroupID].LeaseHolder,
				DestInternalReplicas: destInternalReplicas,
			})
		}
		for index, groupID := range nodeGroups[maxReplicaNode] {
			if groupID == changeGroupID {
				nodeGroups[maxReplicaNode] = append(nodeGroups[maxReplicaNode][:index], nodeGroups[maxReplicaNode][index+1:]...)
			}
		}
		nodeGroups[minReplicaNode] = append(nodeGroups[minReplicaNode], changeGroupID)
		nodeReplicas[maxReplicaNode]--
		nodeReplicas[minReplicaNode]++
		minReplicaNode = minRangeGroupNode(nodeReplicas)
		maxReplicaNode = maxRangeGroupNode(nodeReplicas)
	}
	return isChanged, message
}

func maxRangeGroupNode(nodes map[roachpb.NodeID]int) roachpb.NodeID {
	var nodeID roachpb.NodeID
	nodeAllReplicas := make(map[roachpb.NodeID]int)
	for _, hashInfo := range hrMgr.routerCaches {
		for _, group := range hashInfo.groupsMap {
			for _, replica := range group.InternalReplicas {
				nodeAllReplicas[replica.NodeID]++
			}
		}
	}
	for k, v := range nodes {
		if nodeID == 0 {
			nodeID = k
		} else {
			if v > nodes[nodeID] {
				nodeID = k
			} else if v == nodes[nodeID] {
				if nodeAllReplicas[k] > nodeAllReplicas[nodeID] {
					nodeID = k
				}
			}
		}
	}
	return nodeID
}

func minRangeGroupNode(nodes map[roachpb.NodeID]int) roachpb.NodeID {
	var nodeID roachpb.NodeID
	nodeAllReplicas := make(map[roachpb.NodeID]int)
	for _, hashInfo := range hrMgr.routerCaches {
		for _, group := range hashInfo.groupsMap {
			for _, replica := range group.InternalReplicas {
				nodeAllReplicas[replica.NodeID]++
			}
		}
	}
	for k, v := range nodes {
		if nodeID == 0 {
			nodeID = k
		} else {
			if v < nodes[nodeID] {
				nodeID = k
			} else if v == nodes[nodeID] {
				if nodeAllReplicas[k] < nodeAllReplicas[nodeID] {
					nodeID = k
				}
			}
		}
	}
	return nodeID
}

func getStoreIDByNodeID(
	nodeID roachpb.NodeID, stores map[roachpb.StoreID]roachpb.StoreDescriptor,
) roachpb.StoreID {
	var id roachpb.StoreID
	for storeID, store := range stores {
		if store.Node.NodeID == nodeID {
			id = storeID
			break
		}
	}
	return id
}

func groupChangesOnAddNode(
	nodeID roachpb.NodeID,
	groupsMap map[api.EntityRangeGroupID]*api.EntityRangeGroup,
	stores map[roachpb.StoreID]roachpb.StoreDescriptor,
) {
	nodeReplicas := make(map[roachpb.NodeID]int)
	nodeLeasHolders := make(map[roachpb.NodeID]int)
	nodeAllLeaseHolders := make(map[roachpb.NodeID]int)
	for _, hashInfo := range hrMgr.routerCaches {
		for _, group := range hashInfo.groupsMap {
			for _, replica := range group.InternalReplicas {
				if replica.NodeID == group.LeaseHolder.NodeID {
					nodeAllLeaseHolders[replica.NodeID]++
				}
			}
		}
	}
	for _, group := range groupsMap {
		for _, replica := range group.InternalReplicas {
			if replica.NodeID == group.LeaseHolder.NodeID {
				nodeLeasHolders[replica.NodeID]++
			}
			nodeReplicas[replica.NodeID]++
		}
	}
	var relocated bool
	nodeGroupIDs := make(map[api.EntityRangeGroupID]struct{})
	nodeReplicaSize := 0
	nodeLeaseHolderSize := 0
	maxLeasaHolderNode := maxLeaseHolderRangeGroupNode(nodeLeasHolders)
	for nodeLeasHolders[maxLeasaHolderNode]-nodeLeaseHolderSize > 1 ||
		(nodeAllLeaseHolders[maxLeasaHolderNode]-nodeAllLeaseHolders[nodeID] > 1 && !relocated) {
		fmt.Printf("The maxLeasaHolderNode is : %v", maxLeasaHolderNode)
		for id, group := range groupsMap {
			if group.LeaseHolder.NodeID == maxLeasaHolderNode {
				groupsMap[id].Status = api.EntityRangeGroupStatus_relocating
				groupsMap[id].GroupChanges = append(groupsMap[id].GroupChanges, api.EntityRangeGroupReplica{
					ReplicaID: group.LeaseHolder.ReplicaID,
					NodeID:    nodeID,
					StoreID:   getStoreIDByNodeID(nodeID, stores),
				})
				relocated = true
				nodeGroupIDs[id] = struct{}{}
				nodeReplicas[maxLeasaHolderNode]--
				nodeLeasHolders[maxLeasaHolderNode]--
				nodeLeaseHolderSize++
				nodeReplicaSize++
				break
			}
		}
		maxLeasaHolderNode = maxLeaseHolderRangeGroupNode(nodeLeasHolders)
	}
	maxReplicaNode := maxRangeGroupNode(nodeReplicas)
	for nodeReplicas[maxReplicaNode]-nodeReplicaSize > 1 {
		for id, group := range groupsMap {
			if group.LeaseHolder.NodeID != maxReplicaNode {
				_, ok := nodeGroupIDs[id]
				if ok {
					continue
				}
				var changed bool
				for _, replica := range group.InternalReplicas {
					if replica.NodeID == maxReplicaNode {
						groupsMap[id].Status = api.EntityRangeGroupStatus_relocating
						groupsMap[id].GroupChanges = append(groupsMap[id].GroupChanges, api.EntityRangeGroupReplica{
							ReplicaID: replica.ReplicaID,
							NodeID:    nodeID,
							StoreID:   getStoreIDByNodeID(nodeID, stores),
						})
						changed = true
						nodeGroupIDs[id] = struct{}{}
						nodeReplicaSize++
						nodeReplicas[maxReplicaNode]--
					}
				}
				if changed {
					break
				}
			}
		}
		maxReplicaNode = maxRangeGroupNode(nodeReplicas)
	}
}

func addMessageFromGroupChange(
	groupsMap map[api.EntityRangeGroupID]*api.EntityRangeGroup,
	nodeID roachpb.NodeID,
	stores map[roachpb.StoreID]roachpb.StoreDescriptor,
	hashPartitionSize int,
) []api.EntityRangeGroupChange {
	var message []api.EntityRangePartitionMessage
	var groupChanges []api.EntityRangeGroupChange
	// Construct a Partition and return the final changes to the upper layer
	for _, group := range groupsMap {
		// Get all partitions
		partitions := make(map[uint32]struct{})
		for key := range group.Partitions {
			partitions[key] = struct{}{}
		}
		var destInternalReplicas []api.EntityRangeGroupReplica
		for _, replica := range group.InternalReplicas {
			destInternalReplicas = append(destInternalReplicas, replica)
		}
		if group.LeaseHolderChange.ReplicaID != 0 {
			for id := range partitions {
				message = append(message, api.EntityRangePartitionMessage{
					GroupID:              group.GroupID,
					Partition:            group.Partitions[id],
					SrcLeaseHolder:       group.LeaseHolder,
					SrcInternalReplicas:  group.InternalReplicas,
					DestLeaseHolder:      group.LeaseHolderChange,
					DestInternalReplicas: group.InternalReplicas,
				})
			}
		}
		// Describe the changes in the Partition based on the Group's Change
		var isChange bool
		var leaseHolderNode roachpb.NodeID
		for _, change := range group.GroupChanges {
			for i, replica := range destInternalReplicas {
				if replica.ReplicaID == change.ReplicaID {
					destInternalReplicas[i].NodeID = change.NodeID
					destInternalReplicas[i].StoreID = change.StoreID
					isChange = true
					if replica.ReplicaID == group.LeaseHolder.ReplicaID {
						leaseHolderNode = change.NodeID
					}
					break
				}
			}
		}
		var destLeaseHolder api.EntityRangeGroupReplica
		if leaseHolderNode != 0 {
			destLeaseHolder = api.EntityRangeGroupReplica{
				ReplicaID: group.LeaseHolder.ReplicaID,
				NodeID:    leaseHolderNode,
				StoreID:   getStoreIDByNodeID(leaseHolderNode, stores),
			}
		} else {
			destLeaseHolder = group.LeaseHolder
		}
		if isChange {
			var gc api.EntityRangeGroupChange
			for id := range partitions {
				partitionMsg := api.EntityRangePartitionMessage{
					GroupID:              group.GroupID,
					Partition:            group.Partitions[id],
					SrcLeaseHolder:       group.LeaseHolder,
					SrcInternalReplicas:  group.InternalReplicas,
					DestLeaseHolder:      destLeaseHolder,
					DestInternalReplicas: destInternalReplicas,
				}
				message = append(message, partitionMsg)
				gc.Messages = append(gc.Messages, partitionMsg)
			}
			routing := api.KWDBHashRouting{
				EntityRangeGroupId: group.GroupID,
				TableID:            group.TableID,
				EntityRangeGroup:   *group,
				TsPartitionSize:    int32(hashPartitionSize),
			}
			gc.Routing = routing
			groupChanges = append(groupChanges, gc)
		}
	}
	return groupChanges
}

// TransferPartitionLease to control all ranges lease for partition by hrMgr.
func TransferPartitionLease(
	ctx context.Context, tableID uint32, change api.EntityRangePartitionMessage,
) error {
	return hrMgr.tseDB.TransferPartitionLease(ctx, sqlbase.ID(tableID), change)
}

// AddPartitionReplicas adding a set of replicas to all ranges for partition by hrNgr.
func AddPartitionReplicas(
	ctx context.Context, tableID uint32, change api.EntityRangePartitionMessage,
) error {
	return hrMgr.tseDB.AddPartitionReplicas(ctx, sqlbase.ID(tableID), change)
}

// RemovePartitionReplicas adding a set of replicas to all ranges for partition by hrNgr.
func RemovePartitionReplicas(
	ctx context.Context, tableID uint32, change api.EntityRangePartitionMessage,
) error {
	return hrMgr.tseDB.RemovePartitionReplicas(ctx, sqlbase.ID(tableID), change)
}

// RefreshTSRangeGroup updates PartitionRangeGroup
func RefreshTSRangeGroup(
	ctx context.Context,
	tableID uint32,
	nodeID roachpb.NodeID,
	rangeGroups []api.RangeGroup,
	tsMeta []byte,
) error {
	return hrMgr.tseDB.UpdateTSRangeGroup(ctx, sqlbase.ID(tableID), nodeID, rangeGroups, tsMeta)
}

// RemoveUnusedTSRangeGroups comment .
func RemoveUnusedTSRangeGroups(
	ctx context.Context, tableID uint32, nodeID roachpb.NodeID, rangeGroups []api.RangeGroup,
) error {
	return hrMgr.tseDB.RemoveUnusedTSRangeGroup(ctx, sqlbase.ID(tableID), nodeID, rangeGroups)
}

// TSRequestLease request lease for the target store
func TSRequestLease(
	ctx context.Context, tableID uint32, startPoint api.HashPoint, nodeID roachpb.NodeID,
) error {
	startKey := sqlbase.MakeTsHashPointKey(sqlbase.ID(tableID), uint64(startPoint))
	return hrMgr.tseDB.TsRequestLease(ctx, startKey, nodeID)
}

// RelocatePartitionReplicas adding a set of replicas to all ranges for partition by hrNgr.
func RelocatePartitionReplicas(
	ctx context.Context, tableID uint32, change api.EntityRangePartitionMessage, uselessRange bool,
) error {
	return hrMgr.tseDB.RelocatePartition(ctx, sqlbase.ID(tableID), change, uselessRange)
}

// HRManagerWLock is used to lock
func HRManagerWLock() {
	hrMgr.mu.Lock()
	return
}

// HRManagerWUnLock is used to unlock
func HRManagerWUnLock() {
	hrMgr.mu.Unlock()
	return
}

// HRManagerRLock is used to lock
func HRManagerRLock() {
	hrMgr.mu.RLock()
	return
}

// HRManagerRUnLock is used to unlock
func HRManagerRUnLock() {
	hrMgr.mu.RUnlock()
	return
}

// GetHashInfoByID query kwdb_hash_routing by specific table id
func GetHashInfoByID(
	ctx context.Context, txn *kv.Txn, entityGroupID uint64,
) (*api.KWDBHashRouting, error) {
	var err error
	var hashRouting *api.KWDBHashRouting
	getHashRouting := func(ctx context.Context, newTxn *kv.Txn) error {
		hashRouting, err = hrMgr.GetHashRoutingByID(ctx, newTxn, entityGroupID)
		return err
	}
	if txn == nil {
		err = hrMgr.db.Txn(ctx, getHashRouting)
	} else {
		err = getHashRouting(ctx, txn)
	}
	return hashRouting, err
}

// GetHashInfoByIDInTxn query kwdb_hash_routing by specific entity group id, in an active txn.
func GetHashInfoByIDInTxn(
	ctx context.Context, entitiGroupID uint64, sender kv.Sender, header *roachpb.Header,
) (*api.KWDBHashRouting, error) {
	return hrMgr.GetHashRoutingByIDInTxn(ctx, entitiGroupID, sender, header)
}

// GetHashInfoByTableID query kwdb_hash_routing by specific table id
func GetHashInfoByTableID(
	ctx context.Context, txn *kv.Txn, tableID uint32,
) ([]*api.KWDBHashRouting, error) {
	var hashRoutings []*api.KWDBHashRouting
	var err error
	var table *sqlbase.TableDescriptor
	if txn == nil {
		err = hrMgr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
			table, _, err = sqlbase.GetTsTableDescFromID(ctx, newTxn, sqlbase.ID(tableID))
			if err != nil || table == nil || (table.State != sqlbase.TableDescriptor_ADD && table.State != sqlbase.TableDescriptor_PUBLIC) {
				return err
			}
			hashRoutings, err = hrMgr.GetHashRoutingsByTableID(ctx, newTxn, tableID)
			if err != nil {
				return err
			}
			return nil
		})
	} else {
		table, _, err = sqlbase.GetTsTableDescFromID(ctx, txn, sqlbase.ID(tableID))
		if err != nil || table == nil || (table.State != sqlbase.TableDescriptor_ADD && table.State != sqlbase.TableDescriptor_PUBLIC) {
			return hashRoutings, err
		}
		hashRoutings, err = hrMgr.GetHashRoutingsByTableID(ctx, txn, tableID)
		if err != nil {
			return hashRoutings, err
		}
	}
	return hashRoutings, err
}

// GetAllKWDBHashRoutings query kwdb_hash_routing and fetch all rows
func GetAllKWDBHashRoutings(ctx context.Context, txn *kv.Txn) ([]*api.KWDBHashRouting, error) {
	return hrMgr.GetAllHashRoutings(ctx, txn)
}

func allGroupAvailable(groups map[api.EntityRangeGroupID]*api.EntityRangeGroup) (bool, string) {
	for _, group := range groups {
		if group.Status != api.EntityRangeGroupStatus_Available && group.Status != api.EntityRangeGroupStatus_lacking {
			str := fmt.Sprintf("The group : %v status is : %v", group.GroupID, group.Status)
			return false, str
		}
	}
	return true, ""
}

func makeHashRouter(tableID uint32, routings []*api.KWDBHashRouting) (api.HashRouter, error) {
	if len(routings) == 0 {
		return nil, errors.Errorf("the KWDBHashRouting is empty.")
	}
	var hashPartitionSize int
	var hashPartitionNum int64
	groups := map[api.EntityRangeGroupID]*api.EntityRangeGroup{}
	for _, routing := range routings {
		hashPartitionSize = int(routing.TsPartitionSize)
		hashPartitionNum++
		groups[routing.EntityRangeGroupId] = &routing.EntityRangeGroup
	}
	return &hashRouterInfo{
		tableID:           tableID,
		hashPartitionNum:  hashPartitionNum,
		hashPartitionSize: hashPartitionSize,
		groupsMap:         groups,
	}, nil
}

func makeHRMgr(routings []*api.KWDBHashRouting) (api.HashRouterManager, error) {
	if len(routings) == 0 {
		return hrMgr, nil
	}
	caches := make(map[uint32]*hashRouterInfo)
	hashPartitionSize := make(map[uint32]int)
	hashPartitionNum := make(map[uint32]int64)
	for _, routing := range routings {
		hashPartitionSize[routing.TableID] = int(routing.TsPartitionSize)
		hashPartitionNum[routing.TableID]++
		// Build entityRangeGroup and store it in memory
		_, ok := caches[routing.TableID]
		if !ok {
			caches[routing.TableID] = &hashRouterInfo{
				groupsMap: make(map[api.EntityRangeGroupID]*api.EntityRangeGroup),
			}
		}
		caches[routing.TableID].groupsMap[routing.EntityRangeGroupId] = &routing.EntityRangeGroup
	}
	for tableID, size := range hashPartitionSize {
		caches[tableID].hashPartitionSize = size
		caches[tableID].hashPartitionNum = hashPartitionNum[tableID]
	}
	return &HRManager{
		ctx:          hrMgr.ctx,
		cs:           hrMgr.cs,
		routerCaches: caches,
		execConfig:   hrMgr.execConfig,
		db:           hrMgr.db,
		tseDB:        hrMgr.tseDB,
		gossip:       hrMgr.gossip,
		leaseMgr:     hrMgr.leaseMgr,
		nodeLiveness: hrMgr.nodeLiveness,
		storePool:    hrMgr.storePool,
	}, nil
}
