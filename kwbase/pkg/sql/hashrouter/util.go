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
	"math"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/pkg/errors"
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

// GetTableNodeIDs gets nodeids
func GetTableNodeIDs(ctx context.Context, db *kv.DB, tableID uint32) ([]roachpb.NodeID, error) {
	var nodeIDs []roachpb.NodeID
	nodeIDList := make(map[roachpb.NodeID]struct{})
	var retErr error
	// get range when replica is voter_incoming, because we will
	// miss the node when alter the table on some node.
	for r := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     4 * time.Second,
		Multiplier:     2,
		MaxRetries:     20,
	}); r.Next(); {
		if retErr = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			ranges, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
				Key:    sqlbase.MakeTsRangeKey(sqlbase.ID(tableID), 0, math.MinInt64),
				EndKey: sqlbase.MakeTsRangeKey(sqlbase.ID(tableID), api.HashParamV2-1, math.MaxInt64),
			})
			if err != nil {
				return err
			}
			if len(ranges) == 0 {
				return pgerror.Newf(pgcode.Warning, "can not get table : %v ranges.", tableID)
			}
			for _, r := range ranges {
				var desc roachpb.RangeDescriptor
				if err := r.ValueProto(&desc); err != nil {
					return err
				}
				for _, replica := range desc.InternalReplicas {
					if replica.GetType() == roachpb.VOTER_INCOMING {
						return errors.Errorf("replica %+v is not ready", replica)
					}
					if replica.GetType() == roachpb.VOTER_FULL {
						nodeIDList[replica.NodeID] = struct{}{}
					}
				}
			}
			return nil
		}); retErr != nil {
			log.Warningf(ctx, "get table node id failed: %s", retErr.Error())
			continue
		}
		break
	}
	if len(nodeIDList) == 0 {
		return nil, retErr
	}
	for nodeID := range nodeIDList {
		nodeIDs = append(nodeIDs, nodeID)
	}
	return nodeIDs, nil
}

// CreateTSTable create ts table
func CreateTSTable(
	ctx context.Context, tableID uint32, nodeID roachpb.NodeID, tsMeta []byte,
) error {
	return hrMgr.tseDB.CreateTSTable(ctx, sqlbase.ID(tableID), nodeID, tsMeta)
}
