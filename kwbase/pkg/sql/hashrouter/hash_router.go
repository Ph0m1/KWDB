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
	"hash/fnv"
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/tscoord"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

func init() {
	api.GetHashRouterManagerWithTxn = GetHashRouterManagerWithTxn
	api.GetHashRouterManagerWithCache = GetHashRouterManagerWithCache
	api.GetHashRouterWithTable = GetHashRouterCache
	api.GetHealthyNodeIDs = GetHealthyNodeIDs
	api.GetTableNodeIDs = GetTableNodeIDs
	api.HRManagerWLock = HRManagerWLock
	api.HRManagerWUnLock = HRManagerWUnLock
	api.GetHashInfoByTableID = GetHashInfoByTableID
	api.GetHashInfoByIDInTxn = GetHashInfoByIDInTxn
	api.GetAllHashRoutings = GetAllKWDBHashRoutings
	api.CreateTSTable = CreateTSTable
	api.PreDistributeBySingleReplica = PreDistributeBySingleReplica
	api.GetDistributeInfo = GetDistributeInfo
}

// hashRouterInfo  the group distribute by table
type hashRouterInfo struct {
	tableID           uint32
	cacheVersion      sqlbase.DescriptorVersion
	hashPartitionNum  int64
	hashPartitionSize int
	groupsMap         map[api.EntityRangeGroupID]*api.EntityRangeGroup
	mu                syncutil.RWMutex
}

// HRManager all group distribute
type HRManager struct {
	// context from server
	ctx context.Context
	// cluster settings
	cs            *cluster.Settings
	routerCaches  map[uint32]*hashRouterInfo
	gossipMessage api.GossipEntityRangeGroupMessage
	execConfig    *sql.ExecutorConfig
	db            *kv.DB
	tseDB         *tscoord.DB
	gossip        *gossip.Gossip
	leaseMgr      *sql.LeaseManager
	nodeLiveness  *kvserver.NodeLiveness
	storePool     *kvserver.StorePool
	mu            syncutil.RWMutex
}

var hrMgr *HRManager

// NewHashRouterManager init manager when server start
func NewHashRouterManager(
	ctx context.Context,
	cs *cluster.Settings,
	db *kv.DB,
	tseDB *tscoord.DB,
	execConfig *sql.ExecutorConfig,
	gossip *gossip.Gossip,
	leaseMgr *sql.LeaseManager,
	nodeLiveness *kvserver.NodeLiveness,
	storePool *kvserver.StorePool,
) (*HRManager, error) {
	if execConfig.StartMode == sql.StartSingleReplica || execConfig.StartMode == sql.StartSingleNode {
		api.MppMode = true
	}
	hrMgr = &HRManager{
		ctx:          ctx,
		cs:           cs,
		routerCaches: make(map[uint32]*hashRouterInfo),
		db:           db,
		tseDB:        tseDB,
		execConfig:   execConfig,
		gossip:       gossip,
		leaseMgr:     leaseMgr,
		nodeLiveness: nodeLiveness,
		storePool:    storePool,
	}
	return hrMgr, nil
}

// checkTableExists check whether table exists.
func (mr *HRManager) checkTableExists(ctx context.Context, txn *kv.Txn, tableID uint32) bool {
	check := func(ctx context.Context, newTxn *kv.Txn) error {
		table, _, err := sqlbase.GetTsTableDescFromID(ctx, newTxn, sqlbase.ID(tableID))
		if err != nil {
			return errors.Wrap(err, "[HA]get table failed")
		}
		if table == nil {
			return errors.Errorf("table doesn't exist: %v", table)
		}
		if table.State != sqlbase.TableDescriptor_PUBLIC && table.State != sqlbase.TableDescriptor_ALTER {
			log.Warningf(ctx, "table %v is not public, current status %v", tableID, table.State)
			return errors.Newf("table %v is not public, current status %v", tableID, table.State)
		}
		return nil
	}
	var descErr error
	if txn == nil {
		descErr = mr.db.Txn(ctx, check)
	} else {
		descErr = check(ctx, txn)
	}
	if descErr != nil {
		log.Error(ctx, descErr)
		return false
	}
	return true

}

// GetAllHashRouterInfo get all table distribute from disk
func (mr *HRManager) GetAllHashRouterInfo(
	ctx context.Context, txn *kv.Txn,
) (map[uint32]api.HashRouter, error) {
	result := make(map[uint32]api.HashRouter)
	for tableID, router := range mr.routerCaches {
		if !mr.checkTableExists(ctx, txn, tableID) {
			continue
		}
		result[tableID] = router
	}
	return result, nil
}

// GetHashInfoByTableID get the table groups distribute
func (mr *HRManager) GetHashInfoByTableID(ctx context.Context, tableID uint32) api.HashRouter {
	var info api.HashRouter
	for id, hc := range mr.routerCaches {
		hc.mu.RLock()
		if id == tableID {
			info = hc
		}
		hc.mu.RUnlock()
	}
	return info
}

// GetDistributeInfo get the partitions on create table
func GetDistributeInfo(ctx context.Context, tableID uint32) ([]api.HashPartition, error) {
	var partitions []api.HashPartition
	partitionBalanceNumber := settings.DefaultPartitionCoefficient.Get(hrMgr.execConfig.SV())
	if partitionBalanceNumber == 0 {
		return nil, errors.Errorf("the cluster setting partitionBalanceNumber is 0.please wait and try again")
	}
	var groupNum int
	if hrMgr.execConfig.StartMode == sql.StartSingleNode {
		groupNum = 1
	} else {
		groupNum = api.HashParamV2
	}
	splitMode := int(settings.TSRangeSplitModeSetting.Get(hrMgr.execConfig.SV()))
	if splitMode == 0 {
		for i := 0; i < groupNum; i += int(partitionBalanceNumber) {
			partitions = append(partitions, api.HashPartition{
				StartPoint:     api.HashPoint(i),
				StartTimeStamp: math.MinInt64,
			})
		}
	} else if splitMode == 1 {
		for i := 0; i < groupNum; i += int(partitionBalanceNumber) {
			splitType := api.SplitType(i / int(partitionBalanceNumber))
			switch splitType {
			case api.SplitWithOneHashPoint:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
				}
			case api.SplitWithHashPointAndPositiveTimeStamp:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: 1681111110000,
					})
				}
			case api.SplitWithHashPointAndNegativeTimeStamp:
				for k := 0; k < int(partitionBalanceNumber); k++ {
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: math.MinInt64,
					})
					partitions = append(partitions, api.HashPartition{
						StartPoint:     api.HashPoint(i + k),
						StartTimeStamp: -1681111110000,
					})
				}
			default:
				partitions = append(partitions, api.HashPartition{
					StartPoint:     api.HashPoint(i),
					StartTimeStamp: math.MinInt64,
				})
			}
		}
	}
	return partitions, nil
}

// PreDistributeBySingleReplica is used on StartSingleReplica to get pre distribute
func PreDistributeBySingleReplica(
	ctx context.Context, txn *kv.Txn, tableID uint32, partitions []api.HashPartition,
) ([]roachpb.ReplicaDescriptor, error) {
	var distributeReplicas []roachpb.ReplicaDescriptor
	groupsMap := make(map[api.EntityRangeGroupID]*api.EntityRangeGroup)
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	var nodeList []roachpb.NodeID
	for _, n := range nodeStatus.Nodes {
		// filter nodes which can't be used
		if nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_DEAD ||
			nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_DECOMMISSIONED ||
			nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_UNAVAILABLE ||
			nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_UPGRADING {
			continue
		}
		nodeList = append(nodeList, n.Desc.NodeID)
	}
	for index, partition := range partitions {
		nodeID := nodeList[index%len(nodeList)]
		distributeReplica := roachpb.ReplicaDescriptor{
			NodeID:  nodeID,
			StoreID: getStoreIDByNodeID(nodeID, hrMgr.storePool.GetStores()),
		}
		distributeReplicas = append(distributeReplicas, distributeReplica)
		entityRangeGroupID, err := GenerateUniqueEntityRangeGroupID(ctx, hrMgr.execConfig.DB)
		if err != nil {
			return nil, err
		}
		// make entityRangeGroup
		group := api.EntityRangeGroup{
			GroupID: entityRangeGroupID,
			LeaseHolder: api.EntityRangeGroupReplica{
				NodeID:  distributeReplica.NodeID,
				StoreID: distributeReplica.StoreID,
			},
			Status: api.EntityRangeGroupStatus_Available,
		}
		partitions := make(map[uint32]api.HashPartition)
		partitions[uint32(partition.StartPoint)] = partition
		group.Partitions = partitions
		groupsMap[entityRangeGroupID] = &group
	}

	var kwdbHashRoutings []api.KWDBHashRouting
	for id, group := range groupsMap {
		kwdbHashRoutings = append(kwdbHashRoutings, api.KWDBHashRouting{
			EntityRangeGroupId: id,
			EntityRangeGroup:   *group,
			TableID:            tableID,
		})
	}
	err = hrMgr.PutHashInfo(ctx, txn, kwdbHashRoutings)
	if err != nil {
		return nil, fmt.Errorf("put to disk error : %v", err)
	}
	return distributeReplicas, nil
}

// GetHashRouterManagerWithTxn get the HashRouterManager object with txn
func GetHashRouterManagerWithTxn(ctx context.Context, txn *kv.Txn) (api.HashRouterManager, error) {
	if hrMgr == nil {
		panic("error: HRManager not init")
	}
	info, err := hrMgr.GetAllHashRoutings(ctx, txn)
	if err != nil {
		return nil, errors.Errorf("error: Get All HashRoutings Error: %v", err)
	}
	mgr, err := makeHRMgr(info)
	if err != nil {
		return nil, errors.Errorf("error: Get All HashRoutings Error: %v", err)
	}
	return mgr, nil
}

// GetHashRouterManagerWithCache get the HashRouterManager object from cache
func GetHashRouterManagerWithCache() api.HashRouterManager {
	if hrMgr == nil {
		panic("error: HRManager not init")
	}
	return hrMgr
}

// GetHealthyNodeIDs get all healthy nodes
func GetHealthyNodeIDs(ctx context.Context) ([]roachpb.NodeID, error) {
	var NodeIDList []roachpb.NodeID
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	for id, n := range nodeStatus.LivenessByNodeID {
		switch n {
		case storagepb.NodeLivenessStatus_LIVE:
			NodeIDList = append(NodeIDList, id)
		}
	}
	if len(NodeIDList) == 0 {
		return NodeIDList, pgerror.New(pgcode.Warning, "all nodes are unhealthy")
	}
	return NodeIDList, nil
}

// GetHashRouterCache read the table distribute from disk return the hashrouter object
func GetHashRouterCache(
	databaseID uint32, tableID uint32, isCreateTable bool, txn *kv.Txn,
) (api.HashRouter, error) {
	infos, err := hrMgr.GetHashRoutingsByTableID(context.Background(), nil, tableID)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, errors.Errorf("error: HashRouter not init")
	}
	routerInfo, err := makeHashRouter(tableID, infos)
	if err != nil {
		return nil, err
	}
	return routerInfo, nil
	//}
}

// GetNodeIDByPrimaryTag get the leaseHolder node by primaryTag
func (hc *hashRouterInfo) GetNodeIDByPrimaryTag(
	ctx context.Context, primaryTags ...[]byte,
) ([]roachpb.NodeID, error) {
	var nodeIDs []roachpb.NodeID
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	if hrMgr.execConfig.StartMode == sql.StartSingleNode {
		return []roachpb.NodeID{1}, nil
	}
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	if api.MppMode {

		for _, n := range nodeStatus.LivenessByNodeID {
			switch n {
			case storagepb.NodeLivenessStatus_DEAD:
				return nil, pgerror.New(pgcode.SQLRoutineException, "the cluster has node dead")
			case storagepb.NodeLivenessStatus_DECOMMISSIONING:
				return nil, pgerror.New(pgcode.SQLRoutineException, "the cluster has node decommissioning")
			case storagepb.NodeLivenessStatus_UNAVAILABLE:
				return nil, pgerror.New(pgcode.SQLRoutineException, "the cluster has node unhealthy")
			case storagepb.NodeLivenessStatus_UPGRADING:
				return nil, pgerror.New(pgcode.SQLRoutineException, "the cluster has node upgrading")
			default:
				continue
			}
		}
	}
	for _, primaryTag := range primaryTags {
		// calculate HashPoint
		fnv32 := fnv.New32()
		_, err := fnv32.Write(primaryTag)
		if err != nil {
			return nil, err
		}
		hash := fnv32.Sum32() % api.HashParamV2
		for _, v := range hc.groupsMap {
			if _, ok := v.Partitions[hash]; ok {
				if v.Status == api.EntityRangeGroupStatus_transferring {
					return nil, pgerror.Newf(pgcode.SQLRoutineException, "entity group %v status is: %v", v.GroupID, v.Status)
				}
				if status, exist := nodeStatus.LivenessByNodeID[v.LeaseHolder.NodeID]; !exist || status == storagepb.NodeLivenessStatus_UNAVAILABLE {
					return nil, pgerror.Newf(pgcode.SQLRoutineException, "cluster has unhealthy leaseHolder: %v, please wait a minute", v.LeaseHolder.NodeID)
				} else if !exist || status == storagepb.NodeLivenessStatus_UPGRADING {
					return nil, pgerror.Newf(pgcode.SQLRoutineException, "The leaseholder node is upgrading, please wait a min.")
				}
				nodeIDs = append(nodeIDs, v.LeaseHolder.NodeID)
				break
			}
		}
		fnv32.Reset()
	}

	return nodeIDs, nil
}

// GetPartitionByPoint get hashPartition by hashPoint
func (hc *hashRouterInfo) GetPartitionByPoint(
	ctx context.Context, point api.HashPoint,
) (api.HashPartition, error) {
	var partition api.HashPartition
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	if hc.hashPartitionSize == 0 {
		return api.HashPartition{}, errors.Errorf("the table : %v partitions size is 0.", hc.tableID)
	}
	if hc.hashPartitionNum == 0 {
		return api.HashPartition{}, errors.Errorf("the table : %v partitions num is 0.", hc.tableID)
	}
	// calculate the HashPartitionID, if HashPoint/HashPartition > partition num use the max ID
	hashPartitionID := uint32(point) / uint32(hc.hashPartitionSize)
	if hashPartitionID == uint32(hc.hashPartitionNum) {
		hashPartitionID--
	}
	for _, v := range hc.groupsMap {
		if p, ok := v.Partitions[hashPartitionID]; ok {
			if v.Status != api.EntityRangeGroupStatus_Available {
				return partition, fmt.Errorf("The Point %v EntityRangeGroup status is : %v", point, v.Status)
			}
			partition = p
			break
		} else {
			continue
		}
	}
	return partition, nil
}

// GetGroupIDByPrimaryTag get the leaseHolder groupID by primaryTag
func (hc *hashRouterInfo) GetGroupIDByPrimaryTag(
	ctx context.Context, primaryTag []byte,
) (api.EntityRangeGroupID, error) {
	var groupID api.EntityRangeGroupID
	if api.MppMode {
		return api.EntityRangeGroupID(hc.tableID), nil
	}
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	// calculate HashPoint
	fnv32 := fnv.New32()
	_, err := fnv32.Write(primaryTag)
	if err != nil {
		return 0, pgerror.Wrap(err, pgcode.SQLRoutineException, fmt.Sprintf("calculate hash point from primary tag %v failed", primaryTag))
	}
	hash := fnv32.Sum32() % api.HashParamV2

	for _, v := range hc.groupsMap {
		if _, ok := v.Partitions[hash]; ok {
			if v.Status == api.EntityRangeGroupStatus_transferring {
				return 0, pgerror.Newf(pgcode.SQLRoutineException, "entity group %v status is: %v", v.GroupID, v.Status)
			}
			groupID = v.GroupID
			break
		}
	}
	fnv32.Reset()
	return groupID, nil
}

// GetLeaseHolderNodeIDs get this table leaseHolder NodeID
func (hc *hashRouterInfo) GetLeaseHolderNodeIDs(
	ctx context.Context, isMppMode bool,
) ([]roachpb.NodeID, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	var nodeList []roachpb.NodeID
	nodeSet := make(map[roachpb.NodeID]struct{})
	for _, v := range hc.groupsMap {
		nodeSet[v.LeaseHolder.NodeID] = struct{}{}
	}
	for node := range nodeSet {
		nodeList = append(nodeList, node)
	}

	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}

	for _, id := range nodeList {
		if status, exist := nodeStatus.LivenessByNodeID[id]; !exist || status == storagepb.NodeLivenessStatus_UNAVAILABLE || status == storagepb.NodeLivenessStatus_UPGRADING {
			if status == storagepb.NodeLivenessStatus_UPGRADING {
				return nil, pgerror.Newf(pgcode.SQLRoutineException, "The leaseholder node is uprading, please wait a min.")
			}
			return nil, pgerror.Newf(pgcode.SQLRoutineException, "The leaseholder node is unhealthy, please wait a min.")
		}
	}

	if len(nodeList) == 0 {
		return nodeList, pgerror.New(pgcode.Warning, "all nodes are unhealthy")
	}
	return nodeList, nil
}

// GetHashPartitions get all group distribute
func (hc *hashRouterInfo) GetHashPartitions(
	ctx context.Context,
) map[api.EntityRangeGroupID]*api.EntityRangeGroup {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return hc.groupsMap
}

// GetGroupIDAndRoleOnNode get groupID and role on nodeID
func (hc *hashRouterInfo) GetGroupIDAndRoleOnNode(
	ctx context.Context, nodeID roachpb.NodeID,
) []api.RangeGroup {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	var groups []api.RangeGroup
	for _, group := range hc.groupsMap {
		if group.LeaseHolder.NodeID == nodeID {
			groups = append(groups, api.RangeGroup{
				RangeGroupID: group.GroupID,
				Type:         api.ReplicaType_LeaseHolder,
			})
		} else {
			for _, replica := range group.InternalReplicas {
				if replica.NodeID == nodeID {
					groups = append(groups, api.RangeGroup{
						RangeGroupID: group.GroupID,
						Type:         api.ReplicaType_Follower,
					})
					break
				}
			}
		}
	}
	return groups
}

// GetGroupsOnNode get all groups on nodeID
func (hc *hashRouterInfo) GetGroupsOnNode(
	ctx context.Context, nodeID roachpb.NodeID,
) []api.EntityRangeGroup {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	var groups []api.EntityRangeGroup
	for _, group := range hc.groupsMap {
		for _, replica := range group.InternalReplicas {
			if replica.NodeID == nodeID {
				groups = append(groups, *group)
				break
			}
		}
	}
	return groups
}

// GetNodeDistributeByHashPoint get the HashPoint leaseHolder and follower
func (hc *hashRouterInfo) GetNodeDistributeByHashPoint(
	ctx context.Context, point api.HashPoint,
) (roachpb.NodeID, []roachpb.NodeID, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	// calculate HashPartitionID,
	var leaseHolder roachpb.NodeID
	var follower []roachpb.NodeID
	if hc.hashPartitionSize == 0 {
		return 0, nil, errors.Errorf("the table : %v partitions size is 0.", hc.tableID)
	}
	if hc.hashPartitionNum == 0 {
		return 0, nil, errors.Errorf("the table : %v partitions num is 0.", hc.tableID)
	}
	hashPartitionID := uint32(point) / uint32(hc.hashPartitionSize)
	if hashPartitionID == uint32(hc.hashPartitionNum) {
		hashPartitionID--
	}
	// get the Group by point
	for _, v := range hc.groupsMap {
		if _, ok := v.Partitions[hashPartitionID]; ok {
			if v.Status != api.EntityRangeGroupStatus_Available {
				return 0, nil, fmt.Errorf("The point %v EntityRangeGroup status is : %v", point, v.Status)
			}
			leaseHolder = v.LeaseHolder.NodeID
			for _, replica := range v.InternalReplicas {
				if replica.NodeID != v.LeaseHolder.NodeID {
					follower = append(follower, replica.NodeID)
				}
			}
			break
		} else {
			continue
		}
	}
	return leaseHolder, follower, nil
}

// GetGroupByHashPoint get the group by HashPoint
func (hc *hashRouterInfo) GetGroupByHashPoint(
	ctx context.Context, point api.HashPoint,
) (*api.EntityRangeGroup, error) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	var group *api.EntityRangeGroup
	// calculate HashPartitionID,
	if hc.hashPartitionSize == 0 {
		return nil, errors.Errorf("the table : %v partitions size is 0.", hc.tableID)
	}
	if hc.hashPartitionNum == 0 {
		return nil, errors.Errorf("the table : %v partitions num is 0.", hc.tableID)
	}
	hashPartitionID := uint32(point) / uint32(hc.hashPartitionSize)
	if hashPartitionID == uint32(hc.hashPartitionNum) {
		hashPartitionID--
	}
	// get the Group by point
	for _, v := range hc.groupsMap {
		if _, ok := v.Partitions[hashPartitionID]; ok {
			group = v
			break
		} else {
			continue
		}
	}
	return group, nil
}

// GetAllGroups get all groups on this table
func (hc *hashRouterInfo) GetAllGroups(ctx context.Context) []api.EntityRangeGroup {
	var groups []api.EntityRangeGroup
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	for _, group := range hc.groupsMap {
		groups = append(groups, *group)
	}
	return groups
}

// PutSingleHashInfoWithLock push hash info with lock.
func (mr *HRManager) PutSingleHashInfoWithLock(
	ctx context.Context, tableID uint32, txn *kv.Txn, routing api.KWDBHashRouting,
) (err error) {
	err = mr.PutHashInfo(ctx, txn, []api.KWDBHashRouting{routing})
	return err
}

// IsNodeUpgrading the replica change when node upgrading
func (mr *HRManager) IsNodeUpgrading(
	ctx context.Context, nodeID roachpb.NodeID, tableID uint32,
) ([]api.EntityRangeGroupChange, error) {
	var message []api.EntityRangePartitionMessage
	var groupChanges []api.EntityRangeGroupChange

	hc, ok := mr.routerCaches[tableID]
	if !ok {
		return nil, fmt.Errorf("Can not find table %v hashRouter in HashRingCache", tableID)
	}
	if hc.hashPartitionSize == 0 {
		return nil, errors.Errorf("the table : %v partitions size is 0.", hc.tableID)
	}
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}

	available, msg := allGroupAvailable(hc.groupsMap)
	if !available {
		return nil, errors.Errorf(msg)
	}
	hc.mu.Lock()
	var kwdbHashRoutings []api.KWDBHashRouting
	for _, group := range hc.groupsMap {
		var newLeaseHolderNodeID roachpb.NodeID
		// force find all available nodes in group and set new leaseholder
		if group.LeaseHolder.NodeID == nodeID {
			group.NodeStatus = storagepb.NodeLivenessStatus_UPGRADING
		}
		groupNodeSize := make(map[roachpb.NodeID]int)
		for _, replica := range group.InternalReplicas {
			if nodeStatus.LivenessByNodeID[replica.NodeID] == storagepb.NodeLivenessStatus_LIVE {
				groupNodeSize[replica.NodeID]++
			}
		}
		var destInternalReplicas []api.EntityRangeGroupReplica
		for _, replica := range group.InternalReplicas {
			if replica.NodeID == nodeID {
				group.Status = api.EntityRangeGroupStatus_transferring
				replica.Status = api.EntityRangeGroupReplicaStatus_unavailable
				group.GroupChanges = append(group.GroupChanges, replica)
				if replica.NodeID == group.LeaseHolder.NodeID {
					maxSize := 0
					for k, v := range groupNodeSize {
						if k != nodeID && maxSize < v {
							newLeaseHolderNodeID = k
							maxSize = v
						}
					}
				}
			}
			destInternalReplicas = append(destInternalReplicas, replica)
		}
		if group.Status != api.EntityRangeGroupStatus_transferring {
			continue
		}

		var destLeaseHolder api.EntityRangeGroupReplica
		for _, replica := range group.InternalReplicas {
			if replica.NodeID == newLeaseHolderNodeID {
				group.LeaseHolderChange = replica
				destLeaseHolder = group.LeaseHolderChange
			}
		}
		if group.LeaseHolderChange.ReplicaID == 0 {
			destLeaseHolder = group.LeaseHolder
		}
		var gc api.EntityRangeGroupChange
		for _, partition := range group.Partitions {
			partitionMsg := api.EntityRangePartitionMessage{
				Partition:            partition,
				SrcLeaseHolder:       group.LeaseHolder,
				SrcInternalReplicas:  group.InternalReplicas,
				DestLeaseHolder:      destLeaseHolder,
				DestInternalReplicas: destInternalReplicas,
			}
			message = append(message, partitionMsg)
			gc.Messages = append(gc.Messages, partitionMsg)
		}
		// write to disk
		routing := api.KWDBHashRouting{
			EntityRangeGroupId: group.GroupID,
			TableID:            tableID,
			EntityRangeGroup:   *group,
			TsPartitionSize:    int32(hc.hashPartitionSize),
		}
		kwdbHashRoutings = append(kwdbHashRoutings, routing)
		gc.Routing = routing
		groupChanges = append(groupChanges, gc)
	}
	hc.mu.Unlock()

	return groupChanges, nil
}

// NodeRecover the replica change when unhealthy node rejoin
func (mr *HRManager) NodeRecover(
	ctx context.Context, nodeID roachpb.NodeID, tableID uint32,
) ([]api.EntityRangeGroupChange, error) {
	var message []api.EntityRangePartitionMessage
	var groupChanges []api.EntityRangeGroupChange

	hc, ok := mr.routerCaches[tableID]
	if !ok {
		return nil, fmt.Errorf("Can not find table %v hashRouter in HashRingCache", tableID)
	}
	available, msg := allGroupAvailable(hc.groupsMap)
	if !available {
		return nil, errors.Errorf(msg)
	}
	if hc.hashPartitionSize == 0 {
		return nil, errors.Errorf("the table : %v partitions size is 0.", hc.tableID)
	}
	replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(hrMgr.execConfig.SV()))
	var kwdbHashRoutings []api.KWDBHashRouting
	hc.mu.Lock()
	for _, group := range hc.groupsMap {
		var destInternalReplicas []api.EntityRangeGroupReplica
		destLeaseHolder := group.LeaseHolder
		availableCnt := 0
		for _, replica := range group.InternalReplicas {
			if replica.Status == api.EntityRangeGroupReplicaStatus_unavailable {
				if replica.NodeID == nodeID {
					group.Status = api.EntityRangeGroupStatus_transferring
					replica.Status = api.EntityRangeGroupReplicaStatus_available
					group.GroupChanges = append(group.GroupChanges, replica)
					availableCnt++
				}
			} else {
				availableCnt++
			}
			destInternalReplicas = append(destInternalReplicas, replica)
		}
		if group.Status != api.EntityRangeGroupStatus_transferring {
			continue
		}
		if group.LeaseHolder.Status != api.EntityRangeGroupReplicaStatus_available &&
			availableCnt == replicaNum/2+1 && group.LeaseHolderChange.NodeID == 0 {
			group.LeaseHolderChange = group.GroupChanges[0]
		}
		gc := api.EntityRangeGroupChange{}

		for _, partition := range group.Partitions {
			partitionMsg := api.EntityRangePartitionMessage{
				Partition:            partition,
				SrcLeaseHolder:       group.LeaseHolder,
				SrcInternalReplicas:  group.InternalReplicas,
				DestLeaseHolder:      destLeaseHolder,
				DestInternalReplicas: destInternalReplicas,
			}
			message = append(message, partitionMsg)
			gc.Messages = append(gc.Messages, partitionMsg)

		}

		// write to disk
		routing := api.KWDBHashRouting{
			EntityRangeGroupId: group.GroupID,
			TableID:            tableID,
			EntityRangeGroup:   *group,
			TsPartitionSize:    int32(hc.hashPartitionSize),
		}
		kwdbHashRoutings = append(kwdbHashRoutings, routing)

		gc.Routing = routing
		groupChanges = append(groupChanges, gc)
	}
	hc.mu.Unlock()

	return groupChanges, nil
}

// RefreshHashRouterWithSingleGroup calculate the new group distribute from groupChange and write to disk
func (mr *HRManager) RefreshHashRouterWithSingleGroup(
	ctx context.Context,
	tableID uint32,
	txn *kv.Txn,
	msg string,
	groupChange api.EntityRangeGroupChange,
) error {
	log.Infof(ctx, "refresh table %v hash router, detail:%v ", tableID, msg)
	ring, ok := mr.routerCaches[tableID]
	if !ok {
		return fmt.Errorf("Can not find table %v hashRouter in HashRingCache", tableID)
	}
	if ring.hashPartitionSize == 0 {
		return errors.Errorf("the table : %v partitions size is 0.", ring.tableID)
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	var hashRoutings []api.KWDBHashRouting
	// Cache group information and update memory after successful write to prevent inability to retry after write failure, ensuring idempotence
	changedGroups := make(map[api.EntityRangeGroupID]api.EntityRangeGroup)
	id := groupChange.Routing.EntityRangeGroupId
	group, ok := ring.groupsMap[id]
	if !ok {
		return fmt.Errorf("The table %v EntityRangeGroup %v  is not exist", tableID, id)
	}

	{
		newGroup := *group
		isUpdated := true
		switch newGroup.Status {
		case api.EntityRangeGroupStatus_relocating:
			var changePartitionID []uint32
			for _, partitionChange := range newGroup.PartitionChanges {
				ring.groupsMap[partitionChange.GroupID].Partitions[partitionChange.PartitionID] = partitionChange.Partition
				changePartitionID = append(changePartitionID, partitionChange.PartitionID)
			}
			for _, id := range changePartitionID {
				delete(newGroup.Partitions, id)
			}
			newGroup.PartitionChanges = nil
			for _, groupChange := range newGroup.GroupChanges {
				isAdd := true
				for i, replica := range newGroup.InternalReplicas {
					if groupChange.ReplicaID == replica.ReplicaID {
						isAdd = false
						newGroup.InternalReplicas[i].NodeID = groupChange.NodeID
						newGroup.InternalReplicas[i].StoreID = groupChange.StoreID
					}
					if groupChange.ReplicaID == newGroup.LeaseHolder.ReplicaID {
						newGroup.LeaseHolder.NodeID = groupChange.NodeID
						newGroup.LeaseHolder.StoreID = groupChange.StoreID
					}
				}
				if isAdd {
					newGroup.InternalReplicas = append(newGroup.InternalReplicas, groupChange)
				}
			}
			newGroup.GroupChanges = nil
			newGroup.Status = api.EntityRangeGroupStatus_Available
		case api.EntityRangeGroupStatus_transferring:
			if newGroup.LeaseHolderChange.ReplicaID != 0 { // if leaseholder change is needed
				if newGroup.PreviousLeaseHolder.NodeID == 0 { // = 0, as a new transfer
					newGroup.PreviousLeaseHolder = newGroup.LeaseHolder
				} else { // else, as back transfer to previous leaseholder, then reset prev to be 0 again, which denotes going back to last healthy status
					newGroup.PreviousLeaseHolder = api.EntityRangeGroupReplica{}
				}
				newGroup.LeaseHolder = newGroup.LeaseHolderChange
				newGroup.LeaseHolderChange = api.EntityRangeGroupReplica{}
			}
			for _, change := range newGroup.GroupChanges {
				for i, replica := range newGroup.InternalReplicas {
					if replica.ReplicaID == change.ReplicaID {
						newGroup.InternalReplicas[i] = change
						break
					}
				}
			}
			newGroup.GroupChanges = nil
			newGroup.Status = api.EntityRangeGroupStatus_Available
		case api.EntityRangeGroupStatus_adding:
			// if leaseholder node status of this group is dead, clear previous lease holder
			if newGroup.PreviousLeaseHolder.NodeID != 0 {
				newGroup.PreviousLeaseHolder = api.EntityRangeGroupReplica{}
			}
			for _, change := range newGroup.GroupChanges {
				isAdd := true
				for i, replica := range newGroup.InternalReplicas {
					if replica.ReplicaID == change.ReplicaID {
						isAdd = false
						if change.NodeID == 0 {
							newGroup.InternalReplicas = append(newGroup.InternalReplicas[:i], newGroup.InternalReplicas[i+1:]...)
							break
						} else {
							newGroup.InternalReplicas[i] = change
							break
						}
					}
				}
				if isAdd {
					newGroup.InternalReplicas = append(newGroup.InternalReplicas, change)
				}
			}
			newGroup.GroupChanges = nil
			newGroup.Status = api.EntityRangeGroupStatus_Available
		case api.EntityRangeGroupStatus_lacking:
			for _, change := range newGroup.GroupChanges {
				for i, replica := range newGroup.InternalReplicas {
					if replica.ReplicaID == change.ReplicaID {
						newGroup.InternalReplicas[i] = change
						break
					}
				}
			}
			newGroup.GroupChanges = nil
		default:
			isUpdated = false
		}
		if isUpdated {
			changedGroups[id] = newGroup
			hashRoutings = append(hashRoutings, api.KWDBHashRouting{
				EntityRangeGroupId: id,
				TableID:            tableID,
				EntityRangeGroup:   newGroup,
				TsPartitionSize:    int32(ring.hashPartitionSize),
			})
		}
	}

	err := mr.PutHashInfo(ctx, txn, hashRoutings)
	if err != nil {
		return fmt.Errorf("Put Info To Disk Error : %v ", err)
	}
	for id, group := range ring.groupsMap {
		if len(group.Partitions) == 0 {
			hashRoutings = append(hashRoutings, api.KWDBHashRouting{
				EntityRangeGroupId: id,
				TableID:            tableID,
				EntityRangeGroup:   api.EntityRangeGroup{},
				TsPartitionSize:    int32(ring.hashPartitionSize),
			})
			err = mr.DeleteHashRoutingByID(ctx, txn, uint32(id))
			if err != nil {
				return err
			}
			delete(ring.groupsMap, id)
		}
	}
	for id, newGroup := range changedGroups {
		ring.groupsMap[id] = &newGroup
	}
	log.Infof(ctx, "refresh table %v hash router succeed ", tableID)
	return nil
}

// RefreshHashRouterForGroups calculate the new groups distribute and write to disk without groupChange
func (mr *HRManager) RefreshHashRouterForGroups(
	ctx context.Context,
	tableID uint32,
	txn *kv.Txn,
	msg string,
	nodeStatus storagepb.NodeLivenessStatus,
	groups map[api.EntityRangeGroupID]struct{},
) error {
	log.Infof(ctx, "refresh table %v hash router, detail:%v ", tableID, msg)
	ring, ok := mr.routerCaches[tableID]
	if !ok {
		return fmt.Errorf("The table %v HashRingCache is not exist", tableID)
	}
	ring.mu.Lock()
	defer ring.mu.Unlock()
	for _, group := range ring.groupsMap {
		if group.Status == api.EntityRangeGroupStatus_relocating {
			_, change := groups[group.GroupID]
			if !change {
				group.PartitionChanges = nil
				group.GroupChanges = nil
				group.Status = api.EntityRangeGroupStatus_Available
				group.NodeStatus = nodeStatus
				continue
			}
			var changePartitionID []uint32
			for _, partitionChange := range group.PartitionChanges {
				ring.groupsMap[partitionChange.GroupID].Partitions[partitionChange.PartitionID] = partitionChange.Partition
				changePartitionID = append(changePartitionID, partitionChange.PartitionID)
			}
			for _, id := range changePartitionID {
				delete(group.Partitions, id)
			}
			group.PartitionChanges = nil
			if group.LeaseHolderChange.ReplicaID != 0 { // if leaseholder change is needed
				group.LeaseHolder = group.LeaseHolderChange
				group.LeaseHolderChange = api.EntityRangeGroupReplica{}
			}
			for _, groupChange := range group.GroupChanges {
				isAdd := true
				for i, replica := range group.InternalReplicas {
					if groupChange.ReplicaID == replica.ReplicaID {
						isAdd = false
						group.InternalReplicas[i].NodeID = groupChange.NodeID
						group.InternalReplicas[i].StoreID = groupChange.StoreID
					}
					if groupChange.ReplicaID == group.LeaseHolder.ReplicaID {
						group.LeaseHolder.NodeID = groupChange.NodeID
						group.LeaseHolder.StoreID = groupChange.StoreID
					}
				}
				if isAdd {
					group.InternalReplicas = append(group.InternalReplicas, groupChange)
				}
			}
			group.GroupChanges = nil
			group.Status = api.EntityRangeGroupStatus_Available
		}
		group.NodeStatus = nodeStatus
	}
	if ring.hashPartitionSize == 0 {
		return errors.Errorf("the table : %v partitions size is 0.", ring.tableID)
	}
	var hashRoutings []api.KWDBHashRouting
	for id, group := range ring.groupsMap {
		hashRoutings = append(hashRoutings, api.KWDBHashRouting{
			EntityRangeGroupId: id,
			TableID:            tableID,
			EntityRangeGroup:   *group,
			TsPartitionSize:    int32(ring.hashPartitionSize),
		})
	}

	err := mr.PutHashInfo(ctx, txn, hashRoutings)
	if err != nil {
		return fmt.Errorf("Put Info To Disk Error : %v ", err)
	}
	for id, group := range ring.groupsMap {
		if len(group.Partitions) == 0 {
			hashRoutings = append(hashRoutings, api.KWDBHashRouting{
				EntityRangeGroupId: id,
				TableID:            tableID,
				EntityRangeGroup:   api.EntityRangeGroup{},
				TsPartitionSize:    int32(ring.hashPartitionSize),
			})
			delete(ring.groupsMap, id)
			err = mr.DeleteHashRoutingByID(ctx, txn, uint32(id))
			if err != nil {
				return err
			}
		}
	}
	log.Infof(ctx, "refresh table %v hash router succeed", tableID)
	return nil
}

// DropTableHashInfo drop the table distribute from the disk
func (mr *HRManager) DropTableHashInfo(ctx context.Context, txn *kv.Txn, tableID uint32) error {
	err := mr.DeleteHashRoutingByTableID(ctx, txn, tableID)
	return err
}

// CheckFromDisk refresh the table distribute from manager object
func (mr *HRManager) CheckFromDisk(ctx context.Context, txn *kv.Txn, tableID uint32) error {
	hashRoutings, err := hrMgr.GetHashRoutingsByTableID(ctx, txn, tableID)
	if err != nil {
		return errors.Newf("get table %d hash routing failed: %+v", tableID, err)
	}
	var needRefresh bool
	tableInfo, ok := mr.routerCaches[tableID]
	if !ok {
		needRefresh = true
	}
	for _, info := range hashRoutings {
		if !info.EntityRangeGroup.Equal(tableInfo.groupsMap[info.EntityRangeGroupId]) {
			needRefresh = true
			break
		}
	}
	if needRefresh {
		var hashPartitionSize int
		var hashPartitionNum int64
		groups := map[api.EntityRangeGroupID]*api.EntityRangeGroup{}
		for _, routing := range hashRoutings {
			hashPartitionSize = int(routing.TsPartitionSize)
			hashPartitionNum++
			groups[routing.EntityRangeGroupId] = &routing.EntityRangeGroup
		}
		mr.routerCaches[tableID] = &hashRouterInfo{
			tableID:           tableID,
			hashPartitionNum:  hashPartitionNum,
			hashPartitionSize: hashPartitionSize,
			groupsMap:         groups,
		}
	}
	return nil
}

// PutHashInfo write table kwdb_hash_routing by kv insert.
func (mr *HRManager) PutHashInfo(
	ctx context.Context, txn *kv.Txn, descriptor []api.KWDBHashRouting,
) (err error) {

	hashInfoLog := ""
	// txn is not nil
	if txn != nil {
		err = sql.WriteHashRoutingTableDesc(ctx, txn, descriptor, true)
	} else {
		err = mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
			return sql.WriteHashRoutingTableDesc(ctx, newTxn, descriptor, true)
		})
	}
	if err != nil {
		hashInfoLog = fmt.Sprintf("Push %d Hash Infos Failed. ", len(descriptor))
	} else {
		hashInfoLog = fmt.Sprintf("Push %d Hash Infos Succeed. ", len(descriptor))
	}
	for _, r := range descriptor {
		hashInfoLog += fmt.Sprintf("\t %v", r)
	}
	log.Infof(ctx, hashInfoLog)
	return err
}

// GetHashRoutingByID query kwdb_hash_routing by specific id
func (mr *HRManager) GetHashRoutingByID(
	ctx context.Context, txn *kv.Txn, entityRangeGroupID uint64,
) (*api.KWDBHashRouting, error) {
	// txn is not nil
	if txn != nil {
		return sql.GetKWDBHashRoutingByID(ctx, txn, entityRangeGroupID)
	}

	var info *api.KWDBHashRouting
	var err error
	err = mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
		info, err = sql.GetKWDBHashRoutingByID(ctx, newTxn, entityRangeGroupID)
		return err
	})
	return info, err
}

// GetHashRoutingByIDInTxn query kwdb_hash_routing in a txn flow by specific id,
// it should be called in a txn flow so that do not need specify a txn for it.
func (mr *HRManager) GetHashRoutingByIDInTxn(
	ctx context.Context, entityRangeGroupID uint64, sender kv.Sender, header *roachpb.Header,
) (*api.KWDBHashRouting, error) {
	if header.Txn == nil {
		return nil, errors.New("not in txn")
	}
	return sql.GetKWDBHashRoutingByIDInTxn(ctx, entityRangeGroupID, sender, header)
}

// GetHashRoutingsByTableID query kwdb_hash_routing by specific table id
func (mr *HRManager) GetHashRoutingsByTableID(
	ctx context.Context, txn *kv.Txn, tableID uint32,
) ([]*api.KWDBHashRouting, error) {
	// txn is not nil
	if txn != nil {
		return sql.GetKWDBHashRoutingsByTableID(ctx, txn, tableID)
	}

	var hashRoutings []*api.KWDBHashRouting
	var err error
	err = mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
		hashRoutings, err = sql.GetKWDBHashRoutingsByTableID(ctx, newTxn, tableID)
		return err
	})
	return hashRoutings, err
}

// RebalancedReplica rebalance this table replica
func (hc *hashRouterInfo) RebalancedReplica(
	ctx context.Context, txn *kv.Txn, tableID uint32,
) ([]api.EntityRangePartitionMessage, error) {
	var message []api.EntityRangePartitionMessage
	nodeStatus, err := hrMgr.execConfig.StatusServer.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, err
	}
	var nodeList []roachpb.NodeID
	for _, n := range nodeStatus.Nodes {
		// Filter unavailable nodes
		if nodeStatus.LivenessByNodeID[n.Desc.NodeID] == storagepb.NodeLivenessStatus_LIVE {
			nodeList = append(nodeList, n.Desc.NodeID)
		}
	}

	available, msg := allGroupAvailable(hc.groupsMap)
	if !available {
		return nil, errors.Errorf("The err : %v ", msg)
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	nodeLeasHolders := make(map[roachpb.NodeID]int)
	nodeReplicas := make(map[roachpb.NodeID]int)
	nodeGroups := make(map[roachpb.NodeID][]api.EntityRangeGroupID)
	nodeLeaseHolderGroups := make(map[roachpb.NodeID][]api.EntityRangeGroupID)
	for _, nodeID := range nodeList {
		nodeLeasHolders[nodeID] = 0
		nodeReplicas[nodeID] = 0
		nodeGroups[nodeID] = []api.EntityRangeGroupID{}
		nodeLeaseHolderGroups[nodeID] = []api.EntityRangeGroupID{}
	}
	for _, group := range hc.groupsMap {
		for _, replica := range group.InternalReplicas {
			if replica.Status != api.EntityRangeGroupReplicaStatus_available {
				continue
			}
			if replica.ReplicaID == group.LeaseHolder.ReplicaID {
				nodeLeasHolders[replica.NodeID]++
				nodeLeaseHolderGroups[replica.NodeID] = append(nodeLeaseHolderGroups[replica.NodeID], group.GroupID)
			}
			nodeReplicas[replica.NodeID]++
			nodeGroups[replica.NodeID] = append(nodeGroups[replica.NodeID], group.GroupID)
		}
	}
	leaseHolderChanged, leaseHolderMessage := rebalanceLeaseHolder(nodeLeasHolders, nodeReplicas, nodeGroups, hc.groupsMap, nodeLeaseHolderGroups)
	message = append(message, leaseHolderMessage...)
	replicaChanged, replicaMessage := rebalanceReplica(nodeReplicas, nodeGroups, hc.groupsMap, nodeLeaseHolderGroups)
	message = append(message, replicaMessage...)

	// write to disk
	if leaseHolderChanged || replicaChanged {
		var kwdbHashRoutings []api.KWDBHashRouting
		for id, group := range hc.groupsMap {
			kwdbHashRoutings = append(kwdbHashRoutings, api.KWDBHashRouting{
				EntityRangeGroupId: id,
				TableID:            tableID,
				EntityRangeGroup:   *group,
				TsPartitionSize:    int32(hc.hashPartitionSize),
			})
		}
		err = hrMgr.PutHashInfo(ctx, txn, kwdbHashRoutings)
		if err != nil {
			return nil, fmt.Errorf("put to disk error : %v", err)
		}
	}
	return message, nil
}

// GetAllHashRoutings query kwdb_hash_routing and fetch all rows
func (mr *HRManager) GetAllHashRoutings(
	ctx context.Context, txn *kv.Txn,
) ([]*api.KWDBHashRouting, error) {
	// txn is not nil
	if txn != nil {
		return sql.GetAllKWDBHashRoutings(ctx, txn)
	}
	var hashRoutings []*api.KWDBHashRouting
	var err error
	err = mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
		hashRoutings, err = sql.GetAllKWDBHashRoutings(ctx, newTxn)
		return err
	})
	return hashRoutings, err
}

// DeleteHashRoutingByID delete row from table kwdb_hash_routing by specific id.
func (mr *HRManager) DeleteHashRoutingByID(ctx context.Context, txn *kv.Txn, id uint32) error {
	if txn != nil {
		return sql.DeleteKWDBHashRoutingByID(ctx, txn, uint64(id))
	}

	return mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
		return sql.DeleteKWDBHashRoutingByID(ctx, newTxn, uint64(id))
	})
}

// DeleteHashRoutingByTableID delete rows from table kwdb_hash_routing by specific table id.
func (mr *HRManager) DeleteHashRoutingByTableID(
	ctx context.Context, txn *kv.Txn, tableID uint32,
) error {
	if txn != nil {
		return sql.DeleteKWDBHashRoutingByTableID(ctx, txn, uint64(tableID))
	}

	return mr.db.Txn(ctx, func(ctx context.Context, newTxn *kv.Txn) error {
		return sql.DeleteKWDBHashRoutingByTableID(ctx, newTxn, uint64(tableID))
	})
}
