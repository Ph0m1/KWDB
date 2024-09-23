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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

type rebalanceTsDataNode struct {
	n *tree.RebalanceTsData
}

// RebalanceTsDataNode rebalance ts data.
func (p *planner) RebalanceTsDataNode(
	ctx context.Context, n *tree.RebalanceTsData,
) (planNode, error) {
	advance := settings.AllowAdvanceDistributeSetting.Get(p.execCfg.SV())
	if !advance {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "rebalance ts data is not support when cluster setting server.advanced_distributed_operations.enabled is false.")
	}
	if p.SessionData().SafeUpdates {
		return nil, pgerror.DangerousStatementf(
			"rebalance replica")
	}
	return &rebalanceTsDataNode{n: n}, nil
}

func (n *rebalanceTsDataNode) startExec(params runParams) error {
	hmr, err := api.GetHashRouterManagerWithTxn(params.ctx, params.p.txn)
	if err != nil {
		return pgerror.Newf(pgcode.InvalidTransactionState, "get hashrouter manager failed :%v", err)
	}
	if n.n.TableName.Table() != "" {
		tableID, err := params.p.ResolveTableName(params.ctx, &n.n.TableName)
		if err != nil {
			return err
		}
		tableDesc, err := params.p.Tables().getTableVersionByID(params.ctx, params.p.txn, sqlbase.ID(tableID), tree.ObjectLookupFlags{})
		defer params.p.Tables().releaseLeases(params.ctx)
		if err != nil {
			return err
		}
		if tableDesc.TableType != tree.TimeseriesTable {
			return errors.Errorf("the table : %v ,is not ts table", tableID)
		}
		table, _, err := sqlbase.GetTsTableDescFromID(params.ctx, params.p.txn, sqlbase.ID(tableID))
		if err != nil {
			return errors.Wrap(err, "[HA]get table failed")
		}
		if !(table != nil && table.State == sqlbase.TableDescriptor_PUBLIC) {
			return errors.Errorf("table %v is not public, current status %v", tableID, table.State)
		}
		info := hmr.GetHashInfoByTableID(params.ctx, uint32(tableID))
		changes, err := info.RebalancedReplica(params.ctx, params.p.txn, uint32(tableID))
		if err != nil {
			return err
		}
		groups := make(map[api.EntityRangeGroupID]struct{})
		for _, partition := range changes {
			log.Infof(params.ctx, "The Group %v src leaseHolder is : %v , the src replica is : %v\n", partition.GroupID, partition.SrcLeaseHolder, partition.SrcInternalReplicas)
			log.Infof(params.ctx, "The Group %v dest leaseHolder is : %v , the dest replica is : %v\n", partition.GroupID, partition.DestLeaseHolder, partition.DestInternalReplicas)
			log.Infof(params.ctx, "The group %v relocate partition success", partition.GroupID)
			groups[partition.GroupID] = struct{}{}
		}
		message := fmt.Sprintf("rebalance ts data table : %v", tableID)
		err = hmr.RefreshHashRouterForGroups(params.ctx, uint32(tableID), params.p.txn, message, storagepb.NodeLivenessStatus_LIVE, groups)
		if err != nil {
			return errors.Errorf("rebalance ts data RefreshHashRouter err : %v", err)
		}
		removeNodes := make(map[roachpb.NodeID]interface{}, 0)
		for _, part := range changes {
			for _, partReplica := range part.SrcInternalReplicas {
				srcNodeID := partReplica.NodeID
				if _, ok := removeNodes[srcNodeID]; !ok {
					removeNodes[srcNodeID] = struct{}{}
					rangeGroups := info.GetGroupIDAndRoleOnNode(params.ctx, srcNodeID)
					err = api.RemoveUnusedTSRangeGroups(params.ctx, uint32(tableID), srcNodeID, rangeGroups)
					if err != nil {
						return errors.Errorf("failed to update ts range group: %v", err)
					}
				}
			}
		}
	} else {
		infos, err := hmr.GetAllHashRouterInfo(params.ctx, params.p.txn)
		if err != nil {
			return err
		}
		for tableID, info := range infos {
			_, err := params.p.Tables().getTableVersionByID(params.ctx, params.p.txn, sqlbase.ID(tableID), tree.ObjectLookupFlags{})
			if err != nil {
				params.p.Tables().releaseLeases(params.ctx)
				return err
			}
			table, _, err := sqlbase.GetTsTableDescFromID(params.ctx, params.p.txn, sqlbase.ID(tableID))
			if err != nil {
				params.p.Tables().releaseLeases(params.ctx)
				return errors.Wrap(err, "[HA]get table failed")
			}
			if !(table != nil && table.State == sqlbase.TableDescriptor_PUBLIC) {
				params.p.Tables().releaseLeases(params.ctx)
				return errors.Errorf("table %v is not public, current status %v", tableID, table.State)
			}
			changes, err := info.RebalancedReplica(params.ctx, params.p.txn, tableID)
			if err != nil {
				params.p.Tables().releaseLeases(params.ctx)
				return err
			}
			groups := make(map[api.EntityRangeGroupID]struct{})
			for _, partition := range changes {
				log.Infof(params.ctx, "The group : %v\n", partition.GroupID)
				log.Infof(params.ctx, "The src leaseHolder is : %+v , the src replica is : %+v\n", partition.SrcLeaseHolder, partition.SrcInternalReplicas)
				log.Infof(params.ctx, "The dest leaseHolder is : %+v , the dest replica is : %+v\n", partition.DestLeaseHolder, partition.DestInternalReplicas)
				groups[partition.GroupID] = struct{}{}
			}
			message := fmt.Sprintf("rebalance ts data table : %v", tableID)
			err = hmr.RefreshHashRouterForGroups(params.ctx, tableID, params.p.txn, message, storagepb.NodeLivenessStatus_LIVE, groups)
			if err != nil {
				params.p.Tables().releaseLeases(params.ctx)
				return errors.Errorf("rebalance ts data RefreshHashRouter err : %v", err)
			}
			removeNodes := make(map[roachpb.NodeID]interface{}, 0)
			for _, part := range changes {
				for _, partReplica := range part.SrcInternalReplicas {
					srcNodeID := partReplica.NodeID
					if _, ok := removeNodes[srcNodeID]; !ok {
						removeNodes[srcNodeID] = struct{}{}
						rangeGroups := info.GetGroupIDAndRoleOnNode(params.ctx, srcNodeID)
						err = api.RemoveUnusedTSRangeGroups(params.ctx, tableID, srcNodeID, rangeGroups)
						if err != nil {
							params.p.Tables().releaseLeases(params.ctx)
							return errors.Errorf("failed to update ts range group: %v", err)
						}
					}
				}
			}
			params.p.Tables().releaseLeases(params.ctx)
		}
	}
	return nil
}

func (*rebalanceTsDataNode) Next(runParams) (bool, error) { return false, nil }
func (*rebalanceTsDataNode) Values() tree.Datums          { return tree.Datums{} }
func (*rebalanceTsDataNode) Close(context.Context)        {}
