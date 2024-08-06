// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package server

import (
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// startUpdateRangeGroup update range groups when restart node.
func (s *Server) startUpdateRangeGroup(ctx context.Context) {
	if s.InitialBoot() {
		return
	}
	log.Infof(ctx, "sending range groups message when restart")
	hashRouterMgr, err := api.GetHashRouterManagerWithTxn(ctx, nil)
	if err != nil {
		log.Errorf(ctx, "get hashrouter manager failed :%v", err)
	}
	routerCaches, err := hashRouterMgr.GetAllHashRouterInfo(ctx, nil)
	if err != nil {
		log.Error(ctx, err)
	}
	for tableID, hashRouter := range routerCaches {
		rangeGroups := hashRouter.GetGroupIDAndRoleOnNode(ctx, s.NodeID())
		if len(rangeGroups) > 0 {
			log.Infof(ctx, "tableID: %v, range groups:%+v", tableID, rangeGroups)
			err := s.tsEngine.UpdateRangeGroup(tableID, rangeGroups, nil)
			if err != nil {
				log.Error(ctx, err)
			}
		}
	}
	return
}

// startTSExpansion first startup with join, engine is empty; all restart with engine not empty.
func (s *Server) startTSExpansion(ctx context.Context, engines []storage.Engine) error {
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		flag, err := checkWhetherJoin(ctx, s, engines)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "check whether need to ts-join"))
			continue
		}
		if !flag {
			return nil
		}
		break
	}
	s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.PreJoin)
	waitLivenessGossip(ctx, s.nodeLiveness)
	for {
		var joining bool
		for id := roachpb.NodeID(1); id < s.NodeID(); id++ {
			liveness, err := s.nodeLiveness.GetLiveness(id)
			if err != nil || liveness == (storagepb.Liveness{}) {
				if errors.Is(err, kvserver.ErrNoLivenessRecord) {
					continue
				}
				log.Error(ctx, err)
				joining = true
				break
			}
			if liveness.Status == kvserver.Joining || liveness.Status == kvserver.PreJoin {
				joining = true
				break
			}
		}
		if joining {
			time.Sleep(10 * time.Second)
			continue
		}
		break
	}
	s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Joining)
	tableIDs := s.nodeLiveness.GetAllTsTableID(ctx)
	if len(tableIDs) == 0 {
		s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Healthy)
		api.HRManagerWUnLock()
		return nil
	}
	log.Infof(ctx, "get all ts table's id %v", tableIDs)
	api.HRManagerWLock()
	hashRouterMgr, err := api.GetHashRouterManagerWithTxn(ctx, nil)
	if err != nil {
		return errors.Errorf("get hash router manager failed : %v", err)
	}

	var errCh = make(chan error, 1)
	// start data migration asynchronously
	s.startTSMigration(ctx, tableIDs, hashRouterMgr, errCh, false)

	go func() {
		for {
			select {
			case err := <-errCh:
				if err != nil {
					log.Errorf(ctx, "ts joining failed: %v", err)
				} else {
					log.Infof(ctx, "ts joining success")
				}
				api.HRManagerWUnLock()
				// unset node status joining
				s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Healthy)
				return
			case <-s.stopper.ShouldStop():
				api.HRManagerWUnLock()
				// unset node status joining
				s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Healthy)
				return
			default:
			}
		}
	}()
	return nil
}

func waitLivenessGossip(ctx context.Context, nl *kvserver.NodeLiveness) {
	oldLivenesses := nl.GetLivenesses()
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		var needRetry bool
		for _, oldl := range oldLivenesses {
			live, err := nl.IsLive(oldl.NodeID)
			if err != nil {
				log.Errorf(ctx, "err check node %v status : %v", oldl.NodeID, err)
			}
			if !live {
				continue
			}
			if oldl == (storagepb.Liveness{}) {
				log.Error(ctx, errors.New("got empty waiting liveness gossip"))
				oldLivenesses = nl.GetLivenesses()
				needRetry = true
				break
			}
			newl, err := nl.GetLiveness(oldl.NodeID)
			if err != nil {
				if err == kvserver.ErrNoLivenessRecord {
					continue
				}
				log.Error(ctx, err)
				needRetry = true
				break
			}
			if newl.Status == kvserver.Dead || newl.Status == kvserver.Decommissioned {
				continue
			}
			if newl.Expiration == oldl.Expiration {
				log.Errorf(ctx, "new liveness %+v, old liveness %+v", newl, oldl)
				needRetry = true
				break
			}
		}
		if needRetry {
			continue
		}
		break
	}
}

func checkWhetherJoin(ctx context.Context, s *Server, engines []storage.Engine) (bool, error) {
	if !settings.AllowAdvanceDistributeSetting.Get(&s.cfg.Settings.SV) {
		return false, nil
	}
	var res []*api.KWDBHashRouting
	var err error

	if err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		res, err = sql.GetAllKWDBHashRoutings(ctx, txn)
		return err
	}); err != nil {
		return false, err
	}

	if len(res) == 0 {
		return false, nil
	}
	return s.InitialBoot() && len(engines) == 0, nil
}

func checkAllowStart(ctx context.Context, s *Server) error {
	var val int
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			row, err := s.internalExecutor.QueryRow(ctx, "get-max-desc-id", txn, `SELECT max(id) from system.namespace;`)
			if err != nil {
				return err
			}
			if len(row) == 0 || row[0] == tree.DNull {
				return errors.Errorf("row: %v", row)
			}
			val = int(tree.MustBeDInt(row[0]))
			return nil
		})
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "get max descriptor id failed"))
			continue
		}
		// check node nums less than replica num
		if val >= keys.MinNonPredefinedUserDescID {
			if !settings.AllowAdvanceDistributeSetting.Get(&s.cfg.Settings.SV) {
				return pgerror.New(pgcode.FeatureNotSupported, "empty new node joining cluster is not supported now")
			}
			count := s.nodeLiveness.GetNodeCount()
			replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&s.cfg.Settings.SV))
			if count < replicaNum {
				return errors.Errorf("joining new node with user tables is not supported when current nodes [%v]"+
					"is less than the replicas [%v]", count, replicaNum)
			}
		}
		break
	}
	return nil
}

func (s *Server) checkAllowJoin(ctx context.Context) error {
	// check liveness status
	livenesses := s.nodeLiveness.GetLivenesses()
	for _, liveness := range livenesses {
		if liveness.Status != kvserver.Dead && liveness.Status != kvserver.Healthy &&
			liveness.Status != kvserver.Joining && liveness.Status != kvserver.PreJoin &&
			liveness.Status != kvserver.Decommissioned {
			return errors.Errorf("node[%v] is %s, wait and clear the --store directory and join again",
				liveness.NodeID.String(), sql.StatusToString(liveness.Status))
		}
	}

	// check ts replicas
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		routings, err := sql.GetAllKWDBHashRoutings(ctx, txn)
		if err != nil {
			return err
		}

		for _, routing := range routings {
			replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&s.st.SV))
			groupReplicaNum := routing.EntityRangeGroup.AvailableReplicaCnt()
			if groupReplicaNum != replicaNum {
				return errors.Errorf(
					"group %v replicas %v not equal replicaNum %v, wait replica normal"+
						"and clear the --store directory and join again",
					routing.EntityRangeGroupId, groupReplicaNum, replicaNum)
			}
			if routing.EntityRangeGroup.Status != api.EntityRangeGroupStatus_Available &&
				routing.EntityRangeGroup.Status != api.EntityRangeGroupStatus_relocating {
				return errors.Errorf(
					"entity group %v of table %v is not available now, wait "+
						"and clear the --store directory and join again",
					routing.EntityRangeGroupId, routing.TableID)
			}
		}
		return nil
	})
}

func (s *Server) checkAllowDecommission(ctx context.Context) error {
	// check ts replicas
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		routings, err := sql.GetAllKWDBHashRoutings(ctx, txn)
		if err != nil {
			return err
		}

		for _, routing := range routings {
			replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&s.st.SV))
			groupReplicaNum := routing.EntityRangeGroup.AvailableReplicaCnt()
			if groupReplicaNum != replicaNum {
				return errors.Errorf(
					"group %v replicas %v not equal replicaNum %v, wait "+
						"replica normal and decommission again",
					routing.EntityRangeGroupId, groupReplicaNum, replicaNum)
			}
			if routing.EntityRangeGroup.Status != api.EntityRangeGroupStatus_Available &&
				routing.EntityRangeGroup.Status != api.EntityRangeGroupStatus_relocating {
				return errors.Errorf(
					"entity group %v of table %v is not available now, wait "+
						"replica normal and decommission again",
					routing.EntityRangeGroupId, routing.TableID)
			}
		}
		return nil
	})
}

// createStorageMetadata call storage to create entity group
func (s *Server) createStorageMetadata(
	ctx context.Context, tableID uint32, mgr api.HashRouterManager,
) error {
	log.Info(ctx, "create storage metadata")
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		hashRouting, err := sql.GetKWDBHashRoutingsByTableID(ctx, txn, tableID)
		if err != nil {
			return err
		}
		if hashRouting == nil {
			return errors.Errorf("get empty hash info for table %v", tableID)
		}
		var nodeID roachpb.NodeID
		nodeID = hashRouting[0].EntityRangeGroup.LeaseHolder.NodeID
		rangeGroup := api.RangeGroup{
			RangeGroupID: hashRouting[0].EntityRangeGroupId,
			Type:         api.ReplicaType_LeaseHolder,
		}
		meta, err := s.tseDB.GetTableMetaFromRemote(ctx, sqlbase.ID(tableID), rangeGroup, nodeID)
		if err != nil {
			return err
		}
		rangeGroups := mgr.GetTableGroupsOnNodeForAddNode(ctx, tableID, s.NodeID())
		err = s.tsEngine.CreateRangeGroup(uint64(tableID), meta, rangeGroups)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

var decommissionErr error

// startTSDecommission start ts decommissioning
func (s *Server) startTSDecommission(ctx context.Context) error {
	if decommissionErr != nil {
		s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Healthy)
		returnErr := errors.Errorf("ts decommission failed: %s", decommissionErr)
		decommissionErr = nil
		return returnErr
	}
	// skip decommissioning status and decommissioned status
	liveness, err := s.nodeLiveness.GetLiveness(s.NodeID())
	if err != nil {
		log.Error(ctx, err)
		return err
	}
	if liveness.Status == kvserver.Decommissioning || liveness.Status == kvserver.Decommissioned {
		return nil
	}
	// TODO: When decommissioning is aborted, the status should be reset
	//       to healthy, which may trigger repeated decommission checks.
	err = s.checkAllowDecommission(ctx)
	if err != nil {
		return err
	}
	s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Decommissioning)
	log.Infof(ctx, "decommissioning node: [%v]", s.NodeID())

	// decommission node get all tableID
	tableIDs := s.nodeLiveness.GetAllTsTableID(ctx)
	log.Infof(ctx, "get all ts table's id %v", tableIDs)
	api.HRManagerWLock()
	hashRouterMgr, err := api.GetHashRouterManagerWithTxn(ctx, nil)
	if err != nil {
		return errors.Errorf("get hash router manager failed : %v", err)
	}
	// relocate useless range
	log.Infof(ctx, "decommissioning relocateUselessRange start")
	s.relocateUselessRange(context.Background(), hashRouterMgr)

	var errCh = make(chan error, 1)
	errReceiver(s, errCh)
	if len(tableIDs) == 0 {
		errCh <- nil
		return nil
	}
	log.Infof(ctx, "get all ts table's id %v", tableIDs)
	s.startTSMigration(context.Background(), tableIDs, hashRouterMgr, errCh, true)

	return nil
}

func errReceiver(s *Server, errCh chan error) {
	go func() {
		for {
			select {
			case err := <-errCh:
				ctx := context.Background()
				if err != nil {
					log.Errorf(ctx, "ts decommission failed: %v", err)
					decommissionErr = err
					api.HRManagerWUnLock()
					// unset node status joining
					s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Healthy)
					// set decommissioning to live
					_, setErr := s.nodeLiveness.SetDecommissioning(ctx, s.NodeID(), false)
					if setErr != nil {
						log.Errorf(ctx, "error during liveness update %d -> %t, err: %v", s.NodeID(), false, setErr)
					}
					return
				}

				for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
					var replicaCounts map[roachpb.NodeID]int64
					if err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						const pageSize = 10000
						replicaCounts = make(map[roachpb.NodeID]int64)
						replicaCounts[s.NodeID()] = 0
						return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, pageSize,
							func(rows []kv.KeyValue) error {
								rangeDesc := roachpb.RangeDescriptor{}
								for _, row := range rows {
									if err = row.ValueProto(&rangeDesc); err != nil {
										return errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
									}
									for _, re := range rangeDesc.Replicas().All() {
										if _, ok := replicaCounts[re.NodeID]; ok {
											replicaCounts[re.NodeID]++
										}
									}
								}
								return nil
							})
					}); err != nil {
						log.Error(ctx, err)
						continue
					}
					if v, ok := replicaCounts[s.NodeID()]; ok && v == 0 {
						s.nodeLiveness.SetTSNodeLiveness(ctx, s.NodeID(), kvserver.Decommissioned)
						break
					}
					continue
				}
				log.Infof(ctx, "ts decommission success")
				api.HRManagerWUnLock()
				return
			default:
			}
		}
	}()
}

// relocateUselessRange relocate useless range
func (s *Server) relocateUselessRange(ctx context.Context, hashRouterMgr api.HashRouterManager) {
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		log.Infof(ctx, "relocateUselessRange start")
		// get err ts table
		if err := s.db.Txn(ctx, func(txnCtx context.Context, txn *kv.Txn) error {
			// get all ranges
			log.Infof(ctx, "relocateUselessRange ScanMetaKVs start")
			ranges, err := sql.ScanMetaKVs(ctx, txn, roachpb.Span{
				Key:    keys.UserTableDataMin,
				EndKey: keys.MaxKey,
			})
			if err != nil {
				return errors.Wrap(err, "relocateUselessRange ScanMetaKVs")
			}

			// get all ts table
			tsTableMap := make(map[uint32]bool)
			var rangeDesc roachpb.RangeDescriptor
			for _, r := range ranges {
				if err := r.ValueProto(&rangeDesc); err != nil {
					return errors.Wrap(err, "relocateUselessRange valueProto")
				}
				if rangeDesc.GetRangeType() == roachpb.TS_RANGE {
					tsTableMap[rangeDesc.TableId] = true
				}
			}

			// generate random change
			log.Infof(ctx, "relocateUselessRange RandomChange start")
			change, err := hashRouterMgr.RandomChange(ctx, s.storePool.GetStores())
			if err != nil {
				return errors.Wrap(err, "relocateUselessRange RandomChange")
			}

			log.Infof(ctx, "relocateUselessRange RelocatePartition start")
			// relocate all err range use sample change
			for tableID := range tsTableMap {
				allInfo, _ := hashRouterMgr.GetAllHashRouterInfo(ctx, txn)
				if info := allInfo[tableID]; info == nil {
					log.Infof(ctx, "relocateUselessRange RelocatePartition for table %v", tableID)
					err = s.tseDB.RelocatePartition(ctx, sqlbase.ID(tableID), change, true)
					if err != nil {
						return errors.Wrapf(err, "relocateUselessRange RelocatePartition %v", tableID)
					}
				}
			}
			log.Infof(ctx, "relocateUselessRange end")
			return nil
		}); err != nil {
			log.Infof(ctx, "relocateUselessRange db.Txn", err)
			return
		}
	})
}

func haExecLogTags(nodeID roachpb.NodeID, action string) *logtags.Buffer {
	buf := &logtags.Buffer{}
	buf = buf.Add("nodeID", nodeID)
	buf = buf.Add("action", action)
	return buf
}

// startTSMigration start ts data migration.
func (s *Server) startTSMigration(
	ctx context.Context,
	tableIDs []uint32,
	mgr api.HashRouterManager,
	errCh chan error,
	isDecommission bool,
) {
	s.stopper.RunWorker(ctx, func(ctx context.Context) {

		var errArr []error

		for _, tableID := range tableIDs {
			var err error
			err = sqlbase.CheckTableStatusOk(ctx, nil, s.db, tableID, true)
			if err != nil {
				log.Error(ctx, err)
				continue
			}
			var changes []api.EntityRangeGroupChange
			if isDecommission {
				ctx = logtags.AddTags(ctx, haExecLogTags(s.NodeID(), "removeNode"))
				changes, err = runRemoveNode(ctx, s, tableID, mgr)
				if err != nil {
					log.Errorf(ctx, "remove node for table %v failed: %v", tableID, err)
					errArr = append(errArr, err)
					break
				}
				log.Infof(ctx, "get remove node ha change messages of table %v: %+v", tableID, changes)
				if changes == nil {
					continue
				}
			} else {
				ctx = logtags.AddTags(ctx, haExecLogTags(s.NodeID(), "addNode"))
				changes, err = runAddNode(ctx, s, tableID, mgr)
				if err != nil {
					log.Errorf(ctx, "add node for table %v failed: %v", tableID, err)
					errArr = append(errArr, err)
					refreshHashGroupsForTable(ctx, s, mgr, tableID, nil)
					continue
				}
				log.Infof(ctx, "get add node ha change messages of table %v: %+v", tableID, changes)
				if changes == nil {
					continue
				}
			}

			succeedGroups := make(map[api.EntityRangeGroupID]struct{})
			var nodesNeedRemoveGroups = make(map[roachpb.NodeID]struct{})

			for _, change := range changes {
				routing := change.Routing
				err := mgr.PutSingleHashInfoWithLock(ctx, tableID, nil, routing)
				if err != nil {
					errArr = append(errArr, err)
					log.Errorf(ctx, "put table %v group %v single hashInfo failed : %v. change message is %+v",
						tableID, change.Routing.EntityRangeGroupId, err, change)
				}
				for _, msg := range change.Messages {
					for _, srcReplicas := range msg.SrcInternalReplicas {
						for _, dstReplicas := range msg.DestInternalReplicas {
							if srcReplicas.ReplicaID == dstReplicas.ReplicaID && srcReplicas.NodeID != dstReplicas.NodeID {
								nodesNeedRemoveGroups[srcReplicas.NodeID] = struct{}{}
							}
						}
					}
					log.Infof(ctx, "start relocate partition change %+v for table %v", msg, tableID)
					err := s.tseDB.RelocatePartition(ctx, sqlbase.ID(tableID), msg, false)
					if err != nil {
						log.Errorf(ctx, "relocate table %v partition failed: %s. partition messages:%+v", tableID, err.Error(), change)
						errArr = append(errArr, errors.Errorf("relocate partition failed, error: %s", err.Error()))
						continue
					}
					succeedGroups[msg.GroupID] = struct{}{}
					log.Infof(ctx, "relocate partition change %+v for table %v success", msg, tableID)

					log.Infof(ctx, "start refresh hash router %v", succeedGroups)
					refreshHashGroupsForRangeGroup(ctx, s, mgr, tableID, change)
					updatedNodes := make(map[roachpb.NodeID]interface{}, 0)

					for _, partReplica := range msg.DestInternalReplicas {
						destNodeID := partReplica.NodeID
						if _, ok := updatedNodes[destNodeID]; !ok {
							updatedNodes[destNodeID] = struct{}{}
							hashInfo := mgr.GetHashInfoByTableID(ctx, uint32(tableID))
							rangeGroups := hashInfo.GetGroupIDAndRoleOnNode(ctx, destNodeID)
							log.Infof(ctx, "start update range groups %+v on node %v", rangeGroups, destNodeID)
							err := api.RefreshTSRangeGroup(ctx, tableID, destNodeID, rangeGroups, nil)
							if err != nil {
								log.Error(ctx, errors.Wrapf(err, "refresh ts range group failed: %v. failed groups:%+v", err, rangeGroups))
							}
						}
					}

				}

			}
			err = mgr.ReSetHashRouterForWithFailedSingleGroup(ctx, tableID, nil, "", storagepb.NodeLivenessStatus_LIVE, succeedGroups)
			if err != nil {
				log.Error(ctx, errors.Wrap(err, fmt.Sprintf("reset table %v hash router failed. ", tableID)))
			}

			// todo(fxy):
			//    Currently, unused groups are removed at the
			//    table level, not the group level. It may be
			//    changed to erg level in the future.
			for nodeID := range nodesNeedRemoveGroups {
				hashInfo := mgr.GetHashInfoByTableID(ctx, uint32(tableID))
				rangeGroups := hashInfo.GetGroupIDAndRoleOnNode(ctx, nodeID)
				log.Infof(ctx, "start remove useless ts range groups %+v on node %v", rangeGroups, nodeID)
				err := api.RemoveUnusedTSRangeGroups(ctx, tableID, nodeID, rangeGroups)
				if err != nil {
					log.Errorf(ctx, "remove table %v useless range groups %v on node %v failed: %v", tableID, rangeGroups, nodeID, err)
					errArr = append(errArr, errors.Wrapf(err, "remove table %v useless range groups %v on node %v failed", tableID, rangeGroups, nodeID))
				}
				log.Info(ctx, "remove useless ts range groups success")
			}
		}
		if len(errArr) != 0 {
			errCh <- errArr[0]
		} else {
			errCh <- nil
		}
		return
	})
	return
}

func runAddNode(
	ctx context.Context, s *Server, tableID uint32, mgr api.HashRouterManager,
) ([]api.EntityRangeGroupChange, error) {
	changes, err := mgr.AddNode(ctx, s.NodeID(), tableID, s.storePool.GetStores())
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "get ts migration msg: %+v for table %v", changes, tableID)
	err = s.createStorageMetadata(ctx, tableID, mgr)
	if err != nil {
		return nil, err
	}
	return changes, nil
}

func runRemoveNode(
	ctx context.Context, s *Server, tableID uint32, mgr api.HashRouterManager,
) ([]api.EntityRangeGroupChange, error) {
	changes, err := mgr.RemoveNode(ctx, s.NodeID(), tableID, s.storePool.GetStores())
	if err != nil {
		log.Error(ctx, err)
		refreshHashGroupsForTable(ctx, s, mgr, tableID, nil)
		return nil, errors.Errorf("%v, cannot decommission now, please wait for success and try again", err)
	}
	log.Infof(ctx, "get ts migration msg: %+v for table %v", changes, tableID)
	return changes, nil
}

// refreshHashGroupsForTable call RefreshHashRouterForGroups with succeed groups of one table.
func refreshHashGroupsForTable(
	ctx context.Context,
	s *Server,
	mgr api.HashRouterManager,
	tableID uint32,
	groups map[api.EntityRangeGroupID]struct{},
) {
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var message string
			return mgr.RefreshHashRouterForGroups(ctx, tableID, txn, message, storagepb.NodeLivenessStatus_LIVE, groups)
		})
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "refresh hash router"))
			continue
		}
		log.Info(ctx, "refresh hash router success")
		break
	}
}

// refreshHashGroupsForTable call RefreshHashRouterForGroups with succeed groups of one table.
func refreshHashGroupsForRangeGroup(
	ctx context.Context,
	s *Server,
	mgr api.HashRouterManager,
	tableID uint32,
	groupChangee api.EntityRangeGroupChange,
) {
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			var message string
			return mgr.RefreshHashRouterForGroupsWithSingleGroup(ctx, tableID, txn, message, storagepb.NodeLivenessStatus_LIVE, groupChangee)
		})
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "refresh hash router"))
			continue
		}
		log.Info(ctx, "refresh hash router success")
		break
	}
}
