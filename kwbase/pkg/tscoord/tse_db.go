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

package tscoord

import (
	"context"
	"errors"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"google.golang.org/grpc"
)

// TsSender is a Sender to send TS requests to TS DB
type TsSender struct {
	tsEngine   *tse.TsEngine
	wrapped    kv.Sender
	mppMode    bool
	rpcContext *rpc.Context
	gossip     *gossip.Gossip
	stopper    *stop.Stopper
}

// TsDBConfig is config for building TsSender
type TsDBConfig struct {
	KvDB       *kv.DB
	Sender     kv.Sender
	TsEngine   *tse.TsEngine
	RPCContext *rpc.Context
	Gossip     *gossip.Gossip
	Stopper    *stop.Stopper
	MppMode    bool
}

var _ kv.Sender = &TsSender{}

// Send implements the Sender interface.
func (s *TsSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	resp := &roachpb.BatchResponse{}
	if s.mppMode && !ba.Header.ImportTimeSeriesData {
		var putPayload [][]byte
		for _, ru := range ba.Requests {
			r := ru.GetInner()
			switch tdr := r.(type) {
			case *roachpb.TsPutRequest:
				putPayload = append(putPayload, r.(*roachpb.TsPutRequest).Value.RawBytes)
			case *roachpb.TsTagUpdateRequest:
				var pld [][]byte
				pld = append(pld, tdr.Tags)
				err := s.tsEngine.PutEntity(tdr.RangeGroupId, tdr.TableId, pld, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsTagUpdate{
						TsTagUpdate: &roachpb.TsTagUpdateResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: 1},
						},
					},
				})
			case *roachpb.TsDeleteRequest:
				rows, err := s.tsEngine.DeleteData(tdr.TableId, tdr.RangeGroupId, tdr.PrimaryTags, tdr.TsSpans, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDelete{
						TsDelete: &roachpb.TsDeleteResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(rows)},
						},
					},
				})
			case *roachpb.TsDeleteEntityRequest:
				cnt, err := s.tsEngine.DeleteEntities(tdr.TableId, tdr.RangeGroupId, tdr.PrimaryTags, false, 0)
				if err != nil {
					return nil, &roachpb.Error{Message: err.Error()}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDeleteEntity{
						TsDeleteEntity: &roachpb.TsDeleteEntityResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(cnt)},
						},
					},
				})
			case *roachpb.TsDeleteMultiEntitiesDataRequest:
				var deleteRows uint64
				for _, group := range tdr.DelEntityGroups {
					for _, par := range group.Partitions {
						cnt, err := s.tsEngine.DeleteRangeData(tdr.TableId, group.GroupId, par.StartPoint, par.EndPoint, tdr.TsSpans, 0)
						if err != nil {
							return nil, &roachpb.Error{Message: err.Error()}
						}
						deleteRows += cnt
					}
				}
				resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
					Value: &roachpb.ResponseUnion_TsDeleteMultiEntitiesData{
						TsDeleteMultiEntitiesData: &roachpb.TsDeleteMultiEntitiesDataResponse{
							ResponseHeader: roachpb.ResponseHeader{NumKeys: int64(deleteRows)},
						},
					},
				})
			case *roachpb.AdminRelocatePartitionRequest,
				*roachpb.AdminChangePartitionReplicasRequest,
				*roachpb.AdminTransferPartitionLeaseRequest:
				return s.wrapped.Send(ctx, ba)
			default:
				return s.wrapped.Send(ctx, ba)
			}
		}
		if putPayload != nil {
			dedupRes, err := s.tsEngine.PutData(1, putPayload, 0)
			if err != nil {
				// todo need to process dedupResult
				return nil, &roachpb.Error{Message: err.Error()}
			}
			resp.Responses = append(resp.Responses, roachpb.ResponseUnion{
				Value: &roachpb.ResponseUnion_TsPut{
					TsPut: &roachpb.TsPutResponse{
						ResponseHeader: roachpb.ResponseHeader{
							NumKeys: int64(dedupRes.DedupRows),
						},
						DedupRule:     int64(dedupRes.DedupRule),
						DiscardBitmap: dedupRes.DiscardBitmap,
					},
				},
			})
		}
		return resp, nil
	}

	setConsistency := func() {
		for _, ru := range ba.Requests {
			r := ru.GetInner()
			switch r.(type) {
			case *roachpb.TsPutRequest,
				*roachpb.TsDeleteRequest,
				*roachpb.TsDeleteEntityRequest,
				*roachpb.TsDeleteMultiEntitiesDataRequest,
				*roachpb.TsTagUpdateRequest:
				ba.Header.ReadConsistency = roachpb.READ_UNCOMMITTED
			}
		}
	}

	setConsistency()
	return s.wrapped.Send(ctx, ba)
}

func (s *TsSender) tsRequestLease(
	ctx context.Context, key roachpb.Key, nodeID roachpb.NodeID,
) error {
	log.Infof(ctx, "TsRequestLease for key %v to node %v", key, nodeID)
	addr, err := s.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}
	var conn *grpc.ClientConn
	conn, err = s.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}
	client := serverpb.NewAdminClient(conn)
	req := &serverpb.TsRequestLeaseRequest{
		Key: key,
	}
	if _, err = client.TsRequestLease(ctx, req); err != nil {
		log.Errorf(ctx, "TsRequestLease error :%v", err)
	}
	return err
}

// DB is a database handle to a single ts cluster. A DB is safe for
// concurrent use by multiple goroutines.
type DB struct {
	kdb *kv.DB
	tss *TsSender
}

// NewDB returns a new DB.
func NewDB(cfg TsDBConfig) *DB {
	tsDB := DB{
		kdb: cfg.KvDB,
		tss: &TsSender{
			tsEngine:   cfg.TsEngine,
			wrapped:    cfg.Sender,
			rpcContext: cfg.RPCContext,
			gossip:     cfg.Gossip,
			stopper:    cfg.Stopper,
			mppMode:    cfg.MppMode,
		},
	}
	return &tsDB
}

// Run executes the operations queued up within a batch. Before executing any
// of the operations the batch is first checked to see if there were any errors
// during its construction (e.g. failure to marshal a proto message).
//
// The operations within a batch are run in parallel and the order is
// non-deterministic. It is an unspecified behavior to modify and retrieve the
// same key within a batch.
//
// Upon completion, Batch.Results will contain the results for each
// operation. The order of the results matches the order the operations were
// added to the batch.
func (db *DB) Run(ctx context.Context, b *kv.Batch) error {
	for _, r := range b.Results {
		if r.Err != nil {
			return r.Err
		}
	}
	return kv.SendAndFill(ctx, db.Send, b)
}

// Send runs the specified calls synchronously in a single batch and returns
// any errors. Returns (nil, nil) for an empty batch.
func (db *DB) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return db.kdb.SendUsingSender(ctx, ba, db.tss)
}

// GetTableMetaFromRemote returns the meta get from remote node
func (db *DB) GetTableMetaFromRemote(
	ctx context.Context, tableID sqlbase.ID, rangeGroup api.RangeGroup, nodeID roachpb.NodeID,
) ([]byte, error) {
	log.Infof(ctx, "get %v table meta from node %v", tableID, nodeID)
	addr, err := db.tss.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	var conn *grpc.ClientConn
	conn, err = db.tss.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}
	client := serverpb.NewAdminClient(conn)
	req := &serverpb.GetTsTableMetaRequest{
		TableId:    uint64(tableID),
		RangeGroup: rangeGroup,
	}
	resp, err := client.GetTsTableMeta(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("get empty response")
	}
	return resp.TsMeta, err
}

// UpdateTSRangeGroup update the ts range groups for the target table and node
func (db *DB) UpdateTSRangeGroup(
	ctx context.Context,
	tableID sqlbase.ID,
	nodeID roachpb.NodeID,
	rangeGroups []api.RangeGroup,
	tsMeta []byte,
) error {
	log.Infof(ctx, "rangeGroups %v ,UpdateRangeGroup On Node %d", rangeGroups, nodeID)
	addr, err := db.tss.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}
	var conn *grpc.ClientConn
	conn, err = db.tss.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}
	client := serverpb.NewAdminClient(conn)
	req := &serverpb.UpdateRangeGroupRequest{
		TableId:     uint64(tableID),
		RangeGroups: rangeGroups,
		TsMeta:      tsMeta,
	}
	if _, err = client.UpdateRangeGroup(ctx, req); err != nil {
		log.Errorf(ctx, "update rangeGroup failed:%v", err)
	}
	return err
}

// RemoveUnusedTSRangeGroup remove the unused ts range groups of the target table and node
func (db *DB) RemoveUnusedTSRangeGroup(
	ctx context.Context, tableID sqlbase.ID, nodeID roachpb.NodeID, rangeGroups []api.RangeGroup,
) error {
	log.Infof(ctx, "RemoveUnusedTSRangeGroup On Node %d", nodeID)
	addr, err := db.tss.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return err
	}
	var conn *grpc.ClientConn
	conn, err = db.tss.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return err
	}
	client := serverpb.NewAdminClient(conn)
	req := &serverpb.RemoveUnusedRangeGroupsRequest{
		TableId:     uint64(tableID),
		RangeGroups: rangeGroups,
	}
	if _, err = client.RemoveUnusedRangeGroups(ctx, req); err != nil {
		log.Errorf(ctx, "RemoveUnusedRangeGroups error :%v", err)
	}
	return err
}

// TsRequestLease request lease on target range to target node.
func (db *DB) TsRequestLease(ctx context.Context, key roachpb.Key, nodeID roachpb.NodeID) error {
	return db.tss.tsRequestLease(ctx, key, nodeID)
}

// makeAddPartitionReplicasReq make roachpb.AdminChangePartitionReplicasRequest for partition
func makeAddPartitionReplicasReq(
	tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) roachpb.Request {
	hashPartition := change.Partition
	startPoint := hashPartition.StartPoint
	endPoint := hashPartition.EndPoint
	startKey := sqlbase.MakeTsHashPointKey(tableID, uint64(startPoint))
	endKey := sqlbase.MakeTsHashPointKey(tableID, uint64(endPoint))
	srcTargets := change.SrcInternalReplicas
	destTargets := change.DestInternalReplicas
	var addTargets []roachpb.ReplicationTarget
	for _, t := range destTargets {
		found := false
		for _, replicaDesc := range srcTargets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			addTargets = append(addTargets, roachpb.ReplicationTarget{
				NodeID:  t.NodeID,
				StoreID: t.StoreID,
			})
		}
	}
	var ops roachpb.ReplicationChanges
	if len(addTargets) > 0 {
		ops = append(ops, roachpb.MakeReplicationChanges(
			roachpb.ADD_REPLICA,
			addTargets...)...)

		req := &roachpb.AdminChangePartitionReplicasRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
		}
		req.AddChanges(ops...)

		return req
	}
	return nil
}

// makeRemovePartitionReplicasReq make roachpb.AdminChangePartitionReplicasRequest for partition
func makeRemovePartitionReplicasReq(
	tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) roachpb.Request {
	hashPartition := change.Partition
	startPoint := hashPartition.StartPoint
	endPoint := hashPartition.EndPoint
	startKey := sqlbase.MakeTsHashPointKey(tableID, uint64(startPoint))
	endKey := sqlbase.MakeTsHashPointKey(tableID, uint64(endPoint))
	srcTargets := change.SrcInternalReplicas
	destTargets := change.DestInternalReplicas
	var removeTargets []roachpb.ReplicationTarget
	for _, replicaDesc := range srcTargets {
		found := false
		for _, t := range destTargets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			removeTargets = append(removeTargets, roachpb.ReplicationTarget{
				NodeID:  replicaDesc.NodeID,
				StoreID: replicaDesc.StoreID,
			})
		}
	}
	var ops roachpb.ReplicationChanges
	if len(removeTargets) > 0 {
		ops = append(ops, roachpb.MakeReplicationChanges(
			roachpb.REMOVE_REPLICA,
			removeTargets...)...)

		req := &roachpb.AdminChangePartitionReplicasRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    startKey,
				EndKey: endKey,
			},
		}
		req.AddChanges(ops...)
		return req
	}
	return nil
}

// makeTransferPartitionLeaseReq make roachpb.AdminTransferPartitionLeaseRequest for partition
func makeTransferPartitionLeaseReq(
	tableID sqlbase.ID, hashPartition api.HashPartition, dest api.EntityRangeGroupReplica,
) roachpb.Request {
	startPoint := hashPartition.StartPoint
	endPoint := hashPartition.EndPoint
	startKey := sqlbase.MakeTsHashPointKey(tableID, uint64(startPoint))
	endKey := sqlbase.MakeTsHashPointKey(tableID, uint64(endPoint))
	req := &roachpb.AdminTransferPartitionLeaseRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		Target: dest.StoreID,
	}
	return req
}

// makeRelocatePartitionReq make roachpb.AdminPartitionStatusRequest for partition
func makeRelocatePartitionReq(
	tableID sqlbase.ID,
	hashPartition api.HashPartition,
	dest api.EntityRangePartitionMessage,
	uselessRange bool,
) roachpb.Request {
	var targets []roachpb.ReplicationTarget
	targets = append(targets, roachpb.ReplicationTarget{NodeID: dest.DestLeaseHolder.NodeID, StoreID: dest.DestLeaseHolder.StoreID})
	for _, replica := range dest.DestInternalReplicas {
		if replica.NodeID != dest.DestLeaseHolder.NodeID {
			targets = append(targets, roachpb.ReplicationTarget{NodeID: replica.NodeID, StoreID: replica.StoreID})
		}
	}
	startPoint := hashPartition.StartPoint
	endPoint := hashPartition.EndPoint
	startKey := sqlbase.MakeTsHashPointKey(tableID, uint64(startPoint))
	endKey := sqlbase.MakeTsHashPointKey(tableID, uint64(endPoint))
	req := &roachpb.AdminRelocatePartitionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		Targets:      targets,
		UselessRange: uselessRange,
	}
	return req
}

// makeGetReplicaStatusRequest make roachpb.GetReplicaStatusRequest for partition
func makeGetReplicaStatusRequest(
	tableID sqlbase.ID, hashPartition api.HashPartition,
) roachpb.Request {
	startPoint := hashPartition.StartPoint
	endPoint := hashPartition.EndPoint
	startKey := sqlbase.MakeTsHashPointKey(tableID, uint64(startPoint))
	endKey := sqlbase.MakeTsHashPointKey(tableID, uint64(endPoint))
	req := &roachpb.GetReplicaStatusRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
	}
	return req
}

// AddPartitionReplicas adding a set of replicas to all ranges for partition
func (db *DB) AddPartitionReplicas(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) error {
	b := &kv.Batch{}

	req := makeAddPartitionReplicasReq(tableID, change)
	if req == nil {
		return nil
	}

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	// some errors can be handled by retry, return err if have tried 5 times.
	for i := 1; i <= 5; i++ {
		err := getOneErr(db.Run(ctx, b), b)
		if err == nil {
			return nil
		} else if i == 5 {
			return err
		}
	}

	return nil
}

// RemovePartitionReplicas removing a set of replicas to all ranges for partition
func (db *DB) RemovePartitionReplicas(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) error {
	b := &kv.Batch{}

	req := makeRemovePartitionReplicasReq(tableID, change)
	//fmt.Println(ctx, "xxxx db.relocate %v, %v, %v",  req.Header().Key, req.Header().EndKey,req)
	if req == nil {
		return nil
	}

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	// some errors can be handled by retry, return err if have tried 5 times.
	for i := 1; i <= 5; i++ {
		err := getOneErr(db.Run(ctx, b), b)
		if err == nil {
			return nil
		} else if i == 5 {
			return err
		}
	}

	return nil
}

// TransferPartitionLease to control all ranges lease for partition.
func (db *DB) TransferPartitionLease(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) error {
	b := &kv.Batch{}

	req := makeTransferPartitionLeaseReq(tableID, change.Partition, change.DestLeaseHolder)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	return getOneErr(db.Run(ctx, b), b)
}

// RelocatePartition relocates the replicas for a range onto the specified
// list of stores based on hash distribution info.
// NOTE: currently, only used in test
func (db *DB) RelocatePartition(
	ctx context.Context,
	tableID sqlbase.ID,
	change api.EntityRangePartitionMessage,
	uselessRange bool,
) error {
	b := &kv.Batch{}
	req := makeRelocatePartitionReq(tableID, change.Partition, change, uselessRange)
	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)
	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		log.Warning(ctx, "batch %v ,relocate failed, err is +%v", b, err)
	}
	return err
}

// IsReadyForTransferPartitionLease return transfer lease is complete for partition
func (db *DB) IsReadyForTransferPartitionLease(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage, gap uint64,
) (bool, error) {
	if change.SrcLeaseHolder.NodeID == change.DestLeaseHolder.NodeID {
		return true, nil
	}
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false, err
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false, errors.New("IsReadyForTransferPartitionLease get statusResponse failed")
	}
	resp := makeStatusResp(replicasResp.ReplicaStatus, true, change)

	canApply := gapCanApply(resp, gap)
	return canApply, nil
}

// GetPartitionCandidate return leader candidate
// NOTICE: replica.StoreID might be wrong
func (db *DB) GetPartitionCandidate(
	ctx context.Context, tableID sqlbase.ID, hashPartition api.HashPartition,
) (api.EntityRangeGroupReplica, error) {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, hashPartition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return api.EntityRangeGroupReplica{}, err
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return api.EntityRangeGroupReplica{}, errors.New("GetPartitionCandidate get statusResponse failed")
	}

	resp := makeStatusResp(replicasResp.ReplicaStatus, false, api.EntityRangePartitionMessage{})

	nodeID := getCandidateNodeID(resp)
	if nodeID == 0 {
		return api.EntityRangeGroupReplica{}, errors.New("GetPartitionCandidate get candidate nodeID failed")
	}
	// NOTICE: replica.StoreID might be wrong
	candidate := api.EntityRangeGroupReplica{NodeID: nodeID, StoreID: roachpb.StoreID(nodeID)}
	return candidate, nil
}

// RelocateCheck check members and lease
func (db *DB) RelocateCheck(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) bool {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false
	}
	rangeStatusMap := makeRangeStatusMap(replicasResp.ReplicaStatus, false, change)

	at := requireLeaseAt(rangeStatusMap, change.SrcLeaseHolder)
	have := requireMembers(rangeStatusMap, change.SrcInternalReplicas)
	if !at || !have {
		log.Infof(ctx, "RelocateCheck table %v, change %v, curReplicas %v",
			tableID, change, rangeStatusMap)
	}
	return at && have
}

// TransferCheck check members and lease
func (db *DB) TransferCheck(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) bool {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false
	}
	rangeStatusMap := makeRangeStatusMap(replicasResp.ReplicaStatus, false, change)

	have := requireMembersIncludeOne(rangeStatusMap, change.DestLeaseHolder)
	if !have {
		log.Infof(ctx, "TransferCheck table %v, change %v, curReplicas %v",
			tableID, change, rangeStatusMap)
	}
	return have
}

// TransferDoneCheck check transfer lease done
func (db *DB) TransferDoneCheck(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) bool {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false
	}
	rangeStatusMap := makeRangeStatusMap(replicasResp.ReplicaStatus, false, change)

	at := requireLeaseAt(rangeStatusMap, change.DestLeaseHolder)
	if !at {
		log.Infof(ctx, "TransferDoneCheck table %v, change %v, curReplicas %v",
			tableID, change, rangeStatusMap)
	}
	return at
}

// AddReplicasCheck check members and lease
func (db *DB) AddReplicasCheck(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) bool {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false
	}
	rangeStatusMap := makeRangeStatusMap(replicasResp.ReplicaStatus, false, change)

	srcTargets := change.SrcInternalReplicas
	destTargets := change.DestInternalReplicas
	var add []api.EntityRangeGroupReplica
	for _, t := range destTargets {
		found := false
		for _, replicaDesc := range srcTargets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			add = append(add, t)
		}
	}

	// current replicas members no replica that need to be added
	notHave := requireMembersNotIncludeMulti(rangeStatusMap, add)
	if !notHave {
		log.Infof(ctx, "AddReplicasCheck table %v, change %v, addReplicas %v, curReplicas: %v",
			tableID, change, add, rangeStatusMap)
	}
	return notHave
}

// RemoveReplicasCheck check members and lease
func (db *DB) RemoveReplicasCheck(
	ctx context.Context, tableID sqlbase.ID, change api.EntityRangePartitionMessage,
) bool {
	b := &kv.Batch{}

	req := makeGetReplicaStatusRequest(tableID, change.Partition)

	b.AppendReqs(req)
	b.InitResult(1, 1, false, nil)

	err := getOneErr(db.Run(ctx, b), b)
	if err != nil {
		return false
	}

	replicasResp := b.RawResponse().Responses[0].GetInner().(*roachpb.GetReplicaStatusResponse)
	if len(replicasResp.ReplicaStatus) == 0 {
		return false
	}
	rangeStatusMap := makeRangeStatusMap(replicasResp.ReplicaStatus, false, change)

	srcTargets := change.SrcInternalReplicas
	destTargets := change.DestInternalReplicas
	var remove []api.EntityRangeGroupReplica
	for _, replicaDesc := range srcTargets {
		found := false
		for _, t := range destTargets {
			if replicaDesc.StoreID == t.StoreID && replicaDesc.NodeID == t.NodeID {
				found = true
				break
			}
		}
		if !found {
			remove = append(remove, replicaDesc)
		}
	}

	have := requireMembersIncludeMulti(rangeStatusMap, remove)
	if !have {
		log.Infof(ctx, "RemoveReplicasCheck table %v, change %v, removeReplicas %v, curReplicas %v",
			tableID, change, remove, rangeStatusMap)
	}
	return have
}

// requireLeaseAt require lease at
func requireLeaseAt(
	statusMap map[roachpb.RangeID][]roachpb.ReplicaStatus, lease api.EntityRangeGroupReplica,
) bool {
	for _, status := range statusMap {
		sort.Slice(status, func(i, j int) bool { return status[i].Leader })
		if !(status[0].Leader && status[0].NodeID == lease.NodeID) {
			return false
		}
	}

	return true
}

// requireMembers require members
func requireMembers(
	statusMap map[roachpb.RangeID][]roachpb.ReplicaStatus, replicas []api.EntityRangeGroupReplica,
) bool {
	for _, status := range statusMap {
		sort.Slice(status, func(i, j int) bool { return status[i].NodeID < status[j].NodeID })
		sort.Slice(replicas, func(i, j int) bool { return replicas[i].NodeID < replicas[j].NodeID })
		if len(status) != len(replicas) {
			return false
		}
		for idx, s := range status {
			if s.NodeID != replicas[idx].NodeID {
				return false
			}
		}
	}

	return true
}

// requireMembersIncludeOne require members
func requireMembersIncludeOne(
	statusMap map[roachpb.RangeID][]roachpb.ReplicaStatus, replica api.EntityRangeGroupReplica,
) bool {
	for _, status := range statusMap {
		have := false
		for _, s := range status {
			if !s.Leader && s.NodeID == replica.NodeID {
				have = true
				break
			}
		}
		if !have {
			return false
		}
	}

	return true
}

// requireMembersNotIncludeMulti require members
func requireMembersNotIncludeMulti(
	statusMap map[roachpb.RangeID][]roachpb.ReplicaStatus, replicas []api.EntityRangeGroupReplica,
) bool {
	for _, status := range statusMap {
		for _, replica := range replicas {
			have := false
			for _, s := range status {
				if s.NodeID == replica.NodeID {
					have = true
					break
				}
			}
			if have {
				return false
			}
		}
	}

	return true
}

// requireMembersIncludeMulti require members
func requireMembersIncludeMulti(
	statusMap map[roachpb.RangeID][]roachpb.ReplicaStatus, replicas []api.EntityRangeGroupReplica,
) bool {
	for _, status := range statusMap {
		for _, replica := range replicas {
			have := false
			for _, s := range status {
				if !s.Leader && s.NodeID == replica.NodeID {
					have = true
					break
				}
			}
			if !have {
				return false
			}
		}
	}

	return true
}

// getOneErr returns the error for a single-request Batch that was run.
// runErr is the error returned by Run, b is the Batch that was passed to Run.
func getOneErr(runErr error, b *kv.Batch) error {
	if runErr != nil && len(b.Results) > 0 {
		return b.Results[0].Err
	}
	return runErr
}

// getCandidateNodeID return NodeID
func getCandidateNodeID(resp []rangeStatus) roachpb.NodeID {
	var winner roachpb.NodeID
	voteMap := make(map[roachpb.NodeID]int)
	// Generate voteMap that records the number of node candidates
	for _, r := range resp {
		sort.Sort(r.status)
		nodeID := r.status[0].NodeID
		if _, ok := voteMap[nodeID]; ok {
			voteMap[nodeID]++
		} else {
			voteMap[nodeID] = 1
		}
	}

	if len(voteMap) == 0 {
		return 0
	}

	vote := 0
	for n, v := range voteMap {
		if v > vote {
			vote = v
			winner = n
		}
	}

	return winner
}

// gapCanApply less than gap and can be applied
func gapCanApply(resp []rangeStatus, gap uint64) bool {
	canApply := true
	for _, v := range resp {
		// TODO: len < 2 return false?
		// init start node or missing status
		if len(v.status) < 2 {
			return true
		}
		if v.status[0].LastIndex-v.status[1].LastIndex <= gap {
			continue
		}
		canApply = false
	}

	return canApply
}

type rangeStatus struct {
	status statsSlice
}

// statsSlice implements sort.Interface.
type statsSlice []roachpb.ReplicaStatus

func (s statsSlice) Len() int      { return len(s) }
func (s statsSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s statsSlice) Less(i, j int) bool {
	if s[i].LastIndex == s[j].LastIndex {
		if s[i].ApplyIndex == s[j].ApplyIndex {
			return s[i].Leader
		}
		return s[i].ApplyIndex > s[j].ApplyIndex
	}
	return s[i].LastIndex > s[j].LastIndex
}

// makeStatusResp reorganize the resp structure
func makeStatusResp(
	replicaStatus []roachpb.ReplicaStatus, needFilter bool, change api.EntityRangePartitionMessage,
) []rangeStatus {
	rangeStatusMap := makeRangeStatusMap(replicaStatus, needFilter, change)

	rangeStatuses := make([]rangeStatus, 0, len(rangeStatusMap))
	var rs rangeStatus
	for _, s := range rangeStatusMap {
		rs.status = s
		rangeStatuses = append(rangeStatuses, rs)
	}
	return rangeStatuses
}

func makeRangeStatusMap(
	replicaStatus []roachpb.ReplicaStatus, needFilter bool, change api.EntityRangePartitionMessage,
) map[roachpb.RangeID][]roachpb.ReplicaStatus {
	// TODO: reorganize the status structure with rangeID
	// range1, replica1	=>	range1: replica1, replica2
	// range1, replica2			range2: replica1, replica2
	// range2, replica1			rangeN: ...
	// rangeN, ...
	rangeStatusMap := make(map[roachpb.RangeID][]roachpb.ReplicaStatus)
	for _, s := range replicaStatus {
		rangeID := s.RangeID
		// NOTICE: all replicas is 0 ?
		// {0, 0, 0, false, 0, 0}, {0, 0, 0, false, 0, 0}, {0, 0, 0, false, 0, 0}
		if rangeID != 0 {
			if needFilter {
				// TODO: if != 2??
				if s.Leader || s.NodeID == change.SrcLeaseHolder.NodeID || s.NodeID == change.DestLeaseHolder.NodeID {
					rangeStatusMap[rangeID] = append(rangeStatusMap[rangeID], s)
				}
			} else {
				rangeStatusMap[rangeID] = append(rangeStatusMap[rangeID], s)
			}
		}
	}
	return rangeStatusMap
}
