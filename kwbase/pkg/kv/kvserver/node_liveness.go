// Copyright 2016 The Cockroach Authors.
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

package kvserver

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	errors2 "github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/pkg/errors"
)

var (
	// ErrNoLivenessRecord is returned when asking for liveness information
	// about a node for which nothing is known.
	ErrNoLivenessRecord = errors.New("node not in the liveness table")

	errChangeDecommissioningFailed = errors.New("failed to change the decommissioning status")

	// ErrEpochIncremented is returned when a heartbeat request fails because
	// the underlying liveness record has had its epoch incremented.
	ErrEpochIncremented = errors.New("heartbeat failed on epoch increment")

	// ErrEpochAlreadyIncremented is returned by IncrementEpoch when
	// someone else has already incremented the epoch to the desired
	// value.
	ErrEpochAlreadyIncremented = errors.New("epoch already incremented")

	errLiveClockNotLive = errors.New("not live")
)

// note：alter the function sql.StatusToString if alter following const
const (
	// Dead node status is dead.
	Dead = iota
	// PreJoin node status is pre-join.
	PreJoin
	// Joining node status is joining.
	Joining
	// ReJoining node status is rejoin.
	ReJoining
	// Healthy node status is healthy.
	Healthy
	// UnHealthy node status is unhealthy.
	UnHealthy
	// Decommissioning node status is decommissioning.
	Decommissioning
	// Decommissioned node status is decommissioned.
	Decommissioned
	// Upgrading node status is upgrading
	Upgrading
)

type errRetryLiveness struct {
	error
}

func (e *errRetryLiveness) Cause() error {
	return e.error
}

func (e *errRetryLiveness) Error() string {
	return fmt.Sprintf("%T: %s", *e, e.error)
}

// Node liveness metrics counter names.
var (
	metaLiveNodes = metric.Metadata{
		Name:        "liveness.livenodes",
		Help:        "Number of live nodes in the cluster (will be 0 if this node is not itself live)",
		Measurement: "Nodes",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatSuccesses = metric.Metadata{
		Name:        "liveness.heartbeatsuccesses",
		Help:        "Number of successful node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatFailures = metric.Metadata{
		Name:        "liveness.heartbeatfailures",
		Help:        "Number of failed node liveness heartbeats from this node",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaEpochIncrements = metric.Metadata{
		Name:        "liveness.epochincrements",
		Help:        "Number of times this node has incremented its liveness epoch",
		Measurement: "Epochs",
		Unit:        metric.Unit_COUNT,
	}
	metaHeartbeatLatency = metric.Metadata{
		Name:        "liveness.heartbeatlatency",
		Help:        "Node liveness heartbeat latency",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
)

// LivenessMetrics holds metrics for use with node liveness activity.
type LivenessMetrics struct {
	LiveNodes          *metric.Gauge
	HeartbeatSuccesses *metric.Counter
	HeartbeatFailures  *metric.Counter
	EpochIncrements    *metric.Counter
	HeartbeatLatency   *metric.Histogram
}

// IsLiveCallback is invoked when a node's IsLive state changes to true.
// Callbacks can be registered via NodeLiveness.RegisterCallback().
type IsLiveCallback func(nodeID roachpb.NodeID)

// HeartbeatCallback is invoked whenever this node updates its own liveness status,
// indicating that it is alive.
type HeartbeatCallback func(context.Context)

// NodeLiveness is a centralized failure detector that coordinates
// with the epoch-based range system to provide for leases of
// indefinite length (replacing frequent per-range lease renewals with
// heartbeats to the liveness system).
//
// It is also used as a general-purpose failure detector, but it is
// not ideal for this purpose. It is inefficient due to the use of
// replicated durable writes, and is not very sensitive (it primarily
// tests connectivity from the node to the liveness range; a node with
// a failing disk could still be considered live by this system).
//
// The persistent state of node liveness is stored in the KV layer,
// near the beginning of the keyspace. These are normal MVCC keys,
// written by CPut operations in 1PC transactions (the use of
// transactions and MVCC is regretted because it means that the
// liveness span depends on MVCC GC and can get overwhelmed if GC is
// not working. Transactions were used only to piggyback on the
// transaction commit trigger). The leaseholder of the liveness range
// gossips its contents whenever they change (only the changed
// portion); other nodes rarely read from this range directly.
//
// The use of conditional puts is crucial to maintain the guarantees
// needed by epoch-based leases. Both the Heartbeat and IncrementEpoch
// on this type require an expected value to be passed in; see
// comments on those methods for more.
//
// TODO(bdarnell): Also document interaction with draining and decommissioning.
type NodeLiveness struct {
	ambientCtx        log.AmbientContext
	clock             *hlc.Clock
	db                *kv.DB
	engines           []storage.Engine
	gossip            *gossip.Gossip
	livenessThreshold time.Duration
	heartbeatInterval time.Duration
	selfSem           chan struct{}
	st                *cluster.Settings
	otherSem          chan struct{}
	// heartbeatPaused contains an atomically-swapped number representing a bool
	// (1 or 0). heartbeatToken is a channel containing a token which is taken
	// when heartbeating or when pausing the heartbeat. Used for testing.
	heartbeatPaused uint32
	heartbeatToken  chan struct{}
	metrics         LivenessMetrics

	mu struct {
		syncutil.RWMutex
		callbacks         []IsLiveCallback
		nodes             map[roachpb.NodeID]storagepb.Liveness
		heartbeatCallback HeartbeatCallback
	}
}

// NewNodeLiveness returns a new instance of NodeLiveness configured
// with the specified gossip instance.
func NewNodeLiveness(
	ambient log.AmbientContext,
	clock *hlc.Clock,
	db *kv.DB,
	engines []storage.Engine,
	g *gossip.Gossip,
	livenessThreshold time.Duration,
	renewalDuration time.Duration,
	st *cluster.Settings,
	histogramWindow time.Duration,
) *NodeLiveness {
	nl := &NodeLiveness{
		ambientCtx:        ambient,
		clock:             clock,
		db:                db,
		engines:           engines,
		gossip:            g,
		livenessThreshold: livenessThreshold,
		heartbeatInterval: livenessThreshold - renewalDuration,
		selfSem:           make(chan struct{}, 1),
		st:                st,
		otherSem:          make(chan struct{}, 1),
		heartbeatToken:    make(chan struct{}, 1),
	}
	nl.metrics = LivenessMetrics{
		LiveNodes:          metric.NewFunctionalGauge(metaLiveNodes, nl.numLiveNodes),
		HeartbeatSuccesses: metric.NewCounter(metaHeartbeatSuccesses),
		HeartbeatFailures:  metric.NewCounter(metaHeartbeatFailures),
		EpochIncrements:    metric.NewCounter(metaEpochIncrements),
		HeartbeatLatency:   metric.NewLatency(metaHeartbeatLatency, histogramWindow),
	}
	nl.mu.nodes = map[roachpb.NodeID]storagepb.Liveness{}
	nl.heartbeatToken <- struct{}{}

	livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
	nl.gossip.RegisterCallback(livenessRegex, nl.livenessGossipUpdate)

	return nl
}

var errNodeDrainingSet = errors.New("node is already draining")

func (nl *NodeLiveness) sem(nodeID roachpb.NodeID) chan struct{} {
	if nodeID == nl.gossip.NodeID.Get() {
		return nl.selfSem
	}
	return nl.otherSem
}

// SetDraining attempts to update this node's liveness record to put itself
// into the draining state.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (nl *NodeLiveness) SetDraining(ctx context.Context, drain bool, reporter func(int, string)) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		liveness, err := nl.Self()
		if err != nil && err != ErrNoLivenessRecord {
			log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
		}
		err = nl.setDrainingInternal(ctx, liveness, drain, reporter)
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "attempting to set liveness draining status to %v: %v", drain, err)
			}
			continue
		}
		return
	}
}

// SetDead sets node status to dead according to nodeID
func (nl *NodeLiveness) SetDead(ctx context.Context, nodeID roachpb.NodeID) error {

	allowAdvancedDistribute := settings.AllowAdvanceDistributeSetting.Get(&nl.st.SV)
	if !allowAdvancedDistribute {
		return errors.Errorf("Can not set node status to dead when server.allow_advanced_distributed_operations is false. ")
	}

	// this should be the same as sql.StatusToString()
	StatusToStringMap := map[int32]string{
		0: "dead",
		1: "pre-join",
		2: "joining",
		3: "rejoining",
		4: "healthy",
		5: "unhealthy",
		6: "decommissioning",
		7: "decommissioned",
		8: "upgrading",
	}
	replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&nl.st.SV))

	livenesses := nl.GetLivenesses()
	for _, l := range livenesses {
		if l.NodeID == nodeID {
			if l.Status == UnHealthy {

				const tsHACheckInterval = 4 * time.Second
				times := tsHACheckInterval / time.Second
				var err error
				for i := times; i >= 0; i-- {
					err = nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						routings, err := api.GetAllHashRoutings(ctx, txn)
						if err != nil {
							return err
						}
						allRunning := true
						needReplicas := false
						for _, r := range routings {
							if r.EntityRangeGroup.LeaseHolder.NodeID == nodeID {
								return errors.Errorf("EntityRangeGroup %d did not finish the leaseholder transferring",
									r.EntityRangeGroupId)
							}
							for _, repl := range r.EntityRangeGroup.InternalReplicas {
								if repl.NodeID == nodeID && repl.Status == api.EntityRangeGroupReplicaStatus_available {
									return errors.Errorf("EntityRangeGroup %d has replica %d on node %d",
										r.EntityRangeGroup.GroupID, repl.ReplicaID, nodeID)
								}
							}
							if r.EntityRangeGroup.Status != api.EntityRangeGroupStatus_Available {
								allRunning = false
							}
							availaRepCnt := r.EntityRangeGroup.AvailableReplicaCnt()
							if availaRepCnt != replicaNum {
								needReplicas = true
							}
						}
						if allRunning && needReplicas {
							return nil
						}
						if allRunning && !needReplicas {
							hasLeaseholder := false
							hasReplicas := false
							for _, r := range routings {
								if r.EntityRangeGroup.LeaseHolder.NodeID == nodeID {
									hasLeaseholder = true
								}
								if r.EntityRangeGroup.HasReplicaOnNode(nodeID) {
									hasReplicas = true
								}
								if hasLeaseholder || hasReplicas {
									break
								}
							}
							if !hasLeaseholder && !hasReplicas {
								return nil
							}
							return errors.Errorf("The cluster is waiting to transfer")
						}
						return errors.Errorf("The cluster is transferring")
					})

					if err == nil {
						nodeDeadCh <- nodeID
						nl.SetTSNodeLiveness(context.Background(), nodeID, Dead)
						log.Infof(ctx, " setting node %d to dead \n", nodeID)
						return nil
					}
					if i > 0 {
						time.Sleep(time.Second)
						continue
					}
					return err

				}
			} else if l.Status == Dead {
				return errors.Errorf("Node %d status has already been dead", nodeID)
			} else {
				return errors.Errorf("Can not set node %d to dead when it's %s\n", nodeID, StatusToStringMap[l.Status])
			}
		}
	}
	return errors.Errorf("Can not find node %d\n", nodeID)
}

var errNodeStatusSet = errors.New("node status is already set")

// SetTSNodeLiveness attempts to update this node's liveness record to put itself
// into the joining/dead/healthy/unhealthy state.
func (nl *NodeLiveness) SetTSNodeLiveness(
	ctx context.Context, nodeID roachpb.NodeID, status int32,
) {
	log.Infof(ctx, "set node %v to %v", nodeID.String(), status)
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		liveness, err := nl.GetLiveness(nodeID)
		if err != nil && err != ErrNoLivenessRecord {
			log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
		}
		err = nl.setTsStatusInternal(ctx, nodeID, liveness, status)
		if err != nil {
			log.Warningf(ctx, "attempting to set liveness status to %v failed: %v", status, err)
			continue
		}
		return
	}
}

// SetDecommissioning runs a best-effort attempt of marking the the liveness
// record as decommissioning. It returns whether the function committed a
// transaction that updated the liveness record.
func (nl *NodeLiveness) SetDecommissioning(
	ctx context.Context, nodeID roachpb.NodeID, decommission bool,
) (changeCommitted bool, err error) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)

	attempt := func() (bool, error) {
		// Allow only one decommissioning attempt in flight per node at a time.
		// This is required for correct results since we may otherwise race with
		// concurrent `IncrementEpoch` calls and get stuck in a situation in
		// which the cached liveness is has decommissioning=false while it's
		// really true, and that means that SetDecommissioning becomes a no-op
		// (which is correct) but that our cached liveness never updates to
		// reflect that.
		//
		// See https://gitee.com/kwbasedb/kwbase/issues/17995.
		sem := nl.sem(nodeID)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		defer func() {
			<-sem
		}()

		// We need the current liveness in each iteration.
		//
		// We ignore any liveness record in Gossip because we may have to fall back
		// to the KV store anyway. The scenario in which this is needed is:
		// - kill node 2 and stop node 1
		// - wait for node 2's liveness record's Gossip entry to expire on all surviving nodes
		// - restart node 1; it'll never see node 2 in `GetLiveness` unless the whole
		//   node liveness span gets regossiped (unlikely if it wasn't the lease holder
		//   for that span)
		// - can't decommission node 2 from node 1 without KV fallback.
		//
		// See #20863.
		//
		// NB: this also de-flakes TestNodeLivenessDecommissionAbsent; running
		// decommissioning commands in a tight loop on different nodes sometimes
		// results in unintentional no-ops (due to the Gossip lag); this could be
		// observed by users in principle, too.
		//
		// TODO(bdarnell): This is the one place where a range other than
		// the leaseholder reads from this range. Should this read from
		// gossip instead? (I have vague concerns about concurrent reads
		// and timestamp cache pushes causing problems here)
		var oldLiveness storagepb.Liveness
		if err := nl.db.GetProto(ctx, keys.NodeLivenessKey(nodeID), &oldLiveness); err != nil {
			return false, errors.Wrap(err, "unable to get liveness")
		}
		if (oldLiveness == storagepb.Liveness{}) {
			return false, ErrNoLivenessRecord
		}

		// We may have discovered a Liveness not yet received via Gossip. Offer it
		// to make sure that when we actually try to update the liveness, the
		// previous view is correct. This, too, is required to de-flake
		// TestNodeLivenessDecommissionAbsent.
		nl.maybeUpdate(oldLiveness)

		return nl.setDecommissioningInternal(ctx, nodeID, oldLiveness, decommission)
	}

	for {
		changeCommitted, err := attempt()
		if errors.Cause(err) == errChangeDecommissioningFailed {
			continue // expected when epoch incremented
		}
		return changeCommitted, err
	}
}

// SetUpgrading set the node status to Upgrading and transfer leaseholder for HA.
func (nl *NodeLiveness) SetUpgrading(
	ctx context.Context,
	nodeID roachpb.NodeID,
	isMPPMode bool,
	sfg StoreConfig,
	allNodes []roachpb.NodeID,
) (err error) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)

	if isMPPMode {
		_, err = api.GetAvailableNodeIDs(ctx)
		if err != nil {
			return err
		}
		for _, NodeID := range allNodes {
			connect := checkNodeStatus(ctx, sfg, NodeID)
			if !connect {
				return fmt.Errorf("cluster has node unreachable. Can not connect to node %d ", NodeID)
			}
		}
	}
	nl.SetTSNodeLiveness(ctx, nodeID, Upgrading)

	if isMPPMode {
		return nil
	}

	tableIds := nl.GetAllTsTableID(ctx)
	log.Infof(ctx, "get all table id %v", tableIds)
	api.HRManagerWLock()
	hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
	// No need to check rebalance, we will just trigger the transfer
	if err != nil {
		log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
		return err
	}
	ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "setUpgrade"))

	for _, tableID := range tableIds {

		groupChanges, err := hashRouterManager.IsNodeUpgrading(ctx, nodeID, tableID)
		if err != nil {
			log.Warning(ctx, "get node table %v upgrading ha change message failed: %v", tableID, err)
			continue
		}
		log.Infof(ctx, "get node upgrading ha change messages of table %v: %+v", tableID, groupChanges)

		for _, groupChange := range groupChanges {
			r := groupChange.Routing
			err := hashRouterManager.PutSingleHashInfoWithLock(ctx, tableID, nil, r)
			if err != nil {
				log.Errorf(ctx, "put table %v group %v single hashInfo failed : %v. change message is %+v",
					tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
			}

			for _, part := range groupChange.Messages {
				err = api.TransferPartitionLease(ctx, tableID, part)
				if err != nil {
					log.Warning(ctx, "transfer transfer lease of table %v failed: %v. partition messages :%+v", tableID,
						err, part)
					continue
				}
			}
			log.Infof(ctx, "transfer transfer lease of table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)
			log.Infof(ctx, "start refresh table %v dist message", tableID)

			err = nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				message := fmt.Sprintf("nodeId: %d,tableId: %v, node healthy to upgrading", nodeID, tableID)
				return hashRouterManager.RefreshHashRouterWithSingleGroup(ctx, tableID, txn, message, groupChange)
			})

			if err != nil {
				log.Errorf(ctx, "refresh table %v dist message failed: %+v. err ha msg: %+v ", tableID, err, groupChange)
				panic(err)
			}
			// refresh rangeGroup info to ts engine
			nl.UpdateRangeGroups(ctx, sqlbase.ID(tableID), groupChange.Messages, hashRouterManager)
		}
		log.Infof(ctx, "refresh Table %d success", tableID)

	}

	api.HRManagerWUnLock()
	return nil
}

// SetUpgradingComplete changes status to healthy, recover node and transfer partition
func (nl *NodeLiveness) SetUpgradingComplete(
	ctx context.Context, nodeID roachpb.NodeID,
) (err error) {
	ctx = nl.ambientCtx.AnnotateCtx(ctx)
	nl.SetTSNodeLiveness(ctx, nodeID, Healthy)

	tableIds := nl.GetAllTsTableID(ctx)
	log.Infof(ctx, "get all table id %v", tableIds)
	api.HRManagerWLock()
	hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
	if err != nil {
		log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
		return err
	}
	ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "upgradeComplete"))

	for _, tableID := range tableIds {
		groupChanges, recoverErr := hashRouterManager.NodeRecover(ctx, nodeID, tableID)
		if recoverErr != nil {
			log.Warning(ctx, "get node table %v upgrading-complete ha change message failed: %v", tableID, recoverErr)
			continue
		}
		log.Infof(ctx, "get node upgrading-complete ha change messages of table %v: %+v", tableID, groupChanges)
		for _, groupChange := range groupChanges {
			r := groupChange.Routing
			err := hashRouterManager.PutSingleHashInfoWithLock(ctx, tableID, nil, r)
			if err != nil {
				log.Errorf(ctx, "put table %v group %v single hashInfo failed : %v. change message is %+v",
					tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
			}
			for _, msg := range groupChange.Messages {
				transferErr := api.TransferPartitionLease(ctx, tableID, msg)
				// The high availability processing of this Range failed, ignoring this processing.
				if transferErr != nil {

					log.Warning(ctx, "transfer transfer lease of table %v failed: %v. partition messages :%+v", tableID,
						err, groupChange)
					continue
				}
			}

			log.Infof(ctx, "transfer transfer lease of table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)
			log.Infof(ctx, "start refresh table %v dist message", tableID)
			message := fmt.Sprintf("nodeId: %d, tableId: %v, node upgrading to healthy", nodeID, tableID)

			if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				refreshErr := hashRouterManager.RefreshHashRouterWithSingleGroup(ctx, tableID, txn, message, groupChange)
				return refreshErr
			}); err != nil {
				log.Warningf(ctx, "refresh table %v dist message failed: %+v. err ha msg: %+v ", tableID, err, groupChange)
				continue
			}

			nl.UpdateRangeGroups(ctx, sqlbase.ID(tableID), groupChange.Messages, hashRouterManager)
			log.Infof(ctx, "refresh Table %d success", tableID)
		}

	}

	api.HRManagerWUnLock()
	log.Info(ctx, "Set upgrading-complete success")
	return nil
}

func (nl *NodeLiveness) setDrainingInternal(
	ctx context.Context, liveness storagepb.Liveness, drain bool, reporter func(int, string),
) error {
	nodeID := nl.gossip.NodeID.Get()
	sem := nl.sem(nodeID)
	// Allow only one attempt to set the draining field at a time.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
	}
	if reporter != nil && drain && !update.Draining {
		// Report progress to the Drain RPC.
		reporter(1, "liveness record")
	}
	update.Draining = drain
	update.ignoreCache = true

	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		nl.maybeUpdate(actual)
		if actual.Draining == update.Draining {
			return errNodeDrainingSet
		}
		return errors.New("failed to update liveness record")
	}); err != nil {
		if log.V(1) {
			log.Infof(ctx, "updating liveness record: %v", err)
		}
		if err == errNodeDrainingSet {
			return nil
		}
		return err
	}
	nl.maybeUpdate(update.Liveness)
	return nil
}

func (nl *NodeLiveness) setTsStatusInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness storagepb.Liveness, status int32,
) error {
	sem := nl.sem(nodeID)
	// Allow only one attempt to set the status field at a time.
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
	}
	update.Status = status
	update.ignoreCache = true
	if status == Upgrading {
		update.Upgrading = true
	} else {
		update.Upgrading = false
	}

	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		nl.maybeUpdate(actual)
		if actual.Status == update.Status {
			return errNodeStatusSet
		}
		return errors.New("failed to update liveness record")
	}); err != nil {
		if err == errNodeStatusSet {
			return nil
		}
		return err
	}
	nl.maybeUpdate(update.Liveness)
	return nil
}

type livenessUpdate struct {
	storagepb.Liveness
	// When ignoreCache is set, we won't assume that our in-memory cached version
	// of the liveness record is accurate and will use a CPut on the liveness
	// table with whatever the client supplied. This is used for operations that
	// don't want to deal with the inconsistencies of using the cache.
	ignoreCache bool
}

func (nl *NodeLiveness) setDecommissioningInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness storagepb.Liveness, decommission bool,
) (changeCommitted bool, err error) {
	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
	}
	update.Decommissioning = decommission
	update.ignoreCache = true

	var conditionFailed bool
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		conditionFailed = true
		if actual.Decommissioning == update.Decommissioning {
			return nil
		}
		return errChangeDecommissioningFailed
	}); err != nil {
		return false, err
	}
	committed := !conditionFailed && liveness.Decommissioning != decommission
	return committed, nil
}

// GetLivenessThreshold returns the maximum duration between heartbeats
// before a node is considered not-live.
func (nl *NodeLiveness) GetLivenessThreshold() time.Duration {
	return nl.livenessThreshold
}

// IsLive returns whether or not the specified node is considered live based on
// whether or not its liveness has expired regardless of the liveness status. It
// is an error if the specified node is not in the local liveness table.
func (nl *NodeLiveness) IsLive(nodeID roachpb.NodeID) (bool, error) {
	liveness, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false, err
	}
	// NB: We use clock.Now().GoTime() instead of clock.PhysicalTime() in order to
	// consider clock signals from other nodes.
	return liveness.IsLive(nl.clock.Now().GoTime()), nil
}

// checkIsHaController check whether the node is HA controller,
// which is the leaseholder of system.NodeLiveness.
func checkIsHaController(ctx context.Context, db *kv.DB, node roachpb.NodeID) (bool, error) {
	var leaseHolderNodeID roachpb.NodeID
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		key := keys.NodeLivenessPrefix
		b := &kv.Batch{}
		b.AddRawRequest(&roachpb.LeaseInfoRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		})
		if err := txn.Run(ctx, b); err != nil {
			return pgerror.Newf(pgcode.InvalidParameterValue, "message: %s", err)
		}
		resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)
		leaseHolderNodeID = resp.Lease.Replica.NodeID
		return nil
	})
	if err != nil {
		log.Errorf(ctx, "get node-iveness failed, :%v", err)
		return false, err
	}
	if leaseHolderNodeID != node {
		return false, err
	}
	return true, nil
}

// checkNodeStatus check node connect is ok, avoid node heartbeat suspended
func checkNodeStatus(ctx context.Context, sfg StoreConfig, nodeID roachpb.NodeID) bool {
	err := sfg.NodeDialer.ConnHealth(nodeID, rpc.SystemClass)
	if err != nil {
		log.Errorf(ctx, "check Node Status error, error is :%v", err)
		return false
	}
	return true
}

// StartHALivenessCheck start goroutine to check node liveness for ha process.
func (nl *NodeLiveness) StartHALivenessCheck(
	ctx context.Context, stopper *stop.Stopper, sfg StoreConfig,
) {
	tsHACheckInterval := settings.HaLivenessCheckInterval.Get(&nl.st.SV)
	stopper.RunWorker(ctx, func(context.Context) {
		timer := time.NewTimer(tsHACheckInterval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := contextutil.RunWithTimeout(ctx, "ts-ha-check", 2*tsHACheckInterval, func(checkCtx context.Context) error {
					errCh := make(chan error)
					go func() {
						if ok, _ := checkIsHaController(ctx, nl.db, nl.gossip.NodeID.Get()); !ok {
							log.VEventf(ctx, 2, "cannot be node liveness leaseholder")
						} else {
							log.VEvent(checkCtx, 2, "starting liveness check for join/decommission")
							if !needHACheck(checkCtx, nl, sfg) {
								errCh <- nil
								return
							}
							log.VEvent(checkCtx, 2, "starting liveness check for ha")
							livenesses := nl.GetLivenesses()
							threshold := TimeUntilStoreDead.Get(&nl.st.SV)
							now := nl.clock.Now().GoTime()
							for _, liveness := range livenesses {
								if liveness.Status == Decommissioned || liveness.Status == Upgrading {
									continue
								}
								// quit rejoining status(go back to unhealthy) if rejoin fails
								if liveness.Status == ReJoining && !liveness.IsLive(now) {
									nl.SetTSNodeLiveness(context.Background(), liveness.NodeID, UnHealthy)
									nodeUnhealthyCh <- liveness.NodeID
								}
								status := LivenessStatus(liveness, now, threshold)
								switch status {
								case storagepb.NodeLivenessStatus_DECOMMISSIONING:
									if liveness.Status != Decommissioning {
										continue
									}
									if !liveness.IsLive(now) {
										// proactively checking if the heartbeat is alive, there is an unhealthy reception of all nodes due to the leaseholder switch in the liveness range
										if checkNodeStatus(ctx, sfg, liveness.NodeID) {
											log.Infof(checkCtx, "node is not dead, may be too slow, skipping processing node id: %d", liveness.NodeID)
											continue
										}
										log.Warning(checkCtx, "node unhealthy: %d", liveness.NodeID)
										// refresh hash info before restart
										api.HRManagerWLock()
										hashRouterMgr, err := api.GetHashRouterManagerWithTxn(ctx, nil)
										if err != nil {
											log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
											continue
										}
										for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
											err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
												return hashRouterMgr.RefreshHashRouterForAllGroups(ctx, txn, "refresh all tables", storagepb.NodeLivenessStatus_LIVE, nil)
											})
											if err != nil {
												log.Error(ctx, errors.Wrap(err, "refresh hash router"))
												continue
											}
											log.Info(ctx, "refresh hash router success")
											break
										}
										api.HRManagerWUnLock()
										nl.SetTSNodeLiveness(context.Background(), liveness.NodeID, UnHealthy)
										nodeUnhealthyCh <- liveness.NodeID
										_, setErr := nl.SetDecommissioning(ctx, liveness.NodeID, false)
										if setErr != nil {
											log.Errorf(ctx, "error during liveness update %d -> %t, err: %v", liveness.NodeID, false, setErr)
										}
									}
								case storagepb.NodeLivenessStatus_UNAVAILABLE:
									// double check heartbeat and conn
									if liveness.Status != Healthy {
										if !(liveness.Status == PreJoin || liveness.Status == Joining) {
											continue
										}
									}
									// proactively checking if the heartbeat is alive, there is an unhealthy reception of all nodes due to the leaseholder switch in the liveness range
									if checkNodeStatus(ctx, sfg, liveness.NodeID) {
										log.Infof(checkCtx, "node is not death, may be too slow, skipping processing node id: %d", liveness.NodeID)
										continue
									}
									log.Warning(checkCtx, "node unhealthy: %d", liveness.NodeID)
									// refresh hash info before restart
									if liveness.Status == PreJoin || liveness.Status == Joining {
										api.HRManagerWLock()
										hashRouterMgr, err := api.GetHashRouterManagerWithTxn(ctx, nil)
										if err != nil {
											log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
											continue
										}
										for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
											err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
												return hashRouterMgr.RefreshHashRouterForAllGroups(ctx, txn, "refresh all tables", storagepb.NodeLivenessStatus_LIVE, nil)
											})
											if err != nil {
												log.Error(ctx, errors.Wrap(err, "refresh hash router"))
												continue
											}
											log.Info(ctx, "refresh hash router success")
											break
										}
										api.HRManagerWUnLock()
									}
									nl.SetTSNodeLiveness(context.Background(), liveness.NodeID, UnHealthy)
									nodeUnhealthyCh <- liveness.NodeID
								case storagepb.NodeLivenessStatus_DEAD:

									if liveness.Status != UnHealthy {
										// when unavailable, the check time out, node is in the dead state
										if liveness.Status == Healthy {
											nl.SetTSNodeLiveness(context.Background(), liveness.NodeID, UnHealthy)
											nodeUnhealthyCh <- liveness.NodeID
										}
										continue
									}
									allowAdvancedDistribute := settings.AllowAdvanceDistributeSetting.Get(&nl.st.SV)
									if !allowAdvancedDistribute {
										continue
									}

									log.Infof(checkCtx, "node %d dead", liveness.NodeID)
									nl.SetTSNodeLiveness(context.Background(), liveness.NodeID, Dead)
									nodeDeadCh <- liveness.NodeID
								case storagepb.NodeLivenessStatus_LIVE:
									if !(liveness.Status == Dead || liveness.Status == UnHealthy) {
										continue
									}
									nl.SetTSNodeLiveness(checkCtx, liveness.NodeID, ReJoining)
									log.Infof(checkCtx, "node %d status is healthy", liveness.NodeID)
									if liveness.Status == UnHealthy {
										nodeUnhealthyToHealthy <- liveness.NodeID
									} else {
										nodeDeadToHealthy <- liveness.NodeID
									}
								}
							}
						}
						errCh <- nil
					}()

					select {
					case err := <-errCh:
						return err
					case <-checkCtx.Done():
						return checkCtx.Err()
					}
				}); err != nil {
					log.VEventf(ctx, 3, err.Error())
				}
				tsHACheckInterval = settings.HaLivenessCheckInterval.Get(&nl.st.SV)
				timer.Reset(tsHACheckInterval)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// needHACheck determine whether to perform the HA check
// do not perform HA operations during cluster joining/decommissioning
func needHACheck(ctx context.Context, nl *NodeLiveness, sfg StoreConfig) bool {
	livenesses := nl.GetLivenesses()
	threshold := TimeUntilStoreDead.Get(&nl.st.SV)
	now := nl.clock.Now().GoTime()
	// HA not check during joining/decommissioning
	needCheck := true
	hasJoining := false
	hasPreJoin := false
	for _, liveness := range livenesses {
		if liveness.Status == Joining {
			hasJoining = true
		}
		status := LivenessStatus(liveness, now, threshold)
		switch status {
		case storagepb.NodeLivenessStatus_DECOMMISSIONING:
			if liveness.Status == Decommissioning {
				if !liveness.IsLive(now) {
					// check the heartbeat to invalidate whether the node is healthy, because the
					// change of leaseholder of system.NodeLiveness may make the heartbeat inconsecutive.
					if checkNodeStatus(ctx, sfg, liveness.NodeID) {
						log.Infof(ctx, "node %d is suspended, skip", liveness.NodeID)
						needCheck = false
						break
					}
					log.Infof(ctx, "node %d unhealthy", liveness.NodeID)
				} else {
					needCheck = false
					break
				}
			}
		case storagepb.NodeLivenessStatus_LIVE:
			if liveness.Status == PreJoin {
				hasPreJoin = true
			}
			if liveness.Status == Joining {
				needCheck = false
			}
		case storagepb.NodeLivenessStatus_UNAVAILABLE:
			if liveness.Status == PreJoin {
				if checkNodeStatus(ctx, sfg, liveness.NodeID) {
					log.Infof(ctx, "node %d is suspended, skip", liveness.NodeID)
					hasPreJoin = true
					break
				}
			}
			if liveness.Status == Joining {
				if checkNodeStatus(ctx, sfg, liveness.NodeID) {
					log.Infof(ctx, "node %d is suspended, skip", liveness.NodeID)
					needCheck = false
					break
				}
			}
		}

		if !needCheck {
			break
		}
	}
	// has live joining/decommissioning
	if !needCheck {
		return false
	}
	// only has live pre join
	if !hasJoining && hasPreJoin {
		return false
	}
	return true
}

// StartHeartbeat starts a periodic heartbeat to refresh this node's
// last heartbeat in the node liveness table. The optionally provided
// HeartbeatCallback will be invoked whenever this node updates its own liveness.
func (nl *NodeLiveness) StartHeartbeat(
	ctx context.Context, stopper *stop.Stopper, alive HeartbeatCallback,
) {
	log.VEventf(ctx, 1, "starting liveness heartbeat")
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()

	nl.mu.RLock()
	nl.mu.heartbeatCallback = alive
	nl.mu.RUnlock()

	stopper.RunWorker(ctx, func(context.Context) {
		ambient := nl.ambientCtx
		ambient.AddLogTag("liveness-hb", nil)
		ctx, cancel := stopper.WithCancelOnStop(context.Background())
		defer cancel()
		ctx, sp := ambient.AnnotateCtxWithSpan(ctx, "liveness heartbeat loop")
		defer sp.Finish()
		incrementEpoch := true
		ticker := time.NewTicker(nl.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-nl.heartbeatToken:
			case <-stopper.ShouldStop():
				return
			}
			// Give the context a timeout approximately as long as the time we
			// have left before our liveness entry expires.
			if err := contextutil.RunWithTimeout(ctx, "node liveness heartbeat", nl.livenessThreshold-nl.heartbeatInterval,
				func(ctx context.Context) error {
					// Retry heartbeat in the event the conditional put fails.
					for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
						liveness, err := nl.Self()
						if err != nil && err != ErrNoLivenessRecord {
							log.Errorf(ctx, "unexpected error getting liveness: %+v", err)
						}
						if err := nl.heartbeatInternal(ctx, liveness, incrementEpoch); err != nil {
							if err == ErrEpochIncremented {
								log.Infof(ctx, "%s; retrying", err)
								continue
							}
							return err
						}
						incrementEpoch = false // don't increment epoch after first heartbeat
						break
					}
					return nil
				}); err != nil {
				log.Warningf(ctx, "failed node liveness heartbeat: %+v", err)
			}

			nl.heartbeatToken <- struct{}{}
			select {
			case <-ticker.C:
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// PauseHeartbeat stops or restarts the periodic heartbeat depending on the
// pause parameter. When pause is true, waits until it acquires the heartbeatToken
// (unless heartbeat was already paused); this ensures that no heartbeats happen
// after this is called. This function is only safe for use in tests.
func (nl *NodeLiveness) PauseHeartbeat(pause bool) {
	if pause {
		if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 0, 1); swapped {
			<-nl.heartbeatToken
		}
	} else {
		if swapped := atomic.CompareAndSwapUint32(&nl.heartbeatPaused, 1, 0); swapped {
			nl.heartbeatToken <- struct{}{}
		}
	}
}

// DisableAllHeartbeatsForTest disables all node liveness heartbeats, including
// those triggered from outside the normal StartHeartbeat loop. Returns a
// closure to call to re-enable heartbeats. Only safe for use in tests.
func (nl *NodeLiveness) DisableAllHeartbeatsForTest() func() {
	nl.PauseHeartbeat(true)
	nl.selfSem <- struct{}{}
	nl.otherSem <- struct{}{}
	return func() {
		<-nl.selfSem
		<-nl.otherSem
	}
}

var errNodeAlreadyLive = errors.New("node already live")

// Heartbeat is called to update a node's expiration timestamp. This
// method does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map.
//
// The liveness argument is the expected previous value of this node's
// liveness.
//
// If this method returns nil, the node's liveness has been extended,
// relative to the previous value. It may or may not still be alive
// when this method returns.
//
// On failure, this method returns ErrEpochIncremented, although this
// may not necessarily mean that the epoch was actually incremented.
// TODO(bdarnell): Fix error semantics here.
//
// This method is rarely called directly; heartbeats are normally sent
// by the StartHeartbeat loop.
// TODO(bdarnell): Should we just remove this synchronous heartbeat completely?
func (nl *NodeLiveness) Heartbeat(ctx context.Context, liveness storagepb.Liveness) error {
	return nl.heartbeatInternal(ctx, liveness, false /* increment epoch */)
}

func (nl *NodeLiveness) heartbeatInternal(
	ctx context.Context, liveness storagepb.Liveness, incrementEpoch bool,
) error {
	ctx, sp := tracing.EnsureChildSpan(ctx, nl.ambientCtx.Tracer, "liveness heartbeat")
	defer sp.Finish()
	defer func(start time.Time) {
		dur := timeutil.Now().Sub(start)
		nl.metrics.HeartbeatLatency.RecordValue(dur.Nanoseconds())
		if dur > time.Second {
			log.Warningf(ctx, "slow heartbeat took %0.1fs", dur.Seconds())
		}
	}(timeutil.Now())

	// Allow only one heartbeat at a time.
	nodeID := nl.gossip.NodeID.Get()
	sem := nl.sem(nodeID)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	update := livenessUpdate{
		Liveness: storagepb.Liveness{
			NodeID: nodeID,
			Epoch:  1,
			Status: Healthy,
		},
	}
	if liveness != (storagepb.Liveness{}) {
		update.Liveness = liveness
		if incrementEpoch {
			update.Epoch++
			// Clear draining field.
			update.Draining = false
		}
	}
	// We need to add the maximum clock offset to the expiration because it's
	// used when determining liveness for a node.
	{
		update.Expiration = hlc.LegacyTimestamp(
			nl.clock.Now().Add((nl.livenessThreshold).Nanoseconds(), 0))
		// This guards against the system clock moving backwards. As long
		// as the kwbase process is running, checks inside hlc.Clock
		// will ensure that the clock never moves backwards, but these
		// checks don't work across process restarts.
		if update.Expiration.Less(liveness.Expiration) {
			return errors.Errorf("proposed liveness update expires earlier than previous record")
		}
	}
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		// Update liveness to actual value on mismatch.
		nl.maybeUpdate(actual)
		// If the actual liveness is different than expected, but is
		// considered live, treat the heartbeat as a success. This can
		// happen when the periodic heartbeater races with a concurrent
		// lease acquisition.
		//
		// TODO(bdarnell): If things are very slow, the new liveness may
		// have already expired and we'd incorrectly return
		// ErrEpochIncremented. Is this check even necessary? The common
		// path through this method doesn't check whether the liveness
		// expired while in flight, so maybe we don't have to care about
		// that and only need to distinguish between same and different
		// epochs in our return value.
		if actual.IsLive(nl.clock.Now().GoTime()) && !incrementEpoch {
			return errNodeAlreadyLive
		}
		// Otherwise, return error.
		return ErrEpochIncremented
	}); err != nil {
		if err == errNodeAlreadyLive {
			nl.metrics.HeartbeatSuccesses.Inc(1)
			return nil
		}
		nl.metrics.HeartbeatFailures.Inc(1)
		return err
	}

	log.VEventf(ctx, 1, "heartbeat %+v", update.Expiration)
	nl.maybeUpdate(update.Liveness)
	nl.metrics.HeartbeatSuccesses.Inc(1)
	return nil
}

// Self returns the liveness record for this node. ErrNoLivenessRecord
// is returned in the event that the node has neither heartbeat its
// liveness record successfully, nor received a gossip message containing
// a former liveness update on restart.
func (nl *NodeLiveness) Self() (storagepb.Liveness, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nl.gossip.NodeID.Get())
}

// IsLiveMapEntry encapsulates data about current liveness for a
// node.
type IsLiveMapEntry struct {
	IsLive bool
	Epoch  int64
}

// IsLiveMap is a type alias for a map from NodeID to IsLiveMapEntry.
type IsLiveMap map[roachpb.NodeID]IsLiveMapEntry

// GetIsLiveMap returns a map of nodeID to boolean liveness status of
// each node. This excludes nodes that were removed completely (dead +
// decommissioning).
func (nl *NodeLiveness) GetIsLiveMap() IsLiveMap {
	lMap := IsLiveMap{}
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	now := nl.clock.Now().GoTime()
	for nID, l := range nl.mu.nodes {
		isLive := l.IsLive(now)
		if !isLive && l.Decommissioning {
			// This is a node that was completely removed. Skip over it.
			continue
		}
		lMap[nID] = IsLiveMapEntry{
			IsLive: isLive,
			Epoch:  l.Epoch,
		}
	}
	return lMap
}

// GetLivenesses returns a slice containing the liveness status of
// every node on the cluster known to gossip. Callers should consider
// calling (statusServer).NodesWithLiveness() instead where possible.
func (nl *NodeLiveness) GetLivenesses() []storagepb.Liveness {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	livenesses := make([]storagepb.Liveness, 0, len(nl.mu.nodes))
	for _, l := range nl.mu.nodes {
		livenesses = append(livenesses, l)
	}
	return livenesses
}

// GetLiveness returns the liveness record for the specified nodeID.
// ErrNoLivenessRecord is returned in the event that nothing is yet
// known about nodeID via liveness gossip.
func (nl *NodeLiveness) GetLiveness(nodeID roachpb.NodeID) (storagepb.Liveness, error) {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	return nl.getLivenessLocked(nodeID)
}

func (nl *NodeLiveness) getLivenessLocked(nodeID roachpb.NodeID) (storagepb.Liveness, error) {
	if l, ok := nl.mu.nodes[nodeID]; ok {
		return l, nil
	}
	return storagepb.Liveness{}, ErrNoLivenessRecord
}

// IncrementEpoch is called to attempt to revoke another node's
// current epoch, causing an expiration of all its leases. This method
// does a conditional put on the node liveness record, and if
// successful, stores the updated liveness record in the nodes map. If
// this method is called on a node ID which is considered live
// according to the most recent information gathered through gossip,
// an error is returned.
//
// The liveness argument is used as the expected value on the
// conditional put. If this method returns nil, there was a match and
// the epoch has been incremented. This means that the expiration time
// in the supplied liveness accurately reflects the time at which the
// epoch ended.
//
// If this method returns ErrEpochAlreadyIncremented, the epoch has
// already been incremented past the one in the liveness argument, but
// the conditional put did not find a match. This means that another
// node performed a successful IncrementEpoch, but we can't tell at
// what time the epoch actually ended. (Usually when multiple
// IncrementEpoch calls race, they're using the same expected value.
// But when there is a severe backlog, it's possible for one increment
// to get stuck in a queue long enough for the dead node to make
// another successful heartbeat, and a second increment to come in
// after that)
func (nl *NodeLiveness) IncrementEpoch(ctx context.Context, liveness storagepb.Liveness) error {
	// Allow only one increment at a time.
	sem := nl.sem(liveness.NodeID)
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		<-sem
	}()

	if liveness.IsLive(nl.clock.Now().GoTime()) {
		return errors.Errorf("cannot increment epoch on live node: %+v", liveness)
	}
	update := livenessUpdate{Liveness: liveness}
	update.Epoch++
	if err := nl.updateLiveness(ctx, update, liveness, func(actual storagepb.Liveness) error {
		defer nl.maybeUpdate(actual)
		if actual.Epoch > liveness.Epoch {
			return ErrEpochAlreadyIncremented
		} else if actual.Epoch < liveness.Epoch {
			return errors.Errorf("unexpected liveness epoch %d; expected >= %d", actual.Epoch, liveness.Epoch)
		}
		return errors.Errorf("mismatch incrementing epoch for %+v; actual is %+v", liveness, actual)
	}); err != nil {
		return err
	}

	log.Infof(ctx, "incremented n%d liveness epoch to %d", update.NodeID, update.Epoch)
	nl.maybeUpdate(update.Liveness)
	nl.metrics.EpochIncrements.Inc(1)
	return nil
}

// Metrics returns a struct which contains metrics related to node
// liveness activity.
func (nl *NodeLiveness) Metrics() LivenessMetrics {
	return nl.metrics
}

// RegisterCallback registers a callback to be invoked any time a
// node's IsLive() state changes to true.
func (nl *NodeLiveness) RegisterCallback(cb IsLiveCallback) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.mu.callbacks = append(nl.mu.callbacks, cb)
}

// updateLiveness does a conditional put on the node liveness record for the
// node specified by nodeID. In the event that the conditional put fails, and
// the handleCondFailed callback is not nil, it's invoked with the actual node
// liveness record and nil is returned for an error. If handleCondFailed is nil,
// any conditional put failure is returned as an error to the caller. The
// conditional put is done as a 1PC transaction with a ModifiedSpanTrigger which
// indicates the node liveness record that the range leader should gossip on
// commit.
//
// updateLiveness terminates certain errors that are expected to occur
// sporadically, such as TransactionStatusError (due to the 1PC requirement of
// the liveness txn, and ambiguous results).
func (nl *NodeLiveness) updateLiveness(
	ctx context.Context,
	update livenessUpdate,
	oldLiveness storagepb.Liveness,
	handleCondFailed func(actual storagepb.Liveness) error,
) error {
	for {
		// Before each attempt, ensure that the context has not expired.
		if err := ctx.Err(); err != nil {
			return err
		}

		for _, eng := range nl.engines {
			// We synchronously write to all disks before updating liveness because we
			// don't want any excessively slow disks to prevent leases from being
			// shifted to other nodes. A slow/stalled disk would block here and cause
			// the node to lose its leases.
			if err := storage.WriteSyncNoop(ctx, eng); err != nil {
				return errors.Wrapf(err, "couldn't update node liveness because disk write failed")
			}
		}
		if err := nl.updateLivenessAttempt(ctx, update, oldLiveness, handleCondFailed); err != nil {
			// Intentionally don't errors.Cause() the error, or we'd hop past errRetryLiveness.
			if _, ok := err.(*errRetryLiveness); ok {
				log.Infof(ctx, "retrying liveness update after %s", err)
				continue
			}
			return err
		}
		return nil
	}
}

func (nl *NodeLiveness) updateLivenessAttempt(
	ctx context.Context,
	update livenessUpdate,
	oldLiveness storagepb.Liveness,
	handleCondFailed func(actual storagepb.Liveness) error,
) error {
	// First check the existing liveness map to avoid known conditional
	// put failures.
	if !update.ignoreCache {
		l, err := nl.GetLiveness(update.NodeID)
		if err != nil && err != ErrNoLivenessRecord {
			return err
		}
		if err == nil && l != oldLiveness {
			return handleCondFailed(l)
		}
	}

	if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		key := keys.NodeLivenessKey(update.NodeID)
		val := update.Liveness
		if oldLiveness == (storagepb.Liveness{}) {
			b.CPut(key, &val, nil)
		} else {
			expVal := oldLiveness
			b.CPutDeprecated(key, &val, &expVal)
		}
		// Use a trigger on EndTxn to indicate that node liveness should be
		// re-gossiped. Further, require that this transaction complete as a one
		// phase commit to eliminate the possibility of leaving write intents.
		b.AddRawRequest(&roachpb.EndTxnRequest{
			Commit:     true,
			Require1PC: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ModifiedSpanTrigger: &roachpb.ModifiedSpanTrigger{
					NodeLivenessSpan: &roachpb.Span{
						Key:    key,
						EndKey: key.Next(),
					},
				},
			},
		})
		return txn.Run(ctx, b)
	}); err != nil {
		switch tErr := errors.Cause(err).(type) {
		case *roachpb.ConditionFailedError:
			if handleCondFailed != nil {
				if tErr.ActualValue == nil {
					return handleCondFailed(storagepb.Liveness{})
				}
				var actualLiveness storagepb.Liveness
				if err := tErr.ActualValue.GetProto(&actualLiveness); err != nil {
					return errors.Wrapf(err, "couldn't update node liveness from CPut actual value")
				}
				return handleCondFailed(actualLiveness)
			}
		case *roachpb.TransactionStatusError:
			return &errRetryLiveness{err}
		case *roachpb.AmbiguousResultError:
			return &errRetryLiveness{err}
		}
		return err
	}

	nl.mu.Lock()
	cb := nl.mu.heartbeatCallback
	nl.mu.Unlock()
	if cb != nil {
		cb(ctx)
	}
	return nil
}

// maybeUpdate replaces the liveness (if it appears newer) and invokes the
// registered callbacks if the node became live in the process.
func (nl *NodeLiveness) maybeUpdate(new storagepb.Liveness) {
	nl.mu.Lock()
	// Note that this works fine even if `old` is empty.
	old := nl.mu.nodes[new.NodeID]
	should := shouldReplaceLiveness(old, new)
	var callbacks []IsLiveCallback
	if should {
		nl.mu.nodes[new.NodeID] = new
		callbacks = append(callbacks, nl.mu.callbacks...)
	}
	nl.mu.Unlock()

	if !should {
		return
	}

	now := nl.clock.Now().GoTime()
	if !old.IsLive(now) && new.IsLive(now) {
		for _, fn := range callbacks {
			fn(new.NodeID)
		}
	}
}

func shouldReplaceLiveness(old, new storagepb.Liveness) bool {
	if (old == storagepb.Liveness{}) {
		return true
	}

	// Compare first Epoch, and no change there, Expiration.
	if old.Epoch != new.Epoch {
		return old.Epoch < new.Epoch
	}
	if old.Expiration != new.Expiration {
		return old.Expiration.Less(new.Expiration)
	}

	// If Epoch and Expiration are unchanged, assume that the update is newer
	// when its draining or decommissioning field changed.
	//
	// This has false positives (in which case we're clobbering the liveness). A
	// better way to handle liveness updates in general is to add a sequence
	// number.
	//
	// See #18219.
	return old.Draining != new.Draining || old.Decommissioning != new.Decommissioning ||
		old.Status != new.Status
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (nl *NodeLiveness) livenessGossipUpdate(key string, content roachpb.Value) {
	var liveness storagepb.Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Error(context.TODO(), err)
		return
	}

	nl.maybeUpdate(liveness)
}

// numLiveNodes is used to populate a metric that tracks the number of live
// nodes in the cluster. Returns 0 if this node is not itself live, to avoid
// reporting potentially inaccurate data.
// We export this metric from every live node rather than a single particular
// live node because liveness information is gossiped and thus may be stale.
// That staleness could result in no nodes reporting the metric or multiple
// nodes reporting the metric, so it's simplest to just have all live nodes
// report it.
func (nl *NodeLiveness) numLiveNodes() int64 {
	ctx := nl.ambientCtx.AnnotateCtx(context.Background())

	selfID := nl.gossip.NodeID.Get()
	if selfID == 0 {
		return 0
	}

	nl.mu.RLock()
	defer nl.mu.RUnlock()

	self, err := nl.getLivenessLocked(selfID)
	if err == ErrNoLivenessRecord {
		return 0
	}
	if err != nil {
		log.Warningf(ctx, "looking up own liveness: %+v", err)
		return 0
	}
	now := nl.clock.Now().GoTime()
	// If this node isn't live, we don't want to report its view of node liveness
	// because it's more likely to be inaccurate than the view of a live node.
	if !self.IsLive(now) {
		return 0
	}
	var liveNodes int64
	for _, l := range nl.mu.nodes {
		if l.IsLive(now) {
			liveNodes++
		}
	}
	return liveNodes
}

// AsLiveClock returns a closedts.LiveClockFn that takes a current timestamp off
// the clock and returns it only if node liveness indicates that the node is live
// at that timestamp and the returned epoch.
func (nl *NodeLiveness) AsLiveClock() closedts.LiveClockFn {
	return func(nodeID roachpb.NodeID) (hlc.Timestamp, ctpb.Epoch, error) {
		now := nl.clock.Now()
		liveness, err := nl.GetLiveness(nodeID)
		if err != nil {
			return hlc.Timestamp{}, 0, err
		}
		if !liveness.IsLive(now.GoTime()) {
			return hlc.Timestamp{}, 0, errLiveClockNotLive
		}
		return now, ctpb.Epoch(liveness.Epoch), nil
	}
}

// GetNodeCount returns a count of the number of nodes in the cluster,
// including dead nodes, but excluding decommissioning or decommissioned nodes.
func (nl *NodeLiveness) GetNodeCount() int {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	var count int
	for _, l := range nl.mu.nodes {
		if !l.Decommissioning {
			count++
		}
	}
	return count
}

// nodeDeadCh and nodeUnhealthyCh are used to send ha process trigger.
var nodeDeadCh = make(chan roachpb.NodeID)
var nodeUnhealthyCh = make(chan roachpb.NodeID)
var nodeUnhealthyToHealthy = make(chan roachpb.NodeID)
var nodeDeadToHealthy = make(chan roachpb.NodeID)

// IsGroupNoRebalanced check if cluster group is rebalanced
func (nl *NodeLiveness) IsGroupNoRebalanced(
	ctx context.Context,
	leaseNode roachpb.NodeID,
	isControlNode roachpb.NodeID,
	hashRouterManagerHook api.HashRouterManager,
	currentNodeStatus storagepb.NodeLivenessStatus,
) (bool, error) {
	replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&nl.st.SV))
	groups := hashRouterManagerHook.GetGroupsOnNode(ctx, isControlNode)
	if len(groups) <= 0 {
		log.Infof(ctx, "nothing can rebalanced EntityRangeGroup, %+v", hashRouterManagerHook.GetAllGroups(ctx))
		return false, nil
	}
	liveness, err := nl.getLivenessLocked(isControlNode)
	if err != nil {
		log.Errorf(ctx, "get liveness error, err is :%v", err)
		return false, err
	}
	for _, group := range groups {
		log.Infof(ctx, "the isControlNode : %v The Group :%v Status : %v, PreviousLeaseHolder.NodeID : %v, group.NodeStatus : %v, group.LeaseHolder.NodeID: %v, nodeStatus: %v,liveness.Status : %v\n",
			isControlNode, group.GroupID, group.Status, group.PreviousLeaseHolder.NodeID, group.NodeStatus, group.LeaseHolder.NodeID, currentNodeStatus, liveness.Status)
		if group.Status != api.EntityRangeGroupStatus_Available && group.Status != api.EntityRangeGroupStatus_lacking {
			log.Infof(ctx, "is rebalanced, skip")
			continue
		}
		availableReplCnt := group.AvailableReplicaCnt()
		switch currentNodeStatus {
		case storagepb.NodeLivenessStatus_DEAD:
			if availableReplCnt == replicaNum {
				log.Infof(ctx, "replica number is ready,skip")
				continue
			}
			if availableReplCnt < replicaNum/2+1 {
				log.Warningf(ctx, "no enough available replicas")
				continue
			}
			log.Infof(ctx, "node Dead,group.PreviousLeaseHolder.NodeID:%v ,isControlNode:%v ,group.NodeStatus:%v", group.PreviousLeaseHolder.NodeID, isControlNode, group.NodeStatus)
			return true, nil
		// decommission may require special handling, and if it results in insufficient copies of a certain group, it should not be allowed to shrink
		case storagepb.NodeLivenessStatus_UNAVAILABLE:
			if group.LeaseHolder.NodeID == isControlNode {
				log.Infof(ctx, "node Unhealthy, leaseNode: %v, group Status: %v ", leaseNode, group.Status)
				return true, nil
			}
			log.Infof(ctx, "node Unhealthy, follower choice : %v", group.GroupID)
			return true, nil

		case storagepb.NodeLivenessStatus_LIVE:
			if group.HasReplicaOnNode(isControlNode) && liveness.Status != Healthy {
				log.Infof(ctx, "node Live,group.PreviousLeaseHolder.NodeID:%v ,isControlNode:%v ,group.NodeStatus:%v", group.PreviousLeaseHolder.NodeID, isControlNode, group.NodeStatus)
				return true, nil
			}
		}
	}
	log.Infof(ctx, "cannot do anything,leaseNode:%v ,isControlNode:%v ,currentNodeStatus:%v", leaseNode, isControlNode, currentNodeStatus)
	return false, nil
}

// UpdateRangeGroups updates the range groups for the target partitions
func (nl *NodeLiveness) UpdateRangeGroups(
	ctx context.Context,
	tableID sqlbase.ID,
	partitions []api.EntityRangePartitionMessage,
	mgr api.HashRouterManager,
) {
	updatedNodes := make(map[roachpb.NodeID]interface{}, 0)
	for _, part := range partitions {
		for _, partReplica := range part.DestInternalReplicas {
			destNodeID := partReplica.NodeID
			if _, ok := updatedNodes[destNodeID]; !ok {
				updatedNodes[destNodeID] = struct{}{}
				hashInfo := mgr.GetHashInfoByTableID(ctx, uint32(tableID))
				rangeGroups := hashInfo.GetGroupIDAndRoleOnNode(ctx, destNodeID)
				err := api.RefreshTSRangeGroup(ctx, uint32(tableID), destNodeID, rangeGroups, nil)
				if err != nil {
					// node unhealthy or dead, it can be ignore
					// TODO rejoin will by panic?
					log.Warningf(ctx, "failed to update ts range group: %v, err is :", rangeGroups, err)
				}
			}
		}
	}
	log.Infof(ctx, "done refresh storage rangeGroup info:%v", updatedNodes)
}

func haExecLogTags(nodeID roachpb.NodeID, action string) *logtags.Buffer {
	buf := &logtags.Buffer{}
	buf = buf.Add("nodeID", nodeID)
	buf = buf.Add("action", action)
	return buf
}

// StartHAProcess start ha go routine.
func (nl *NodeLiveness) StartHAProcess(ctx context.Context, stopper *stop.Stopper) {
	//　TODO with concurrency and transaction
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for {
			select {
			case nodeID := <-nodeUnhealthyCh:
				ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "unhealthy"))

				log.Infof(ctx, "node %v unhealthy", nodeID)
				tableIDs := nl.GetAllTsTableID(ctx)
				log.Infof(ctx, "get all table ids %v", tableIDs)
				api.HRManagerWLock()
				hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
				if err != nil {
					log.Errorf(ctx, "get hashrouter manager failed :%v", err)
					continue
				}
				ok, err := nl.IsGroupNoRebalanced(ctx, nl.gossip.NodeID.Get(), nodeID, hashRouterManager, storagepb.NodeLivenessStatus_UNAVAILABLE)
				if err != nil || !ok {
					api.HRManagerWUnLock()
					log.Infof(ctx, "skip node %v Unhealthy", nodeID)
					continue
				}
				err2 := nl.handleNodeUnHealthy(ctx, nodeID, hashRouterManager, tableIDs)
				api.HRManagerWUnLock()
				if err2 != nil {
					log.Warningf(ctx, "handle node unhealthy failed : %v", err)
					continue
				}
				log.Infof(ctx, "handle node %v unhealthy succeed", nodeID)
			case nodeID := <-nodeDeadCh:
				ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "dead"))

				log.Infof(ctx, "node %v dead", nodeID)
				tableIDs := nl.GetAllTsTableID(ctx)
				log.Infof(ctx, "get all table ids %v", tableIDs)
				replicaNum := int(settings.DefaultEntityRangeReplicaNum.Get(&nl.st.SV))
				if len(nl.GetIsLiveMap()) < replicaNum {
					log.Warningf(ctx, "the number of healthy nodes %d is less than the required replica count %d.", len(nl.GetIsLiveMap()), replicaNum)
					break
				}
				hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
				if err != nil {
					log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
					continue
				}
				ok, err := nl.IsGroupNoRebalanced(ctx, nl.gossip.NodeID.Get(), nodeID, hashRouterManager, storagepb.NodeLivenessStatus_DEAD)
				if err != nil || !ok {
					log.Infof(ctx, "skip node %v Dead", nodeID)
					continue
				}
				err2 := nl.handleNodeDead(ctx, nodeID, hashRouterManager, tableIDs)
				if err2 != nil {
					log.Warningf(ctx, "handle node %d dead failed : %v", nodeID, err2)
					continue
				}
				log.Infof(ctx, "node dead : %v, ha process success", nodeID)
			case nodeID := <-nodeUnhealthyToHealthy:
				ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "unhealthyToRejoin"))

				log.Infof(ctx, "node unhealthy to healthy %v", nodeID)
				tableIDs := nl.GetAllTsTableID(ctx)
				log.Infof(ctx, "get all table ids %v", tableIDs)
				hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
				if err != nil {
					log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
					continue
				}
				ok, err := nl.IsGroupNoRebalanced(ctx, nl.gossip.NodeID.Get(), nodeID, hashRouterManager, storagepb.NodeLivenessStatus_LIVE)
				if err != nil || !ok {
					// reset to healthy if no need to rebalanced
					nl.SetTSNodeLiveness(ctx, nodeID, Healthy)
					if err != nil {
						log.Warning(ctx, "check groups not reblanced: ", err)
					}
					continue
				}
				err2 := nl.handleNodeUnHealthyRejoin(ctx, nodeID, hashRouterManager, tableIDs)
				if err2 != nil {
					log.Warning(ctx, "handle node %d unhealthy to rejoin failed : %v", nodeID, err)
					continue
				}
				nl.SetTSNodeLiveness(ctx, nodeID, Healthy)
				log.Infof(ctx, "handle node %v unhealthy to heanlthy succeed.", nodeID)
			case nodeID := <-nodeDeadToHealthy:
				ctx = logtags.AddTags(ctx, haExecLogTags(nodeID, "deadToRejoin"))

				log.Infof(ctx, "node %v dead to healthy", nodeID)
				tableIDs := nl.GetAllTsTableID(ctx)
				log.Infof(ctx, "get all table ids %v", tableIDs)
				api.HRManagerWLock()
				hashRouterManager, err := api.GetHashRouterManagerWithTxn(ctx, nil)
				if err != nil {
					log.Errorf(ctx, "getting hashrouter manager failed :%v", err)
					continue
				}
				hashInfo, hashErr := hashRouterManager.GetAllHashRouterInfo(ctx, nil)
				if hashErr != nil {
					log.Errorf(ctx, "get hash info failed :%v", hashErr)
				}
				for tableID, router := range hashInfo {
					rangeGroups := router.GetGroupIDAndRoleOnNode(ctx, nodeID)
					removeErr := api.RemoveUnusedTSRangeGroups(ctx, tableID, nodeID, rangeGroups)
					if removeErr != nil {
						log.Errorf(ctx, "remove rangeGroup failed : TableID %d, NodeID %d, err: %v", tableID, nodeID, removeErr)
					}
				}
				ok, err := nl.IsGroupNoRebalanced(ctx, nl.gossip.NodeID.Get(), nodeID, hashRouterManager, storagepb.NodeLivenessStatus_LIVE)
				if err != nil || !ok {
					api.HRManagerWUnLock()
					// reset to healthy if no need to rebalance
					nl.SetTSNodeLiveness(ctx, nodeID, Healthy)
					if err != nil {
						log.Warning(ctx, "check groups not reblanced: ", err)
					}
					continue
				}
				err2 := nl.handleNodeDeadRejoin(ctx, nodeID, hashRouterManager, tableIDs)
				if err2 != nil {
					api.HRManagerWUnLock()
					log.Warning(ctx, "handle node %d dead rejoin failed : %v", nodeID, err)
					continue
				}
				api.HRManagerWUnLock()
				log.Infof(ctx, "handle dead %v to healthy succeed", nodeID)
				nl.SetTSNodeLiveness(ctx, nodeID, Healthy)
			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// handleNodeUnHealthy dealing with unhealthy node, heartbeat without 4.5s
// With too many node unhealthy, maybe not success. It can be ignore err and continue
func (nl *NodeLiveness) handleNodeUnHealthy(
	ctx context.Context,
	nodeID roachpb.NodeID,
	hashRouterManager api.HashRouterManager,
	tableIds []uint32,
) error {

	for _, tableID := range tableIds {
		retryOpts := base.DefaultRetryOptions()
		retryOpts.MaxRetries = 5
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				err := hashRouterManager.CheckFromDisk(ctx, txn, tableID)
				if err != nil {
					log.Warning(ctx, "check from disk failed: %v", err)
					return err
				}
				groupChanges, unhealthyErr := hashRouterManager.IsNodeUnhealthy(ctx, nodeID, tableID)
				if unhealthyErr != nil {
					log.Warning(ctx, "get node table %v unhealthy ha change message failed: %v", tableID, unhealthyErr)
					return unhealthyErr
				}
				log.Infof(ctx, "get node unhealthy ha change messages of table %v: %+v", tableID, groupChanges)
				// transfer it by rangeGroup
				for _, groupChange := range groupChanges {
					err = nl.transferGroup(ctx, txn, nodeID, hashRouterManager, groupChange, tableID, false)
					if err != nil {
						log.Errorf(ctx, "transfer table %d group %d failed: %v. change message is %+v",
							tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
						return err
					}
					log.Infof(ctx, "transfer table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)

				}
				log.Infof(ctx, "refresh Table %d success", tableID)
				return nil
			}); err != nil {
				log.Warningf(ctx, "handle node unhealthy failed, table %v, err: %+v", tableID, err)
				continue
			} else {
				log.Infof(ctx, "handle node unhealthy succeed, table %v", tableID)
				break
			}
		}
	}
	return nil
}

// transferGroup transfer group with txn. It has tow exceptional case:　a.Put distribute message error b.Raft transfer err.
// For a, txn rollback; For b, handle this case and retry in distsender
func (nl *NodeLiveness) transferGroup(
	ctx context.Context,
	txn *kv.Txn,
	nodeID roachpb.NodeID,
	hashRouterManager api.HashRouterManager,
	groupChange api.EntityRangeGroupChange,
	tableID uint32,
	isJoin bool,
) error {
	r := groupChange.Routing
	err := hashRouterManager.PutSingleHashInfoWithLock(ctx, tableID, txn, r)
	if err != nil {
		log.Errorf(ctx, "Put Single HashInfo Failed : %v", err)
		return errors.Errorf("Put Single HashInfo Failed : %v", err)
	}
	// todo(fxy) : If not Unhealthy to Healthy, don't need to judge if
	// leaseholder changes, do TSRequestLease for all messages.
	// else, do TSRequestLease for nodes whose leaseholder changes only.
	// But we should not do this operation here. Maybe we should consider
	// do same jobs whether Unhealthy to Healthy or Healthy to Unhealthy.

	for _, r := range groupChange.Messages {
		if !isJoin && r.DestLeaseHolder.NodeID == r.SrcLeaseHolder.NodeID {
			continue
		}
		transferErr := api.TSRequestLease(ctx, tableID, r.Partition.StartPoint, r.DestLeaseHolder.NodeID)
		// TODO(fyx): The high availability processing of this Range failed, ignoring this processing
		if transferErr != nil {
			log.Warning(ctx, "tableID: %d, GroupID %d,ha process transfer failed:%v, skip it",
				tableID, groupChange.Routing.EntityRangeGroupId, transferErr)
			continue
		}
	}

	log.Infof(ctx, "refresh Table %d GroupID %d start", tableID, groupChange.Routing.EntityRangeGroupId)
	message := fmt.Sprintf("nodeId: %d,node healthy to unhealthy", nodeID)

	refreshErr := hashRouterManager.RefreshHashRouterWithSingleGroup(ctx, tableID, txn, message, groupChange)
	if refreshErr != nil {
		log.Warning(ctx, "refresh table %v dist message failed: %+v. err ha msg: %+v ", tableID, err, groupChange)
		return errors.Errorf("tableID: %d ,ha refresh failed:%v, skip it", tableID, refreshErr)
	}
	log.Infof(ctx, "refresh Table %d, GroupID %d success", tableID, groupChange.Routing.EntityRangeGroupId)

	nl.UpdateRangeGroups(ctx, sqlbase.ID(tableID), groupChange.Messages, hashRouterManager)
	log.Infof(ctx, "refresh Table %d GroupID %d success", tableID, groupChange.Routing.EntityRangeGroupId)
	return nil
}

// handleNodeDead dealing with dead node, heartbeat without 30min
func (nl *NodeLiveness) handleNodeDead(
	ctx context.Context,
	nodeID roachpb.NodeID,
	hashRouterManager api.HashRouterManager,
	tableIds []uint32,
) error {
	allowAdvancedDistribute := settings.AllowAdvanceDistributeSetting.Get(&nl.st.SV)
	if !allowAdvancedDistribute {
		return errors.Errorf("Can not set node status to dead when server.allow_advanced_distributed_operations is false. ")
	}
	for _, tableID := range tableIds {
		// if the table is alter, should wait for table available. The reason is that there are limitations in storing snapshots, it will be improve soon

		err := sqlbase.CheckTableStatusOk(ctx, nil, nl.db, tableID, true)
		if err != nil {
			log.Warning(ctx, "check table %v status failed: %v", tableID, err)
			continue
		}
		groupChanges, deadErr := hashRouterManager.IsNodeDead(ctx, nodeID, tableID)
		if deadErr != nil {
			log.Warning(ctx, "get table %v dead ha change message failed: %v", tableID, deadErr)
			continue
		}
		log.Infof(ctx, "get node dead ha change messages of table %v: %+v", tableID, groupChanges)

		for _, groupChange := range groupChanges {

			r := groupChange.Routing
			api.HRManagerWLock()
			err := hashRouterManager.PutSingleHashInfoWithLock(ctx, tableID, nil, r)
			api.HRManagerWUnLock()
			if err != nil {
				log.Errorf(ctx, "put table %v group %v single hashInfo failed : %v. change message is %+v",
					tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
			}
			for _, part := range groupChange.Messages {

				// todo(fxy): check if leaseholder changes. if changes, panic.
				//            The transfer of leaseholder occurs in the unhealthy
				//            stage. Normally, the transfer of leaseholder does not
				//            occur in the death stage.
				relocateErr := api.RelocatePartitionReplicas(ctx, tableID, part, false)
				if relocateErr != nil {
					log.Warning(ctx, "relocate table %v replicas failed: %v. partition messages :%+v", tableID,
						relocateErr, part)
					continue
				}
			}
			log.Infof(ctx, "relocate table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)

			log.Infof(ctx, "start refresh table %v dist message", tableID)

			message := fmt.Sprintf("nodeId: %d,node unhealthy to dead", nodeID)

			// due to the migration and completion of the replica, the node status is normal for this group
			if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				api.HRManagerWLock()
				refreshErr := hashRouterManager.RefreshHashRouterWithSingleGroup(ctx, tableID, txn, message, groupChange)
				api.HRManagerWUnLock()
				return refreshErr
			}); err != nil {
				log.Warning(ctx, "refresh table %v dist message failed: %+v. err ha msg: %+v ", tableID, err, groupChange)
				continue
			}

			nl.UpdateRangeGroups(ctx, sqlbase.ID(tableID), groupChange.Messages, hashRouterManager)
			log.Infof(ctx, "end update table %d group %v rangeGroups: %+v",
				tableID, groupChange.Routing.EntityRangeGroupId, groupChange)
		}
		log.Infof(ctx, "refresh Table %d success", tableID)
	}

	return nil
}

// GetAllTsTableID get all ts tables' id.
func (nl *NodeLiveness) GetAllTsTableID(ctx context.Context) []uint32 {
	var tableIDs []uint32

	for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
		tableIDs = nil
		err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			descriptors, err := getAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			for i := range descriptors {
				tableDesc, ok := descriptors[i].(*sqlbase.TableDescriptor)
				if ok && tableDesc.IsTSTable() && tableDesc.ID > keys.MinNonPredefinedUserDescID {
					tableIDs = append(tableIDs, uint32(tableDesc.ID))
				}
			}

			return nil
		})
		if err != nil {
			log.Errorf(ctx, "get all ts tables' id failed, error: %s", err.Error())
			continue
		}
		break
	}

	return tableIDs
}

// getAllDescriptors is same as sql.GetAllDescriptors.
func getAllDescriptors(ctx context.Context, txn *kv.Txn) ([]sqlbase.DescriptorProto, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	descs := make([]sqlbase.DescriptorProto, 0, len(kvs))
	for _, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			table := desc.Table(kv.Value.Timestamp)
			if err := table.MaybeFillInDescriptor(ctx, txn); err != nil {
				return nil, err
			}
			descs = append(descs, table)
		case *sqlbase.Descriptor_Database:
			descs = append(descs, desc.GetDatabase())
		case *sqlbase.Descriptor_Schema:
			descs = append(descs, desc.GetSchema())
		default:
			return nil, errors2.AssertionFailedf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return descs, nil
}

// handleNodeUnHealthyRejoin dealing with unhealthy to healthy node, heartbeat is reconnect
func (nl *NodeLiveness) handleNodeUnHealthyRejoin(
	ctx context.Context,
	nodeID roachpb.NodeID,
	hashRouterManager api.HashRouterManager,
	tableIds []uint32,
) error {

	for _, tableID := range tableIds {
		if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			err := hashRouterManager.CheckFromDisk(ctx, txn, tableID)
			if err != nil {
				log.Warning(ctx, "check from disk failed: %v", err)
				return err
			}
			groupChanges, recoverErr := hashRouterManager.NodeRecover(ctx, nodeID, tableID)
			if recoverErr != nil {
				log.Warning(ctx, "get node table %v  unhealthy to rejoin ha change messages failed:"+
					" %v", tableID, recoverErr)
				return err
			}
			log.Infof(ctx, "get node unhealthy to rejoin ha change messages of table %v: %+v", tableID, groupChanges)
			for _, groupChange := range groupChanges {
				err = nl.transferGroup(ctx, txn, nodeID, hashRouterManager, groupChange, tableID, true)
				if err != nil {
					log.Errorf(ctx, "transfer table %d group %d failed: %v. change message is %+v",
						tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
					return err
				}
				log.Infof(ctx, "transfer table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)
			}
			return nil
		}); err != nil {
			log.Warningf(ctx, "handle node unhealthyToRejoin failed, table %v, err: %+v", tableID, err)
		}
	}

	return nil
}

// handleNodeDeadRejoin dealing with dead to healthy node, heartbeat is reconnect
func (nl *NodeLiveness) handleNodeDeadRejoin(
	ctx context.Context,
	nodeID roachpb.NodeID,
	hashRouterManager api.HashRouterManager,
	tableIds []uint32,
) error {

	for _, tableID := range tableIds {
		err := sqlbase.CheckTableStatusOk(ctx, nil, nl.db, tableID, true)
		if err != nil {
			log.Warning(ctx, "check from disk failed: %v", err)
			continue
		}
		groupChanges, restartErr := hashRouterManager.ReStartNode(ctx, nodeID, tableID)
		if restartErr != nil {
			log.Warning(ctx, "get node deadToRejoin ha change message failed: %v", restartErr)
			continue
		}
		log.Infof(ctx, "get node deadToRejoin ha change messages of table %v: %+v", tableID, groupChanges)

		for _, groupChange := range groupChanges {
			r := groupChange.Routing
			err := hashRouterManager.PutSingleHashInfoWithLock(ctx, tableID, nil, r)
			if err != nil {
				log.Errorf(ctx, "put table %v group %v single hashInfo failed : %v. change message is %+v",
					tableID, groupChange.Routing.EntityRangeGroupId, err, groupChange)
			}
			for _, msg := range groupChange.Messages {
				relocateErr := api.RelocatePartitionReplicas(ctx, tableID, msg, false)
				if relocateErr != nil {
					log.Warning(ctx, "relocate table %v replicas failed: %v. partition messages :%+v", tableID,
						relocateErr, msg)
				}

				if msg.DestLeaseHolder.NodeID != msg.SrcLeaseHolder.NodeID {
					transferErr := api.TSRequestLease(ctx, tableID, msg.Partition.StartPoint, msg.DestLeaseHolder.NodeID)
					if transferErr != nil {
						log.Warning(ctx, "request table %d lease failed: %v, skip. err ha msg: %+v", transferErr, groupChange)
						continue
					}
				}
			}
			log.Infof(ctx, "relocate table %d group %d succeed.", tableID, groupChange.Routing.EntityRangeGroupId)
			log.Infof(ctx, "start refresh table %v dist message", tableID)

			message := fmt.Sprintf("nodeId: %d,tableId: %v, node dead to healthy", nodeID, tableID)

			if err := nl.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				refreshErr := hashRouterManager.RefreshHashRouterWithSingleGroup(ctx, tableID, txn, message, groupChange)
				return refreshErr
			}); err != nil {
				log.Warning(ctx, "refresh table %v dist message failed: %+v. err ha msg: %+v ", tableID, err, groupChange)
				continue
			}

			// update rangeGroup to ts engine
			nl.UpdateRangeGroups(ctx, sqlbase.ID(tableID), groupChange.Messages, hashRouterManager)
			log.Infof(ctx, "end update table %d group %v rangeGroups: %+v",
				tableID, groupChange.Routing.EntityRangeGroupId, groupChange)
		}
		log.Infof(ctx, "refresh Table %d success", tableID)
	}

	return nil
}
