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

// This file includes test-only helper methods added to types in
// package storage. These methods are only linked in to tests in this
// directory (but may be used from tests in both package storage and
// package storage_test).

package kvserver

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/quotapool"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
)

func (s *Store) Transport() *RaftTransport {
	return s.cfg.Transport
}

func (s *Store) FindTargetAndTransferLease(
	ctx context.Context, repl *Replica, desc *roachpb.RangeDescriptor, zone *zonepb.ZoneConfig,
) (bool, error) {
	return s.replicateQueue.findTargetAndTransferLease(
		ctx, repl, desc, zone, transferLeaseOptions{},
	)
}

// AddReplica adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplica(repl *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addReplicaInternalLocked(repl); err != nil {
		return err
	}
	s.metrics.ReplicaCount.Inc(1)
	return nil
}

// ComputeMVCCStats immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store).
func (s *Store) ComputeMVCCStats() (enginepb.MVCCStats, error) {
	var totalStats enginepb.MVCCStats
	var err error

	now := s.Clock().PhysicalNow()
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		var stats enginepb.MVCCStats
		stats, err = rditer.ComputeStatsForRange(r.Desc(), s.Engine(), now)
		if err != nil {
			return false
		}
		totalStats.Add(stats)
		return true
	})
	return totalStats, err
}

// ConsistencyQueueShouldQueue invokes the shouldQueue method on the
// store's consistency queue.
func (s *Store) ConsistencyQueueShouldQueue(
	ctx context.Context, now hlc.Timestamp, r *Replica, cfg *config.SystemConfig,
) (bool, float64) {
	return s.consistencyQueue.shouldQueue(ctx, now, r, cfg)
}

// LogReplicaChangeTest adds a fake replica change event to the log for the
// range which contains the given key.
func (s *Store) LogReplicaChangeTest(
	ctx context.Context,
	txn *kv.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason storagepb.RangeLogEventReason,
	details string,
) error {
	return s.logChange(ctx, txn, changeType, replica, desc, reason, details)
}

// ReplicateQueuePurgatoryLength returns the number of replicas in replicate
// queue purgatory.
func (s *Store) ReplicateQueuePurgatoryLength() int {
	return s.replicateQueue.PurgatoryLength()
}

// SplitQueuePurgatoryLength returns the number of replicas in split
// queue purgatory.
func (s *Store) SplitQueuePurgatoryLength() int {
	return s.splitQueue.PurgatoryLength()
}

// SetRaftLogQueueActive enables or disables the raft log queue.
func (s *Store) SetRaftLogQueueActive(active bool) {
	s.setRaftLogQueueActive(active)
}

// SetReplicaGCQueueActive enables or disables the replica GC queue.
func (s *Store) SetReplicaGCQueueActive(active bool) {
	s.setReplicaGCQueueActive(active)
}

// SetSplitQueueActive enables or disables the split queue.
func (s *Store) SetSplitQueueActive(active bool) {
	s.setSplitQueueActive(active)
}

// SetMergeQueueActive enables or disables the split queue.
func (s *Store) SetMergeQueueActive(active bool) {
	s.setMergeQueueActive(active)
}

// SetRaftSnapshotQueueActive enables or disables the raft snapshot queue.
func (s *Store) SetRaftSnapshotQueueActive(active bool) {
	s.setRaftSnapshotQueueActive(active)
}

// SetReplicaScannerActive enables or disables the scanner. Note that while
// inactive, removals are still processed.
func (s *Store) SetReplicaScannerActive(active bool) {
	s.setScannerActive(active)
}

// EnqueueRaftUpdateCheck enqueues the replica for a Raft update check, forcing
// the replica's Raft group into existence.
func (s *Store) EnqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.enqueueRaftUpdateCheck(rangeID)
}

func manualQueue(s *Store, q queueImpl, repl *Replica) error {
	cfg := s.Gossip().GetSystemConfig()
	if cfg == nil {
		return fmt.Errorf("%s: system config not yet available", s)
	}
	ctx := repl.AnnotateCtx(context.TODO())
	return q.process(ctx, repl, cfg)
}

// ManualGC processes the specified replica using the store's GC queue.
func (s *Store) ManualGC(repl *Replica) error {
	return manualQueue(s, s.gcQueue, repl)
}

// ManualReplicaGC processes the specified replica using the store's replica
// GC queue.
func (s *Store) ManualReplicaGC(repl *Replica) error {
	return manualQueue(s, s.replicaGCQueue, repl)
}

// ManualRaftSnapshot will manually send a raft snapshot to the target replica.
func (s *Store) ManualRaftSnapshot(repl *Replica, target roachpb.ReplicaID) error {
	return s.raftSnapshotQueue.processRaftSnapshot(context.TODO(), repl, target)
}

func (s *Store) ReservationCount() int {
	return len(s.snapshotApplySem)
}

// RaftSchedulerPriorityID returns the Raft scheduler's prioritized range.
func (s *Store) RaftSchedulerPriorityID() roachpb.RangeID {
	return s.scheduler.PriorityID()
}

// ClearClosedTimestampStorage clears the closed timestamp storage of all
// knowledge about closed timestamps.
func (s *Store) ClearClosedTimestampStorage() {
	s.cfg.ClosedTimestamp.Storage.Clear()
}

// RequestClosedTimestamp instructs the closed timestamp client to request the
// relevant node to publish its MLAI for the provided range.
func (s *Store) RequestClosedTimestamp(nodeID roachpb.NodeID, rangeID roachpb.RangeID) {
	s.cfg.ClosedTimestamp.Clients.Request(nodeID, rangeID)
}

// AssertInvariants verifies that the store's bookkeping is self-consistent. It
// is only valid to call this method when there is no in-flight traffic to the
// store (e.g., after the store is shut down).
func (s *Store) AssertInvariants() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.mu.replicas.Range(func(_ int64, p unsafe.Pointer) bool {
		ctx := s.cfg.AmbientCtx.AnnotateCtx(context.Background())
		repl := (*Replica)(p)
		// We would normally need to hold repl.raftMu. Otherwise we can observe an
		// initialized replica that is not in s.replicasByKey, e.g., if we race with
		// a goroutine that is currently initializing repl. The lock ordering makes
		// acquiring repl.raftMu challenging; instead we require that this method is
		// called only when there is no in-flight traffic to the store, at which
		// point acquiring repl.raftMu is unnecessary.
		if repl.IsInitialized() {
			if ex := s.mu.replicasByKey.Get(repl); ex != repl {
				log.Fatalf(ctx, "%v misplaced in replicasByKey; found %v instead", repl, ex)
			}
		} else if _, ok := s.mu.uninitReplicas[repl.RangeID]; !ok {
			log.Fatalf(ctx, "%v missing from uninitReplicas", repl)
		}
		return true // keep iterating
	})
}

func NewTestStorePool(cfg StoreConfig) *StorePool {
	TimeUntilStoreDead.Override(&cfg.Settings.SV, TestTimeUntilStoreDeadOff)
	return NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Gossip,
		cfg.Clock,
		// NodeCountFunc
		func() int {
			return 1
		},
		func(roachpb.NodeID, time.Time, time.Duration) storagepb.NodeLivenessStatus {
			return storagepb.NodeLivenessStatus_LIVE
		},
		/* deterministic */ false,
	)
}

func (r *Replica) AssertState(ctx context.Context, reader storage.Reader) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.assertStateLocked(ctx, reader)
}

func (r *Replica) RaftLock() {
	r.raftMu.Lock()
}

func (r *Replica) RaftUnlock() {
	r.raftMu.Unlock()
}

// GetLastIndex is the same function as LastIndex but it does not require
// that the replica lock is held.
func (r *Replica) GetLastIndex() (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.raftLastIndexLocked()
}

func (r *Replica) LastAssignedLeaseIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
}

// MaxClosed returns the maximum closed timestamp known to the Replica.
func (r *Replica) MaxClosed(ctx context.Context) (_ hlc.Timestamp, ok bool) {
	return r.maxClosed(ctx)
}

// SetQuotaPool allows the caller to set a replica's quota pool initialized to
// a given quota. Additionally it initializes the replica's quota release queue
// and its command sizes map. Only safe to call on the replica that is both
// lease holder and raft leader while holding the raftMu.
func (r *Replica) InitQuotaPool(quota uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var appliedIndex uint64
	err := r.withRaftGroupLocked(false, func(r *raft.RawNode) (unquiesceAndWakeLeader bool, err error) {
		appliedIndex = r.BasicStatus().Applied
		return false, nil
	})
	if err != nil {
		return err
	}

	r.mu.proposalQuotaBaseIndex = appliedIndex
	if r.mu.proposalQuota != nil {
		r.mu.proposalQuota.Close("re-creating")
	}
	r.mu.proposalQuota = quotapool.NewIntPool(r.rangeStr.String(), quota)
	r.mu.quotaReleaseQueue = nil
	return nil
}

// QuotaAvailable returns the quota available in the replica's quota pool. Only
// safe to call on the replica that is both lease holder and raft leader.
func (r *Replica) QuotaAvailable() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.proposalQuota.ApproximateQuota()
}

// GetProposalQuota returns the Replica's internal proposal quota.
// It is not safe to be used concurrently so do ensure that the Replica is
// no longer active.
func (r *Replica) GetProposalQuota() *quotapool.IntPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.proposalQuota
}

func (r *Replica) QuotaReleaseQueueLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.quotaReleaseQueue)
}

func (r *Replica) IsFollowerActive(ctx context.Context, followerID roachpb.ReplicaID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastUpdateTimes.isFollowerActive(ctx, followerID, timeutil.Now())
}

// GetTSCacheHighWater returns the high water mark of the replica's timestamp
// cache.
func (r *Replica) GetTSCacheHighWater() hlc.Timestamp {
	start := roachpb.Key(r.Desc().StartKey)
	end := roachpb.Key(r.Desc().EndKey)
	t, _ := r.store.tsCache.GetMax(start, end)
	return t
}

// ShouldBackpressureWrites returns whether writes to the range should be
// subject to backpressure.
func (r *Replica) ShouldBackpressureWrites() bool {
	return r.shouldBackpressureWrites()
}

// GetRaftLogSize returns the approximate raft log size and whether it is
// trustworthy.. See r.mu.raftLogSize for details.
func (r *Replica) GetRaftLogSize() (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.raftLogSize, r.mu.raftLogSizeTrusted
}

// GetCachedLastTerm returns the cached last term value. May return
// invalidLastTerm if the cache is not set.
func (r *Replica) GetCachedLastTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastTerm
}

func (r *Replica) IsRaftGroupInitialized() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.internalRaftGroup != nil
}

// GetStoreList exposes getStoreList for testing only, but with a hardcoded
// storeFilter of storeFilterNone.
func (sp *StorePool) GetStoreList(rangeID roachpb.RangeID) (StoreList, int, int) {
	list, available, throttled := sp.getStoreList(rangeID, storeFilterNone)
	return list, available, len(throttled)
}

// Stores returns a copy of sl.stores.
func (sl *StoreList) Stores() []roachpb.StoreDescriptor {
	stores := make([]roachpb.StoreDescriptor, len(sl.stores))
	copy(stores, sl.stores)
	return stores
}

// SideloadedRaftMuLocked returns r.raftMu.sideloaded. Requires a previous call
// to RaftLock() or some other guarantee that r.raftMu is held.
func (r *Replica) SideloadedRaftMuLocked() SideloadStorage {
	return r.raftMu.sideloaded
}

func MakeSSTable(key, value string, ts hlc.Timestamp) ([]byte, storage.MVCCKeyValue) {
	sstFile := &storage.MemFile{}
	sst := storage.MakeIngestionSSTWriter(sstFile)
	defer sst.Close()

	v := roachpb.MakeValueFromBytes([]byte(value))
	v.InitChecksum([]byte(key))

	kv := storage.MVCCKeyValue{
		Key: storage.MVCCKey{
			Key:       []byte(key),
			Timestamp: ts,
		},
		Value: v.RawBytes,
	}

	if err := sst.Put(kv.Key, kv.Value); err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	if err := sst.Finish(); err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	return sstFile.Data(), kv
}

func ProposeAddSSTable(ctx context.Context, key, val string, ts hlc.Timestamp, store *Store) error {
	var ba roachpb.BatchRequest
	ba.RangeID = store.LookupReplica(roachpb.RKey(key)).RangeID

	var addReq roachpb.AddSSTableRequest
	addReq.Data, _ = MakeSSTable(key, val, ts)
	addReq.Key = roachpb.Key(key)
	addReq.EndKey = addReq.Key.Next()
	ba.Add(&addReq)

	_, pErr := store.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}

func SetMockAddSSTable() (undo func()) {
	prev, _ := batcheval.LookupCommand(roachpb.AddSSTable)

	// TODO(tschottdorf): this already does nontrivial work. Worth open-sourcing the relevant
	// subparts of the real evalAddSSTable to make this test less likely to rot.
	evalAddSSTable := func(
		ctx context.Context, _ storage.ReadWriter, cArgs batcheval.CommandArgs, _ roachpb.Response,
	) (result.Result, error) {
		log.Event(ctx, "evaluated testing-only AddSSTable mock")
		args := cArgs.Args.(*roachpb.AddSSTableRequest)

		return result.Result{
			Replicated: storagepb.ReplicatedEvalResult{
				AddSSTable: &storagepb.ReplicatedEvalResult_AddSSTable{
					Data:  args.Data,
					CRC32: util.CRC32(args.Data),
				},
			},
		}, nil
	}

	batcheval.UnregisterCommand(roachpb.AddSSTable)
	batcheval.RegisterReadWriteCommand(roachpb.AddSSTable, batcheval.DefaultDeclareKeys, evalAddSSTable)
	return func() {
		batcheval.UnregisterCommand(roachpb.AddSSTable)
		batcheval.RegisterReadWriteCommand(roachpb.AddSSTable, prev.DeclareKeys, prev.EvalRW)
	}
}

// IsQuiescent returns whether the replica is quiescent or not.
func (r *Replica) IsQuiescent() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.quiescent
}

// GetQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) GetQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	return r.getQueueLastProcessed(ctx, queue)
}

func (r *Replica) UnquiesceAndWakeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unquiesceAndWakeLeaderLocked()
}

func (r *Replica) ReadProtectedTimestamps(ctx context.Context) {
	var ts cachedProtectedTimestampState
	defer r.maybeUpdateCachedProtectedTS(&ts)
	r.mu.RLock()
	defer r.mu.RUnlock()
	ts = r.readProtectedTimestampsRLocked(ctx, nil /* f */)
}

func (nl *NodeLiveness) SetDrainingInternal(
	ctx context.Context, liveness storagepb.Liveness, drain bool,
) error {
	return nl.setDrainingInternal(ctx, liveness, drain, nil /* reporter */)
}

func (nl *NodeLiveness) SetDecommissioningInternal(
	ctx context.Context, nodeID roachpb.NodeID, liveness storagepb.Liveness, decommission bool,
) (changeCommitted bool, err error) {
	return nl.setDecommissioningInternal(ctx, nodeID, liveness, decommission)
}

// GetCircuitBreaker returns the circuit breaker controlling
// connection attempts to the specified node.
func (t *RaftTransport) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) *circuit.Breaker {
	return t.dialer.GetCircuitBreaker(nodeID, class)
}

func WriteRandomDataToRange(
	t testing.TB, store *Store, rangeID roachpb.RangeID, keyPrefix []byte,
) (midpoint []byte) {
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := append([]byte(nil), keyPrefix...)
		key = append(key, randutil.RandBytes(src, int(src.Int31n(1<<7)))...)
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		if _, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Return approximate midway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	midKey := append([]byte(nil), keyPrefix...)
	midKey = append(midKey, []byte("Z")...)
	return midKey
}

func WatchForDisappearingReplicas(t testing.TB, store *Store) {
	m := make(map[int64]struct{})
	for {
		select {
		case <-store.Stopper().ShouldQuiesce():
			return
		default:
		}

		store.mu.replicas.Range(func(k int64, v unsafe.Pointer) bool {
			m[k] = struct{}{}
			return true
		})

		for k := range m {
			if _, ok := store.mu.replicas.Load(k); !ok {
				t.Fatalf("r%d disappeared from Store.mu.replicas map", k)
			}
		}
	}
}
