// Copyright 2014 The Cockroach Authors.
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

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/abortspan"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/ts"
	"gitee.com/kwbasedb/kwbase/pkg/ts/tspb"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/ctxgroup"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
)

// adminSplitArgs creates an AdminSplitRequest for the provided split key.
func adminSplitArgs(splitKey roachpb.Key) *roachpb.AdminSplitRequest {
	return &roachpb.AdminSplitRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: splitKey,
		},
		SplitKey: splitKey,
	}
}

// TestStoreRangeSplitAtIllegalKeys verifies a range cannot be split
// at illegal keys.
func TestStoreRangeSplitAtIllegalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	cfg := kvserver.TestStoreConfig(nil)
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.DisableMergeQueue = true
	store := createTestStoreWithConfig(t, stopper, cfg)

	for _, key := range []roachpb.Key{
		keys.Meta1Prefix,
		testutils.MakeKey(keys.Meta1Prefix, []byte("a")),
		testutils.MakeKey(keys.Meta1Prefix, roachpb.RKeyMax),
		keys.Meta2KeyMax,
		testutils.MakeKey(keys.Meta2KeyMax, []byte("a")),
		keys.MakeTablePrefix(10 /* system descriptor ID */),
	} {
		args := adminSplitArgs(key)
		_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args)
		if !testutils.IsPError(pErr, "cannot split") {
			t.Errorf("%q: unexpected split error %s", key, pErr)
		}
	}
}

// Verify that on a split, only the non-expired abort span records are copied
// into the right hand side of the split.
func TestStoreSplitAbortSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manualClock := hlc.NewManualClock(2400 * time.Hour.Nanoseconds())
	clock := hlc.NewClock(manualClock.UnixNano, time.Millisecond)
	storeCfg := kvserver.TestStoreConfig(clock)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	ctx := context.Background()

	left, middle, right := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	txn := func(key roachpb.Key, ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction("test", key, 0, ts, 0)
		return &txn
	}

	var expAll []roachpb.AbortSpanEntry

	populateAbortSpan := func(key roachpb.Key, ts hlc.Timestamp) *roachpb.ResolveIntentRequest {
		pushee := txn(key, ts)

		// First write an intent on the key...
		incArgs := incrementArgs(key, 1)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: pushee}, incArgs)
		if pErr != nil {
			t.Fatalf("while sending +%v: %s", incArgs, pErr)
		}

		// Then resolve the intent and poison. Without the intent write, the
		// intent resolution would be a no-op and wouldn't leave an AbortSpan
		// entry.
		expAll = append(expAll, roachpb.AbortSpanEntry{
			Key:       key,
			Timestamp: ts,
		})
		return &roachpb.ResolveIntentRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
			IntentTxn: pushee.TxnMeta,
			Status:    roachpb.ABORTED,
			Poison:    true,
		}
	}

	key := func(k roachpb.Key, i int) roachpb.Key {
		var r []byte
		r = append(r, k...)
		r = append(r, []byte(strconv.Itoa(i))...)
		return r
	}

	thresh := storagebase.TxnCleanupThreshold.Nanoseconds()
	// Pick a non-gcable and gcable timestamp, respectively. Avoid the clock's
	// exact timestamp because of unpredictable logical ticks.
	tsFresh := hlc.Timestamp{WallTime: manualClock.UnixNano() - thresh + 1}
	tsStale := hlc.Timestamp{WallTime: manualClock.UnixNano() - thresh - 1}

	args := []roachpb.Request{
		populateAbortSpan(key(left, 1), tsFresh),
		populateAbortSpan(key(left, 2), tsStale),
		populateAbortSpan(key(middle, 1), tsFresh),
		populateAbortSpan(key(middle, 2), tsStale),
		populateAbortSpan(key(right, 1), tsFresh),
		populateAbortSpan(key(right, 2), tsStale),
		adminSplitArgs(middle),
	}

	// Nothing gets removed from the LHS during the split. This could
	// be done but has to be done carefully to avoid large Raft proposals,
	// and the stats computation needs to be checked carefully.
	expL := []roachpb.AbortSpanEntry{
		{Key: key(left, 1), Timestamp: tsFresh},
		{Key: key(left, 2), Timestamp: tsStale},
		{Key: key(middle, 1), Timestamp: tsFresh},
		{Key: key(middle, 2), Timestamp: tsStale},
		{Key: key(right, 1), Timestamp: tsFresh},
		{Key: key(right, 2), Timestamp: tsStale},
	}

	// But we don't blindly copy everything over to the RHS. Only entries with
	// recent timestamp are duplicated. This is important because otherwise the
	// Raft command size can blow up and splits fail.
	expR := []roachpb.AbortSpanEntry{
		{Key: key(left, 1), Timestamp: tsFresh},
		{Key: key(middle, 1), Timestamp: tsFresh},
		{Key: key(right, 1), Timestamp: tsFresh},
	}

	for _, arg := range args {
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), arg)
		if pErr != nil {
			t.Fatalf("while sending +%v: %s", arg, pErr)
		}
	}

	collect := func(as *abortspan.AbortSpan) []roachpb.AbortSpanEntry {
		var results []roachpb.AbortSpanEntry
		if err := as.Iterate(ctx, store.Engine(), func(_ roachpb.Key, entry roachpb.AbortSpanEntry) error {
			entry.Priority = 0 // don't care about that
			results = append(results, entry)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		sort.Slice(results, func(i, j int) bool {
			c := bytes.Compare(results[i].Key, results[j].Key)
			if c == 0 {
				return results[i].Timestamp.Less(results[j].Timestamp)
			}
			return c < 0
		})
		return results
	}

	l := collect(store.LookupReplica(keys.MustAddr(left)).AbortSpan())
	r := collect(store.LookupReplica(keys.MustAddr(right)).AbortSpan())

	if !reflect.DeepEqual(expL, l) {
		t.Fatalf("left hand side: expected %+v, got %+v", expL, l)
	}
	if !reflect.DeepEqual(expR, r) {
		t.Fatalf("right hand side: expected %+v, got %+v", expR, r)
	}
}

// TestStoreRangeSplitAtTablePrefix verifies a range can be split at
// UserTableDataMin and still gossip the SystemConfig properly.
func TestStoreRangeSplitAtTablePrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	key := keys.UserTableDataMin
	args := adminSplitArgs(key)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatalf("%q: split unexpected error: %s", key, pErr)
	}

	var desc sqlbase.TableDescriptor
	descBytes, err := protoutil.Marshal(&desc)
	if err != nil {
		t.Fatal(err)
	}

	// Update SystemConfig to trigger gossip.
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		// We don't care about the values, just the keys.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(keys.MinUserDescID))
		return txn.Put(ctx, k, &desc)
	}); err != nil {
		t.Fatal(err)
	}

	successChan := make(chan struct{}, 1)
	store.Gossip().RegisterCallback(gossip.KeySystemConfig, func(_ string, content roachpb.Value) {
		contentBytes, err := content.GetBytes()
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(contentBytes, descBytes) {
			select {
			case successChan <- struct{}{}:
			default:
			}
		}
	})

	select {
	case <-time.After(time.Second):
		t.Errorf("expected a schema gossip containing %q, but did not see one", descBytes)
	case <-successChan:
	}
}

// TestStoreRangeSplitInsideRow verifies an attempt to split a range inside of
// a table row will cause a split at a boundary between rows.
func TestStoreRangeSplitInsideRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Manually create some the column keys corresponding to the table:
	//
	//   CREATE TABLE t (id STRING PRIMARY KEY, col1 INT, col2 INT)
	tableKey := keys.MakeTablePrefix(keys.MinUserDescID)
	rowKey := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
	rowKey = encoding.EncodeStringAscending(encoding.EncodeVarintAscending(rowKey, 1), "a")
	col1Key, err := keys.EnsureSafeSplitKey(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 1))
	if err != nil {
		t.Fatal(err)
	}
	col2Key, err := keys.EnsureSafeSplitKey(keys.MakeFamilyKey(append([]byte(nil), rowKey...), 2))
	if err != nil {
		t.Fatal(err)
	}

	// We don't care about the value, so just store any old thing.
	if err := store.DB().Put(context.TODO(), col1Key, "column 1"); err != nil {
		t.Fatal(err)
	}
	if err := store.DB().Put(context.TODO(), col2Key, "column 2"); err != nil {
		t.Fatal(err)
	}

	// Split between col1Key and col2Key by splitting before col2Key.
	args := adminSplitArgs(col2Key)
	_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args)
	if pErr != nil {
		t.Fatalf("%s: split unexpected error: %s", col1Key, pErr)
	}

	repl1 := store.LookupReplica(roachpb.RKey(col1Key))
	repl2 := store.LookupReplica(roachpb.RKey(col2Key))

	// Verify the two columns are still on the same range.
	if !reflect.DeepEqual(repl1, repl2) {
		t.Fatalf("%s: ranges differ: %+v vs %+v", col1Key, repl1, repl2)
	}
	// Verify we split on a row key.
	if startKey := repl1.Desc().StartKey; !startKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s", col1Key, rowKey, startKey)
	}

	// Verify the previous range was split on a row key.
	repl3 := store.LookupReplica(tableKey)
	if endKey := repl3.Desc().EndKey; !endKey.Equal(rowKey) {
		t.Fatalf("%s: expected split on %s, but found %s", col1Key, rowKey, endKey)
	}
}

// TestStoreRangeSplitIntents executes a split of a range and verifies
// that all intents are cleared and the transaction record cleaned up.
func TestStoreRangeSplitIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), []byte("foo"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), []byte("bar"))
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the range.
	splitKey := roachpb.Key("m")
	args := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	splitKeyAddr, err := keys.Addr(splitKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(roachpb.RKeyMin), keys.RangeDescriptorKey(splitKeyAddr)} {
		if _, _, err := storage.MVCCGet(
			context.Background(), store.Engine(), key, store.Clock().Now(), storage.MVCCGetOptions{},
		); err != nil {
			t.Errorf("failed to read consistent range descriptor for key %s: %+v", key, err)
		}
	}

	txnPrefix := func(key roachpb.Key) roachpb.Key {
		rk, err := keys.Addr(key)
		if err != nil {
			t.Fatal(err)
		}
		return keys.MakeRangeKey(rk, keys.LocalTransactionSuffix, nil)
	}
	// Verify the transaction record is gone.
	start := storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMin))
	end := storage.MakeMVCCMetadataKey(keys.MakeRangeKeyPrefix(roachpb.RKeyMax))
	iter := store.Engine().NewIterator(storage.IterOptions{UpperBound: roachpb.KeyMax})

	defer iter.Close()
	for iter.SeekGE(start); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok || !iter.UnsafeKey().Less(end) {
			break
		}

		if bytes.HasPrefix([]byte(iter.Key().Key), txnPrefix(roachpb.KeyMin)) ||
			bytes.HasPrefix([]byte(iter.Key().Key), txnPrefix(splitKey)) {
			t.Errorf("unexpected system key: %s; txn record should have been cleaned up", iter.Key())
		}
	}
}

// TestStoreRangeSplitAtRangeBounds verifies that attempting to
// split a range at its start key is a no-op and does not actually
// perform a split (would create zero-length range!). This sort
// of thing might happen in the wild if two split requests arrived for
// same key. The first one succeeds and second would try to split
// at the start of the newly split range.
func TestStoreRangeSplitAtRangeBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split range 1 at an arbitrary key.
	key := roachpb.Key("a")
	rngID := store.LookupReplica(roachpb.RKey(key)).RangeID
	h := roachpb.Header{RangeID: rngID}
	args := adminSplitArgs(key)
	if _, pErr := kv.SendWrappedWith(context.Background(), store, h, args); pErr != nil {
		t.Fatal(pErr)
	}
	replCount := store.ReplicaCount()

	// An AdminSplit request sent to the end of the old range
	// should fail with a RangeKeyMismatchError.
	_, pErr := kv.SendWrappedWith(context.Background(), store, h, args)
	if _, ok := pErr.GetDetail().(*roachpb.RangeKeyMismatchError); !ok {
		t.Fatalf("expected RangeKeyMismatchError, found: %v", pErr)
	}

	// An AdminSplit request sent to the start of the new range
	// should succeed but no new ranges should be created.
	newRng := store.LookupReplica(roachpb.RKey(key))
	h.RangeID = newRng.RangeID
	if _, pErr := kv.SendWrappedWith(context.Background(), store, h, args); pErr != nil {
		t.Fatal(pErr)
	}

	newReplCount := store.ReplicaCount()
	if replCount != newReplCount {
		t.Fatalf("splitting at a range boundary should not create a new range; before second split "+
			"found %d ranges, after second split found %d ranges", replCount, newReplCount)
	}
}

// TestSplitTriggerRaftSnapshotRace verifies that when an uninitialized Replica
// resulting from a split hasn't been initialized via the split trigger yet, a
// grace period prevents the replica from requesting an errant Raft snapshot.
// This is verified by running a number of splits and asserting that no Raft
// snapshots are observed. As a nice side effect, this also verifies that log
// truncations don't cause any Raft snapshots in this test.
func TestSplitTriggerRaftSnapshotRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	const numNodes = 3
	var args base.TestClusterArgs
	// NB: the merge queue is enabled for additional "chaos". Note that the test
	// uses three nodes and so there is no replica movement, which would other-
	// wise tickle Raft snapshots for unrelated reasons.
	tc := testcluster.StartTestCluster(t, numNodes, args)
	defer tc.Stopper().Stop(ctx)

	numSplits := 100
	if util.RaceEnabled {
		// Running 100 splits is overkill in race builds.
		numSplits = 10
	}
	perm := rand.Perm(numSplits)
	idx := int32(-1) // accessed atomically

	numRaftSnaps := func(when string) int {
		var totalSnaps int
		for i := 0; i < numNodes; i++ {
			var n int // num rows (sanity check against test rotting)
			var c int // num Raft snapshots
			if err := tc.ServerConn(i).QueryRow(`
SELECT count(*), sum(value) FROM kwdb_internal.node_metrics WHERE
	name = 'range.snapshots.normal-applied'
`).Scan(&n, &c); err != nil {
				t.Fatal(err)
			}
			if expRows := 1; n != expRows {
				t.Fatalf("%s: expected %d rows, got %d", when, expRows, n)
			}
			totalSnaps += c
		}
		return totalSnaps
	}

	// There are usually no raft snaps before, but there is a race condition where
	// they can occasionally happen during upreplication.
	numSnapsBefore := numRaftSnaps("before")

	doSplit := func(ctx context.Context, _ int) error {
		_, _, err := tc.SplitRange(
			[]byte(fmt.Sprintf("key-%d", perm[atomic.AddInt32(&idx, 1)])))
		return err
	}

	if err := ctxgroup.GroupWorkers(ctx, numSplits, doSplit); err != nil {
		t.Fatal(err)
	}

	// Check that no snaps happened during the splits.
	require.Equal(t, numSnapsBefore, numRaftSnaps("after"))
}

// TestStoreRangeSplitIdempotency executes a split of a range and
// verifies that the resulting ranges respond to the right key ranges
// and that their stats have been properly accounted for and requests
// can't be replayed.
func TestStoreRangeSplitIdempotency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithOpts(t,
		testStoreOpts{
			// This test was written before the test stores were able to start with
			// more than one range and is not prepared to handle many ranges.
			dontCreateSystemRanges: true,
			cfg:                    &storeCfg},
		stopper)
	rangeID := roachpb.RangeID(1)
	splitKey := roachpb.Key("m")
	content := roachpb.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs := putArgs([]byte("c"), content)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs([]byte("x"), content)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Increments are a good way of testing idempotency. Up here, we
	// address them to the original range, then later to the one that
	// contains the key.
	txn := roachpb.MakeTransaction("test", []byte("c"), 10, store.Clock().Now(), 0)
	lIncArgs := incrementArgs([]byte("apoptosis"), 100)
	lTxn := txn
	lTxn.Sequence++
	lIncArgs.Sequence = lTxn.Sequence
	if _, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		Txn: &lTxn,
	}, lIncArgs); pErr != nil {
		t.Fatal(pErr)
	}
	rIncArgs := incrementArgs([]byte("wobble"), 10)
	rTxn := txn
	rTxn.Sequence++
	rIncArgs.Sequence = rTxn.Sequence
	if _, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		Txn: &rTxn,
	}, rIncArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the original stats for key and value bytes.
	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), store.Engine())
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, valBytes := ms.KeyBytes, ms.ValBytes

	// Split the range.
	args := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify no intents remains on range descriptor keys.
	splitKeyAddr, err := keys.Addr(splitKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []roachpb.Key{keys.RangeDescriptorKey(roachpb.RKeyMin), keys.RangeDescriptorKey(splitKeyAddr)} {
		if _, _, err := storage.MVCCGet(
			context.Background(), store.Engine(), key, store.Clock().Now(), storage.MVCCGetOptions{},
		); err != nil {
			t.Fatal(err)
		}
	}

	repl := store.LookupReplica(roachpb.RKeyMin)
	rngDesc := repl.Desc()
	newRng := store.LookupReplica([]byte("m"))
	newRngDesc := newRng.Desc()
	if !bytes.Equal(newRngDesc.StartKey, splitKey) || !bytes.Equal(splitKey, rngDesc.EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRngDesc.StartKey, splitKey, rngDesc.EndKey)
	}
	if !bytes.Equal(newRngDesc.EndKey, roachpb.RKeyMax) || !bytes.Equal(rngDesc.StartKey, roachpb.RKeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rngDesc.StartKey, newRngDesc.EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs := getArgs([]byte("c"))
	if reply, pErr := kv.SendWrapped(context.Background(), store.TestSender(), gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, pErr := reply.(*roachpb.GetResponse).Value.GetBytes(); pErr != nil {
		t.Fatal(pErr)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}
	gArgs = getArgs([]byte("x"))
	if reply, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		RangeID: newRng.RangeID,
	}, gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, content) {
		t.Fatalf("actual value %q did not match expected value %q", replyBytes, content)
	}

	// Send out an increment request copied from above (same txn/sequence)
	// which remains in the old range.
	_, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		Txn: &lTxn,
	}, lIncArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Send out the same increment copied from above (same txn/sequence), but
	// now to the newly created range (which should hold that key).
	_, pErr = kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
		RangeID: newRng.RangeID,
		Txn:     &rTxn,
	}, rIncArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Compare stats of split ranges to ensure they are non zero and
	// exceed the original range when summed.
	left, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), store.Engine())
	if err != nil {
		t.Fatal(err)
	}
	lKeyBytes, lValBytes := left.KeyBytes, left.ValBytes
	right, err := stateloader.Make(newRng.RangeID).LoadMVCCStats(context.Background(), store.Engine())
	if err != nil {
		t.Fatal(err)
	}
	rKeyBytes, rValBytes := right.KeyBytes, right.ValBytes

	if lKeyBytes == 0 || rKeyBytes == 0 {
		t.Errorf("expected non-zero key bytes; got %d, %d", lKeyBytes, rKeyBytes)
	}
	if lValBytes == 0 || rValBytes == 0 {
		t.Errorf("expected non-zero val bytes; got %d, %d", lValBytes, rValBytes)
	}
	if lKeyBytes+rKeyBytes <= keyBytes {
		t.Errorf("left + right key bytes don't match; %d + %d <= %d", lKeyBytes, rKeyBytes, keyBytes)
	}
	if lValBytes+rValBytes <= valBytes {
		t.Errorf("left + right val bytes don't match; %d + %d <= %d", lValBytes, rValBytes, valBytes)
	}
}

// TestStoreRangeSplitStats starts by splitting the system keys from user-space
// keys and verifying that the user space side of the split (which is empty),
// has all zeros for stats. It then writes random data to the user space side,
// splits it halfway and verifies the two splits have stats exactly equaling
// the pre-split.
func TestStoreRangeSplitStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	storeCfg := kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	ctx := context.Background()

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MinUserDescID)
	args := adminSplitArgs(keyPrefix)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	repl := store.LookupReplica(keyPrefix)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), repl.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random data.
	midKey := kvserver.WriteRandomDataToRange(t, store, repl.RangeID, keyPrefix)

	// Get the range stats now that we have data.
	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	ms, err := stateloader.Make(repl.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRecomputedStats(snap, repl.Desc(), ms, manual.UnixNano()); err != nil {
		t.Fatalf("failed to verify range's stats before split: %+v", err)
	}
	if inMemMS := repl.GetMVCCStats(); inMemMS != ms {
		t.Fatalf("in-memory and on-disk diverged:\n%+v\n!=\n%+v", inMemMS, ms)
	}

	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(midKey)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: repl.RangeID,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	snap = store.Engine().NewSnapshot()
	defer snap.Close()
	msLeft, err := stateloader.Make(repl.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	replRight := store.LookupReplica(midKey)
	msRight, err := stateloader.Make(replRight.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// The stats should be exactly equal when added.
	expMS := enginepb.MVCCStats{
		LiveBytes:   msLeft.LiveBytes + msRight.LiveBytes,
		KeyBytes:    msLeft.KeyBytes + msRight.KeyBytes,
		ValBytes:    msLeft.ValBytes + msRight.ValBytes,
		IntentBytes: msLeft.IntentBytes + msRight.IntentBytes,
		LiveCount:   msLeft.LiveCount + msRight.LiveCount,
		KeyCount:    msLeft.KeyCount + msRight.KeyCount,
		ValCount:    msLeft.ValCount + msRight.ValCount,
		IntentCount: msLeft.IntentCount + msRight.IntentCount,
	}
	ms.SysBytes, ms.SysCount = 0, 0
	ms.LastUpdateNanos = 0
	if expMS != ms {
		t.Errorf("expected left plus right ranges to equal original, but\n %+v\n+\n %+v\n!=\n %+v", msLeft, msRight, ms)
	}

	// Stats should both have the new timestamp.
	now := manual.UnixNano()
	if lTs := msLeft.LastUpdateNanos; lTs != now {
		t.Errorf("expected left range stats to have new timestamp, want %d, got %d", now, lTs)
	}
	if rTs := msRight.LastUpdateNanos; rTs != now {
		t.Errorf("expected right range stats to have new timestamp, want %d, got %d", now, rTs)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, repl.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %+v", err)
	}
	if err := verifyRecomputedStats(snap, replRight.Desc(), msRight, now); err != nil {
		t.Fatalf("failed to verify right range's stats after split: %+v", err)
	}
}

// RaftMessageHandlerInterceptor wraps a storage.RaftMessageHandler. It
// delegates all methods to the underlying storage.RaftMessageHandler, except
// that HandleSnapshot calls receiveSnapshotFilter with the snapshot request
// header before delegating to the underlying HandleSnapshot method.
type RaftMessageHandlerInterceptor struct {
	kvserver.RaftMessageHandler
	handleSnapshotFilter func(header *kvserver.SnapshotRequest_Header)
}

func (mh RaftMessageHandlerInterceptor) HandleSnapshot(
	header *kvserver.SnapshotRequest_Header, respStream kvserver.SnapshotResponseStream,
) error {
	mh.handleSnapshotFilter(header)
	return mh.RaftMessageHandler.HandleSnapshot(header, respStream)
}

// TestStoreEmptyRangeSnapshotSize tests that the snapshot request header for a
// range that contains no user data (an "empty" range) has RangeSize == 0. This
// is arguably a bug, because system data like the range descriptor and raft log
// should also count towards the size of the snapshot. Currently, though, this
// property conveniently allows us to optimize the rebalancing of empty ranges
// by throttling snapshots of empty ranges separately from non-empty snapshots.
//
// If you change the accounting of RangeSize such that this test breaks, please
// preserve the optimization by introducing an alternative means of identifying
// snapshot requests for empty or near-empty ranges, and then adjust this test
// accordingly.
func TestStoreEmptyRangeSnapshotSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// Disable the replicate queue, the split queue, and the merge queue as we
	// want to control both rebalancing, splits, and merges ourselves.
	sc := kvserver.TestStoreConfig(nil)
	sc.TestingKnobs.DisableReplicateQueue = true
	sc.TestingKnobs.DisableSplitQueue = true
	sc.TestingKnobs.DisableMergeQueue = true

	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Split the range after the last table data key to get a range that contains
	// no user data.
	splitKey := keys.MakeTablePrefix(keys.MinUserDescID)
	splitArgs := adminSplitArgs(splitKey)
	if _, err := kv.SendWrapped(ctx, mtc.distSenders[0], splitArgs); err != nil {
		t.Fatal(err)
	}

	// Wrap store 1's message handler to intercept and record all incoming
	// snapshot request headers.
	messageRecorder := struct {
		syncutil.Mutex
		headers []*kvserver.SnapshotRequest_Header
	}{}
	messageHandler := RaftMessageHandlerInterceptor{
		RaftMessageHandler: mtc.stores[1],
		handleSnapshotFilter: func(header *kvserver.SnapshotRequest_Header) {
			// Each snapshot request is handled in a new goroutine, so we need
			// synchronization.
			messageRecorder.Lock()
			defer messageRecorder.Unlock()
			messageRecorder.headers = append(messageRecorder.headers, header)
		},
	}
	mtc.transport.Listen(mtc.stores[1].StoreID(), messageHandler)

	// Replicate the newly-split range to trigger a snapshot request from store 0
	// to store 1.
	rangeID := mtc.stores[0].LookupReplica(splitKey).RangeID
	mtc.replicateRange(rangeID, 1)

	// Verify that we saw at least one snapshot request,
	messageRecorder.Lock()
	defer messageRecorder.Unlock()
	if a := len(messageRecorder.headers); a < 1 {
		t.Fatalf("expected at least one snapshot header, but got %d", a)
	}
	for i, header := range messageRecorder.headers {
		if e, a := header.State.Desc.RangeID, rangeID; e != a {
			t.Errorf("%d: expected RangeID to be %d, but got %d", i, e, a)
		}
		if header.RangeSize != 0 {
			t.Errorf("%d: expected RangeSize to be 0, but got %d", i, header.RangeSize)
		}
	}
}

// TestStoreRangeSplitStatsWithMerges starts by splitting the system keys from
// user-space keys and verifying that the user space side of the split (which is empty),
// has all zeros for stats. It then issues a number of Merge requests to the user
// space side, simulating TimeSeries data. Finally, the test splits the user space
// side halfway and verifies the stats on either side of the split are equal to a
// recomputation.
//
// Note that unlike TestStoreRangeSplitStats, we do not check if the two halves of the
// split's stats are equal to the pre-split stats when added, because this will not be
// true of ranges populated with Merge requests. The reason for this is that Merge
// requests' impact on MVCCStats are only estimated. See updateStatsOnMerge.
func TestStoreRangeSplitStatsWithMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	storeCfg := kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	ctx := context.Background()

	// Split the range after the last table data key.
	keyPrefix := keys.MakeTablePrefix(keys.MinUserDescID)
	args := adminSplitArgs(keyPrefix)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}
	// Verify empty range has empty stats.
	repl := store.LookupReplica(keyPrefix)
	// NOTE that this value is expected to change over time, depending on what
	// we store in the sys-local keyspace. Update it accordingly for this test.
	empty := enginepb.MVCCStats{LastUpdateNanos: manual.UnixNano()}
	if err := verifyRangeStats(store.Engine(), repl.RangeID, empty); err != nil {
		t.Fatal(err)
	}

	// Write random TimeSeries data.
	midKey := writeRandomTimeSeriesDataToRange(t, store, repl.RangeID, keyPrefix)
	manual.Increment(100)

	// Split the range at approximate halfway point.
	args = adminSplitArgs(midKey)
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
		RangeID: repl.RangeID,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	snap := store.Engine().NewSnapshot()
	defer snap.Close()
	msLeft, err := stateloader.Make(repl.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}
	replRight := store.LookupReplica(midKey)
	msRight, err := stateloader.Make(replRight.RangeID).LoadMVCCStats(ctx, snap)
	if err != nil {
		t.Fatal(err)
	}

	// Stats should both have the new timestamp.
	now := manual.UnixNano()
	if lTs := msLeft.LastUpdateNanos; lTs != now {
		t.Errorf("expected left range stats to have new timestamp, want %d, got %d", now, lTs)
	}
	if rTs := msRight.LastUpdateNanos; rTs != now {
		t.Errorf("expected right range stats to have new timestamp, want %d, got %d", now, rTs)
	}

	// Stats should agree with recomputation.
	if err := verifyRecomputedStats(snap, repl.Desc(), msLeft, now); err != nil {
		t.Fatalf("failed to verify left range's stats after split: %+v", err)
	}
	if err := verifyRecomputedStats(snap, replRight.Desc(), msRight, now); err != nil {
		t.Fatalf("failed to verify right range's stats after split: %+v", err)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written or the given range has split.
func fillRange(
	t *testing.T,
	store *kvserver.Store,
	rangeID roachpb.RangeID,
	prefix roachpb.Key,
	bytes int64,
	singleKey bool,
) {
	src := rand.New(rand.NewSource(0))
	var key []byte
	for {
		ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), store.Engine())
		if err != nil {
			t.Fatal(err)
		}
		keyBytes, valBytes := ms.KeyBytes, ms.ValBytes
		if keyBytes+valBytes >= bytes {
			return
		}
		if key == nil || !singleKey {
			key = append(append([]byte(nil), prefix...), randutil.RandBytes(src, 100)...)
			key = keys.MakeFamilyKey(key, src.Uint32())
		}
		val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
		pArgs := putArgs(key, val)
		_, pErr := kv.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: rangeID,
		}, pArgs)
		// When the split occurs in the background, our writes may start failing.
		// We know we can stop writing when this happens.
		if _, ok := pErr.GetDetail().(*roachpb.RangeKeyMismatchError); ok {
			return
		} else if pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestStoreZoneUpdateAndRangeSplit verifies that modifying the zone
// configuration changes range max bytes and Range.maybeSplit() takes
// max bytes into account when deciding whether to enqueue a range for
// splitting. It further verifies that the range is in fact split on
// exceeding zone's RangeMaxBytes.
func TestStoreZoneUpdateAndRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	config.TestingSetupZoneConfigHook(stopper)

	const maxBytes = 1 << 16
	// Set max bytes.
	descID := uint32(keys.MinUserDescID)
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
	config.TestingSetZoneConfig(descID, zoneConfig)

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfigEntries{}, 0); err != nil {
		t.Fatal(err)
	}

	tableBoundary := keys.MakeTablePrefix(descID)

	{
		var repl *kvserver.Replica

		// Wait for the range to be split along table boundaries.
		expectedRSpan := roachpb.RSpan{Key: roachpb.RKey(tableBoundary), EndKey: roachpb.RKeyMax}
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(tableBoundary)
			if actualRSpan := repl.Desc().RSpan(); !actualRSpan.Equal(expectedRSpan) {
				return errors.Errorf("expected range %s to span %s", repl, expectedRSpan)
			}
			return nil
		})

		// Check range's max bytes settings.
		if actualMaxBytes := repl.GetMaxBytes(); actualMaxBytes != maxBytes {
			t.Fatalf("range %s max bytes mismatch, got: %d, expected: %d", repl, actualMaxBytes, maxBytes)
		}

		// Look in the range after prefix we're writing to.
		fillRange(t, store, repl.RangeID, tableBoundary, maxBytes, false /* singleKey */)
	}

	// Verify that the range is in fact split.
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MakeTablePrefix(descID + 1))
		rngDesc := repl.Desc()
		rngStart, rngEnd := rngDesc.StartKey, rngDesc.EndKey
		if rngStart.Equal(tableBoundary) || !rngEnd.Equal(roachpb.RKeyMax) {
			return errors.Errorf("range %s has not yet split", repl)
		}
		return nil
	})
}

// TestStoreRangeSplitWithMaxBytesUpdate tests a scenario where a new
// zone config that updates the max bytes is set and triggers a range
// split.
func TestStoreRangeSplitWithMaxBytesUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	config.TestingSetupZoneConfigHook(stopper)

	origRng := store.LookupReplica(roachpb.RKeyMin)

	// Set max bytes.
	const maxBytes = 1 << 16
	descID := uint32(keys.MinUserDescID)
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
	config.TestingSetZoneConfig(descID, zoneConfig)

	// Trigger gossip callback.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfigEntries{}, 0); err != nil {
		t.Fatal(err)
	}

	// Verify that the range is split and the new range has the correct max bytes.
	testutils.SucceedsSoon(t, func() error {
		newRng := store.LookupReplica(keys.MakeTablePrefix(descID))
		if newRng.RangeID == origRng.RangeID {
			return errors.Errorf("expected new range created by split")
		}
		if newRng.GetMaxBytes() != maxBytes {
			return errors.Errorf("expected %d max bytes for the new range, but got %d",
				maxBytes, newRng.GetMaxBytes())
		}
		return nil
	})
}

// TestStoreRangeSplitBackpressureWrites tests that ranges that grow too large
// begin enforcing backpressure on writes until the range is able to split. In
// the test, a range is filled past the point where it will begin applying
// backpressure. Splits are then blocked in-flight and we test that any future
// writes wait until the split succeeds and reduces the range size beneath the
// backpressure threshold.
func TestStoreRangeSplitBackpressureWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Backpressured writes react differently depending on whether there is an
	// ongoing split or not. If there is an ongoing split then the writes wait
	// on the split are only allowed to proceed if the split succeeds. If there
	// is not an ongoing split or if the range is unsplittable and in the split
	// queue's purgatory, the write is rejected immediately.
	testCases := []struct {
		splitOngoing    bool
		splitErr        bool
		splitImpossible bool
		expErr          string
	}{
		{splitOngoing: true, splitErr: false, expErr: ""},
		{splitOngoing: true, splitErr: true, expErr: "split failed while applying backpressure.* boom"},
		{splitOngoing: false, expErr: ""},
		{splitImpossible: true, expErr: "split failed while applying backpressure.* could not find valid split key"},
	}
	for _, tc := range testCases {
		var name string
		if tc.splitImpossible {
			name = fmt.Sprintf("splitImpossible=%t", tc.splitImpossible)
		} else {
			name = fmt.Sprintf("splitOngoing=%t,splitErr=%t", tc.splitOngoing, tc.splitErr)
		}
		t.Run(name, func(t *testing.T) {
			var activateSplitFilter int32
			splitKey := roachpb.RKey(keys.UserTableDataMin)
			splitPending, blockSplits := make(chan struct{}), make(chan struct{})
			storeCfg := kvserver.TestStoreConfig(nil)
			// Set maxBytes to something small so we can exceed the maximum split
			// size without adding 2x64MB of data.
			const maxBytes = 1 << 16
			storeCfg.DefaultZoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
			storeCfg.TestingKnobs.DisableGCQueue = true
			storeCfg.TestingKnobs.DisableMergeQueue = true
			storeCfg.TestingKnobs.DisableSplitQueue = true
			storeCfg.TestingKnobs.TestingRequestFilter =
				func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
					for _, req := range ba.Requests {
						if cPut, ok := req.GetInner().(*roachpb.ConditionalPutRequest); ok {
							if cPut.Key.Equal(keys.RangeDescriptorKey(splitKey)) {
								if atomic.CompareAndSwapInt32(&activateSplitFilter, 1, 0) {
									splitPending <- struct{}{}
									<-blockSplits
									if tc.splitErr {
										return roachpb.NewErrorf("boom")
									}
								}
							}
						}
					}
					return nil
				}

			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store := createTestStoreWithConfig(t, stopper, storeCfg)

			// Split at the split key.
			sArgs := adminSplitArgs(splitKey.AsRawKey())
			repl := store.LookupReplica(splitKey)
			if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{
				RangeID: repl.RangeID,
			}, sArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// Fill the new range past the point where writes should backpressure.
			repl = store.LookupReplica(splitKey)
			singleKey := tc.splitImpossible
			fillRange(t, store, repl.RangeID, splitKey.AsRawKey(), 2*maxBytes+1, singleKey)

			if !repl.ShouldBackpressureWrites() {
				t.Fatal("expected ShouldBackpressureWrites=true, found false")
			}

			// If necessary, allow the range to begin splitting and wait until
			// it gets blocked in the response filter.
			if tc.splitOngoing {
				atomic.StoreInt32(&activateSplitFilter, 1)
				if err := stopper.RunAsyncTask(ctx, "force split", func(_ context.Context) {
					store.SetSplitQueueActive(true)
					if err := store.ForceSplitScanAndProcess(); err != nil {
						log.Fatal(ctx, err)
					}
				}); err != nil {
					t.Fatal(err)
				}
				<-splitPending
			} else if tc.splitImpossible {
				store.SetSplitQueueActive(true)
				if err := store.ForceSplitScanAndProcess(); err != nil {
					t.Fatal(err)
				}
				if l := store.SplitQueuePurgatoryLength(); l != 1 {
					t.Fatalf("expected split queue purgatory to contain 1 replica, found %d", l)
				}
			}

			// Send a Put request. This should be backpressured on the split, so it should
			// not be able to succeed until we allow the split to continue.
			putRes := make(chan error)
			go func() {
				// Write to the first key of the range to make sure that
				// we don't end up on the wrong side of the split.
				putRes <- store.DB().Put(ctx, splitKey, "test")
			}()

			// Send a Delete request in a transaction. Should also be backpressured on the split,
			// so it should not be able to succeed until we allow the split to continue.
			delRes := make(chan error)
			go func() {
				// Write to the first key of the range to make sure that
				// we don't end up on the wrong side of the split.
				delRes <- store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					b := txn.NewBatch()
					b.Del(splitKey)
					return txn.CommitInBatch(ctx, b)
				})
			}()

			// Make sure the write doesn't return while a split is ongoing. If no
			// split is ongoing, the write will return an error immediately.
			if tc.splitOngoing {
				select {
				case err := <-putRes:
					close(blockSplits)
					t.Fatalf("put was not blocked on split, returned err %v", err)
				case err := <-delRes:
					close(blockSplits)
					t.Fatalf("delete was not blocked on split, returned err %v", err)
				case <-time.After(100 * time.Millisecond):
				}

				// Let split through. Write should follow.
				close(blockSplits)
			}

			for op, resCh := range map[string]chan error{
				"put":    putRes,
				"delete": delRes,
			} {
				if err := <-resCh; tc.expErr == "" {
					if err != nil {
						t.Fatalf("%s returned err %v, expected success", op, err)
					}
				} else {
					if !testutils.IsError(err, tc.expErr) {
						t.Fatalf("%s returned err %s, expected pattern %q", op, err, tc.expErr)
					}
				}
			}

		})
	}
}

// TestStoreRangeSystemSplits verifies that splits are based on the contents of
// the system.descriptor table.
func TestStoreRangeSystemSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	//TODO(zxy): The test failed because the schema creation was supported
	t.Skip("skipped by zxy")
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	// Intentionally leave the merge queue enabled. This indrectly tests that the
	// merge queue respects these split points.
	store, _ := createTestStore(t, stopper)

	userTableMax := keys.MinUserDescID + 4
	var exceptions map[int]struct{}
	schema := sqlbase.MakeMetadataSchema(zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	// Write table descriptors for the tables in the metadata schema as well as
	// five dummy user tables. This does two things:
	//   - descriptor IDs are used to determine split keys
	//   - the write triggers a SystemConfig update and gossip
	// We should end up with splits at each user table prefix.
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		descTablePrefix := keys.MakeTablePrefix(keys.DescriptorTableID)
		kvs, _ /* splits */ := schema.GetInitialValues(clusterversion.TestingClusterVersion)
		for _, kv := range kvs {
			if !bytes.HasPrefix(kv.Key, descTablePrefix) {
				continue
			}
			if err := txn.Put(ctx, kv.Key, &kv.Value); err != nil {
				return err
			}
		}
		for i := keys.MinUserDescID; i <= userTableMax; i++ {
			// We don't care about the value, just the key.
			key := sqlbase.MakeDescMetadataKey(sqlbase.ID(i))
			if err := txn.Put(ctx, key, &sqlbase.TableDescriptor{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes := func() {
		t.Helper()
		// We expect splits at each of the user tables and at a few fixed system
		// range boundaries, but not at system config table boundaries.
		expKeys := []roachpb.Key{
			testutils.MakeKey(keys.Meta2Prefix, keys.NodeLivenessPrefix),
			testutils.MakeKey(keys.Meta2Prefix, keys.NodeLivenessKeyMax),
			testutils.MakeKey(keys.Meta2Prefix, keys.TimeseriesPrefix),
			testutils.MakeKey(keys.Meta2Prefix, keys.TimeseriesPrefix.PrefixEnd()),
			testutils.MakeKey(keys.Meta2Prefix, keys.TableDataMin),
		}
		ids := schema.DescriptorIDs()
		maxID := uint32(ids[len(ids)-1])
		for i := uint32(keys.MaxSystemConfigDescID + 1); i <= maxID; i++ {
			expKeys = append(expKeys,
				testutils.MakeKey(keys.Meta2Prefix, keys.MakeTablePrefix(i)),
			)
		}
		for i := keys.MinUserDescID; i <= userTableMax; i++ {
			if _, ok := exceptions[i]; !ok {
				expKeys = append(expKeys,
					testutils.MakeKey(keys.Meta2Prefix, keys.MakeTablePrefix(uint32(i))),
				)
			}
		}
		expKeys = append(expKeys, testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax))

		testutils.SucceedsSoon(t, func() error {
			rows, err := store.DB().Scan(context.TODO(), keys.Meta2Prefix, keys.MetaMax, 0)
			if err != nil {
				return err
			}
			keys := make([]roachpb.Key, 0, len(expKeys))
			for _, r := range rows {
				keys = append(keys, r.Key)
			}
			if !reflect.DeepEqual(keys, expKeys) {
				return errors.Errorf("expected split keys:\n%v\nbut found:\n%v", expKeys, keys)
			}
			return nil
		})
	}

	verifySplitsAtTablePrefixes()

	// Write another, disjoint (+3) descriptor for a user table.
	userTableMax += 3
	exceptions = map[int]struct{}{userTableMax - 1: {}, userTableMax - 2: {}}
	if err := store.DB().Txn(context.TODO(), func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		// This time, only write the last table descriptor. Splits only occur for
		// the descriptor we add. We don't care about the value, just the key.
		k := sqlbase.MakeDescMetadataKey(sqlbase.ID(userTableMax))
		return txn.Put(ctx, k, &sqlbase.TableDescriptor{})
	}); err != nil {
		t.Fatal(err)
	}

	verifySplitsAtTablePrefixes()
}

// runSetupSplitSnapshotRace engineers a situation in which a range has
// been split but node 3 hasn't processed it yet. There is a race
// depending on whether node 3 learns of the split from its left or
// right side. When this function returns most of the nodes will be
// stopped, and depending on the order in which they are restarted, we
// can arrange for both possible outcomes of the race.
//
// Range 1 is the system keyspace, located on node 0.
//
// The range containing leftKey is the left side of the split, located
// on nodes 1, 2, and 3.
//
// The range containing rightKey is the right side of the split,
// located on nodes 3, 4, and 5.
//
// Nodes 1-5 are stopped; only node 0 is running.
//
// See https://gitee.com/kwbasedb/kwbase/issues/1644.
func runSetupSplitSnapshotRace(
	t *testing.T, testFn func(*multiTestContext, roachpb.Key, roachpb.Key),
) {
	sc := kvserver.TestStoreConfig(nil)
	// We'll control replication by hand.
	sc.TestingKnobs.DisableReplicateQueue = true
	// Async intent resolution can sometimes lead to hangs when we stop
	// most of the stores at the end of this function.
	sc.TestingKnobs.IntentResolverKnobs.DisableAsyncIntentResolution = true
	// Avoid fighting with the merge queue while trying to reproduce this race.
	sc.TestingKnobs.DisableMergeQueue = true
	sc.TestingKnobs.DisableGCQueue = true
	// Disable the split delay mechanism, or it'll spend 10s going in circles.
	// (We can't set it to zero as otherwise the default overrides us).
	sc.RaftDelaySplitToSuppressSnapshotTicks = -1
	sc.Clock = nil // manual clock
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 6)

	leftKey := roachpb.Key("a")
	rightKey := roachpb.Key("z")

	// First, do a couple of writes; we'll use these to determine when
	// the dust has settled.
	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Split the system range from the rest of the keyspace.
	splitArgs := adminSplitArgs(keys.SystemMax)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the left range's ID. This is currently 2, but using
	// LookupReplica is more future-proof (and see below for
	// rightRangeID).
	leftRangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a")).RangeID

	// Replicate the left range onto nodes 1-3 and remove it from node 0. We have
	// to transfer the lease before unreplicating from range 0 because it isn't
	// safe (or allowed) for a leaseholder to remove itself from a cluster
	// without first giving up its lease.
	mtc.replicateRange(leftRangeID, 1, 2, 3)
	mtc.transferLease(context.TODO(), leftRangeID, 0, 1)
	mtc.unreplicateRange(leftRangeID, 0)

	mtc.waitForValues(leftKey, []int64{0, 1, 1, 1, 0, 0})
	mtc.waitForValues(rightKey, []int64{0, 2, 2, 2, 0, 0})

	// Stop node 3 so it doesn't hear about the split.
	mtc.stopStore(3)
	mtc.advanceClock(context.TODO())

	// Split the data range.
	splitArgs = adminSplitArgs(roachpb.Key("m"))
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the right range's ID. Since the split was performed on node
	// 1, it is currently 11 and not 3 as might be expected.
	var rightRangeID roachpb.RangeID
	testutils.SucceedsSoon(t, func() error {
		rightRangeID = mtc.stores[1].LookupReplica(roachpb.RKey("z")).RangeID
		if rightRangeID == leftRangeID {
			return errors.Errorf("store 1 hasn't processed split yet")
		}
		return nil
	})

	// Relocate the right range onto nodes 3-5.
	mtc.replicateRange(rightRangeID, 4, 5)
	mtc.unreplicateRange(rightRangeID, 2)
	mtc.transferLease(context.TODO(), rightRangeID, 1, 4)
	mtc.unreplicateRange(rightRangeID, 1)

	// Perform another increment after all the replication changes. This
	// lets us ensure that all the replication changes have been
	// processed and applied on all replicas. This is necessary because
	// the range is in an unstable state at the time of the last
	// unreplicateRange call above. It has four members which means it
	// can only tolerate one failure without losing quorum. That failure
	// is store 3 which we stopped earlier. Stopping store 1 too soon
	// (before it has committed the final config change *and* propagated
	// that commit to the followers 4 and 5) would constitute a second
	// failure and render the range unable to achieve quorum after
	// restart (in the SnapshotWins branch).
	incArgs = incrementArgs(rightKey, 3)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Store 3 still has the old value, but 4 and 5 are up to date.
	mtc.waitForValues(rightKey, []int64{0, 0, 0, 2, 5, 5})

	// Scan the meta ranges to resolve all intents
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0],
		&roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    keys.MetaMin,
				EndKey: keys.MetaMax,
			},
		}); pErr != nil {
		t.Fatal(pErr)
	}

	// Stop the remaining data stores.
	mtc.stopStore(1)
	mtc.stopStore(2)
	// 3 is already stopped.
	mtc.stopStore(4)
	mtc.stopStore(5)

	testFn(mtc, leftKey, rightKey)
}

// TestSplitSnapshotRace_SplitWins exercises one outcome of the
// split/snapshot race: The left side of the split propagates first,
// so the split completes before it sees a competing snapshot. This is
// the more common outcome in practice.
func TestSplitSnapshotRace_SplitWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runSetupSplitSnapshotRace(t, func(mtc *multiTestContext, leftKey, rightKey roachpb.Key) {
		// Bring the left range up first so that the split happens before it sees a snapshot.
		for i := 1; i <= 3; i++ {
			mtc.restartStore(i)
		}

		// Perform a write on the left range and wait for it to propagate.
		incArgs := incrementArgs(leftKey, 10)
		if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

		// Now wake the other stores up.
		mtc.restartStore(4)
		mtc.restartStore(5)

		// Write to the right range.
		incArgs = incrementArgs(rightKey, 20)
		if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(rightKey, []int64{0, 0, 0, 25, 25, 25})
	})
}

// TestSplitSnapshotRace_SnapshotWins exercises one outcome of the
// split/snapshot race: The right side of the split replicates first,
// so the target node sees a raft snapshot before it has processed the
// split, so it still has a conflicting range.
func TestSplitSnapshotRace_SnapshotWins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runSetupSplitSnapshotRace(t, func(mtc *multiTestContext, leftKey, rightKey roachpb.Key) {
		// Bring the right range up first.
		for i := 3; i <= 5; i++ {
			mtc.restartStore(i)
		}

		// Perform a write on the right range.
		incArgs := incrementArgs(rightKey, 20)
		if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// It immediately propagates between nodes 4 and 5, but node 3
		// remains at its old value. It can't accept the right-hand range
		// because it conflicts with its not-yet-split copy of the left-hand
		// range. This test is not completely deterministic: we want to make
		// sure that node 3 doesn't panic when it receives the snapshot, but
		// since it silently drops the message there is nothing we can wait
		// for. There is a high probability that the message will have been
		// received by the time that nodes 4 and 5 have processed their
		// update.
		mtc.waitForValues(rightKey, []int64{0, 0, 0, 2, 25, 25})

		// Wake up the left-hand range. This will allow the left-hand
		// range's split to complete and unblock the right-hand range.
		mtc.restartStore(1)
		mtc.restartStore(2)

		// Perform writes on both sides. This is not strictly necessary but
		// it helps wake up dormant ranges that would otherwise have to wait
		// for retry timeouts.
		incArgs = incrementArgs(leftKey, 10)
		if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(leftKey, []int64{0, 11, 11, 11, 0, 0})

		incArgs = incrementArgs(rightKey, 200)
		if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
			t.Fatal(pErr)
		}
		mtc.waitForValues(rightKey, []int64{0, 0, 0, 225, 225, 225})
	})
}

// TestStoreSplitTimestampCacheDifferentLeaseHolder prevents regression of
// #7899. When the first lease holder of the right-hand side of a Split was
// not equal to the left-hand side lease holder (at the time of the split),
// its timestamp cache would not be properly initialized, which would allow
// for writes which invalidated reads previously served by the pre-split lease.
func TestStoreSplitTimestampCacheDifferentLeaseHolder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("b")
	rightKey := roachpb.Key("c")

	// This filter is better understood when reading the meat of the test
	// below first.
	var noLeaseForDesc atomic.Value
	filter := func(args storagebase.FilterArgs) *roachpb.Error {
		leaseReq, argOK := args.Req.(*roachpb.RequestLeaseRequest)
		forbiddenDesc, descOK := noLeaseForDesc.Load().(*roachpb.ReplicaDescriptor)
		if !argOK || !descOK || !bytes.Equal(leaseReq.Key, splitKey) {
			return nil
		}
		log.Infof(ctx, "received lease request (%s, %s)",
			leaseReq.Span(), leaseReq.Lease)
		if !reflect.DeepEqual(*forbiddenDesc, leaseReq.Lease.Replica) {
			return nil
		}
		log.Infof(ctx,
			"refusing lease request (%s, %s) because %+v held lease for LHS of split",
			leaseReq.Span(), leaseReq.Lease, forbiddenDesc)
		return roachpb.NewError(&roachpb.NotLeaseHolderError{RangeID: args.Hdr.RangeID})
	}

	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		EvalKnobs: storagebase.BatchEvalTestingKnobs{
			TestingEvalFilter: filter,
		},
	}

	tc := testcluster.StartTestCluster(t, 2, args)
	defer tc.Stopper().Stop(context.TODO())

	// Split the data range, mainly to avoid other splits getting in our way.
	for _, k := range []roachpb.Key{leftKey, rightKey} {
		if _, _, err := tc.SplitRange(k); err != nil {
			t.Fatal(errors.Wrapf(err, "split at %s", k))
		}
	}
	if _, err := tc.AddReplicas(leftKey, tc.Target(1)); err != nil {
		t.Fatal(err)
	}

	db := tc.Servers[0].DB() // irrelevant which one we use

	// Make a context tied to the Stopper. The test works without, but this
	// is cleaner since we won't properly terminate the transaction below.
	ctx, cancel := tc.Server(0).Stopper().WithCancelOnQuiesce(ctx)
	defer cancel()

	// This transaction will try to write "under" a served read.
	txnOld := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)

	// Do something with txnOld so that its timestamp gets set.
	if _, err := txnOld.Scan(ctx, "a", "b", 0); err != nil {
		t.Fatal(err)
	}

	// Another client comes along at a higher timestamp, touching everything on
	// the right of the (soon-to-be) split key.
	if _, err := db.Scan(ctx, splitKey, rightKey, 0); err != nil {
		t.Fatal(err)
	}

	// This block makes sure that from now on, we don't allow the current
	// lease holder of our range to extend. Any attempt of doing so will
	// catch a NotLeaseHolderError, which means a retry by DistSender (until
	// the other node gets to be the lease holder instead).
	//
	// This makes sure that once we split, we'll be in the situation described
	// in #7899 (before the fix): The first lease holder of the right hand side
	// of the Split will not be that of the pre-split Range.
	// With the fix, the right-hand lease is initialized from the left-hand
	// lease, so the lease holders are the same, and there will never be a
	// lease request for the right-hand side in this test.
	leaseHolder := func(k roachpb.Key) roachpb.ReplicaDescriptor {
		desc, err := tc.LookupRange(k)
		if err != nil {
			t.Fatal(err)
		}
		lease, _, err := tc.FindRangeLease(desc, nil)
		if err != nil {
			t.Fatal(err)
		}
		leaseHolder := lease.Replica
		replica, found := desc.GetReplicaDescriptor(leaseHolder.StoreID)
		if !found {
			t.Fatalf("no replica on store %d found in %+v", leaseHolder.StoreID, desc)
		}
		return replica
	}
	blacklistedLeaseHolder := leaseHolder(leftKey)
	log.Infof(ctx, "blacklisting replica %+v for leases", blacklistedLeaseHolder)
	noLeaseForDesc.Store(&blacklistedLeaseHolder)

	// Pull the trigger. This actually also reads the RHS descriptor after the
	// split, so when this returns, we've got the leases set up already.
	//
	// There's a slight race here: Just above, we've settled on who must not
	// be the future lease holder. But between then and now, that lease could
	// have expired and the other Replica could have obtained it. This would
	// have given it a proper initialization of the timestamp cache, and so
	// the split trigger would populate the right hand side with a timestamp
	// cache which does not exhibit the anomaly.
	//
	// In practice, this should only be possible if second-long delays occur
	// just above this comment, and we assert against it below.
	log.Infof(ctx, "splitting at %s", splitKey)
	if _, _, err := tc.SplitRange(splitKey); err != nil {
		t.Fatal(err)
	}

	if currentLHSLeaseHolder := leaseHolder(leftKey); !reflect.DeepEqual(
		currentLHSLeaseHolder, blacklistedLeaseHolder) {
		t.Fatalf("lease holder changed from %+v to %+v, should de-flake this test",
			blacklistedLeaseHolder, currentLHSLeaseHolder)
	}

	// This write (to the right-hand side of the split) should hit the
	// timestamp cache and flag the txn for a restart when we try to commit it
	// below. With the bug in #7899, the RHS of the split had an empty
	// timestamp cache and would simply let us write behind the previous read.
	if err := txnOld.Put(ctx, "bb", "bump"); err != nil {
		t.Fatal(err)
	}

	if err := txnOld.Commit(ctx); err != nil {
		t.Fatalf("unexpected txn commit err: %+v", err)
	}

	// Verify that the txn's safe timestamp was set.
	if txnOld.TestingCloneTxn().ReadTimestamp == (hlc.Timestamp{}) {
		t.Fatal("expected non-zero refreshed timestamp")
	}

	// As outlined above, the anomaly was fixed by giving the right-hand side
	// of the split the same lease as the left-hand side of the Split. Check
	// that that's what's happened (we actually test a little more, namely
	// that it's the same ReplicaID, which is not required but should always
	// hold).
	if rhsLease := leaseHolder(rightKey); !reflect.DeepEqual(
		rhsLease, blacklistedLeaseHolder,
	) {
		t.Errorf("expected LHS and RHS to have same lease holder")
	}
}

// TestStoreSplitOnRemovedReplica prevents regression of #23673. In that issue,
// it was observed that the retry loop in AdminSplit could go into an infinite
// loop if the replica it was being run on had been removed from the range. The
// loop now checks that the replica performing the split is the leaseholder
// before each iteration.
func TestStoreSplitOnRemovedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("b")
	rightKey := roachpb.Key("c")

	var newDesc roachpb.RangeDescriptor
	inFilter := make(chan struct{}, 1)
	beginBlockingSplit := make(chan struct{})
	finishBlockingSplit := make(chan struct{})
	filter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
		// Block replica 1's attempt to perform the AdminSplit. We detect the
		// split's range descriptor update and block until the rest of the test
		// is ready. We then return a ConditionFailedError, simulating a
		// descriptor update race.
		if ba.Replica.NodeID == 1 {
			for _, req := range ba.Requests {
				if cput, ok := req.GetInner().(*roachpb.ConditionalPutRequest); ok {
					leftDescKey := keys.RangeDescriptorKey(roachpb.RKey(leftKey))
					if cput.Key.Equal(leftDescKey) {
						var desc roachpb.RangeDescriptor
						if err := cput.Value.GetProto(&desc); err != nil {
							panic(err)
						}

						if desc.EndKey.Equal(splitKey) {
							select {
							case <-beginBlockingSplit:
								select {
								case inFilter <- struct{}{}:
									// Let the test know we're in the filter.
								default:
								}
								<-finishBlockingSplit

								var val roachpb.Value
								if err := val.SetProto(&newDesc); err != nil {
									panic(err)
								}
								return roachpb.NewError(&roachpb.ConditionFailedError{
									ActualValue: &val,
								})
							default:
							}
						}
					}
				}
			}
		}
		return nil
	}

	var args base.TestClusterArgs
	args.ReplicationMode = base.ReplicationManual
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filter,
	}

	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(context.TODO())

	// Split the data range, mainly to avoid other splits getting in our way.
	for _, k := range []roachpb.Key{leftKey, rightKey} {
		if _, _, err := tc.SplitRange(k); err != nil {
			t.Fatal(errors.Wrapf(err, "split at %s", k))
		}
	}

	// Send an AdminSplit request to the replica. In the filter above we'll
	// block the first cput in this split until we're ready to let it loose
	// again, which will be after we remove the replica from the range.
	splitRes := make(chan error)
	close(beginBlockingSplit)
	go func() {
		_, _, err := tc.SplitRange(splitKey)
		splitRes <- err
	}()
	<-inFilter

	// Move the range from node 0 to node 1. Then add node 2 to the range.
	// node 0 will never hear about this range descriptor update.
	var err error
	if newDesc, err = tc.AddReplicas(leftKey, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	if err := tc.TransferRangeLease(newDesc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.RemoveReplicas(leftKey, tc.Target(0)); err != nil {
		t.Fatal(err)
	}
	if newDesc, err = tc.AddReplicas(leftKey, tc.Target(2)); err != nil {
		t.Fatal(err)
	}

	// Stop blocking the split request's cput. This will cause the cput to fail
	// with a ConditionFailedError. The error will warrant a retry in
	// AdminSplit's retry loop, but when the removed replica notices that it is
	// no longer the leaseholder, it will return a NotLeaseholderError. This in
	// turn will allow the AdminSplit to be re-routed to the new leaseholder,
	// where it will succeed.
	close(finishBlockingSplit)
	if err = <-splitRes; err != nil {
		t.Errorf("AdminSplit returned error: %+v", err)
	}
}

func TestStoreSplitGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("b")
	rightKey := roachpb.Key("c")
	content := []byte("test")

	pArgs := putArgs(leftKey, content)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs = putArgs(rightKey, content)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	specifiedGCThreshold := hlc.Timestamp{
		WallTime: 2e9,
	}
	gcArgs := &roachpb.GCRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    leftKey,
			EndKey: rightKey,
		},
		Threshold: specifiedGCThreshold,
	}
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), gcArgs); pErr != nil {
		t.Fatal(pErr)
	}

	args := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	repl := store.LookupReplica(roachpb.RKey(splitKey))
	gcThreshold := repl.GetGCThreshold()

	if !reflect.DeepEqual(gcThreshold, specifiedGCThreshold) {
		t.Fatalf("expected RHS's GCThreshold is equal to %v, but got %v", specifiedGCThreshold, gcThreshold)
	}

	repl.AssertState(context.TODO(), store.Engine())
}

// TestStoreRangeSplitRaceUninitializedRHS reproduces #7600 (before it was
// fixed). While splits are happening, we simulate incoming messages for the
// right-hand side to trigger a race between the creation of the proper replica
// and the uninitialized replica reacting to messages.
func TestStoreRangeSplitRaceUninitializedRHS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	// An aggressive tick interval lets groups communicate more and thus
	// triggers test failures much more reliably. We can't go too aggressive
	// or race tests never make any progress.
	storeCfg.RaftTickInterval = 50 * time.Millisecond
	storeCfg.RaftElectionTimeoutTicks = 2
	currentTrigger := make(chan *roachpb.SplitTrigger, 1)
	var seen struct {
		syncutil.Mutex
		sids map[storagebase.CmdIDKey][2]bool
	}
	seen.sids = make(map[storagebase.CmdIDKey][2]bool)

	storeCfg.TestingKnobs.EvalKnobs.TestingEvalFilter = func(args storagebase.FilterArgs) *roachpb.Error {
		et, ok := args.Req.(*roachpb.EndTxnRequest)
		if !ok || et.InternalCommitTrigger == nil {
			return nil
		}
		trigger := protoutil.Clone(et.InternalCommitTrigger.GetSplitTrigger()).(*roachpb.SplitTrigger)
		// The first time the trigger arrives (on each of the two stores),
		// return a transaction retry. This allows us to pass the trigger to
		// the goroutine creating faux incoming messages for the yet
		// nonexistent right-hand-side, giving it a head start. This code looks
		// fairly complicated since it wants to ensure that the two replicas
		// don't diverge.
		if trigger != nil && len(trigger.RightDesc.InternalReplicas) == 2 && args.Hdr.Txn.Epoch == 0 {
			seen.Lock()
			defer seen.Unlock()
			sid, sl := int(args.Sid)-1, seen.sids[args.CmdID]
			if !sl[sid] {
				sl[sid] = true
				seen.sids[args.CmdID] = sl
			} else {
				return nil
			}
			select {
			case currentTrigger <- trigger:
			default:
			}
			return roachpb.NewError(
				roachpb.NewReadWithinUncertaintyIntervalError(
					args.Hdr.Timestamp, args.Hdr.Timestamp, nil,
				))
		}
		return nil
	}

	mtc.storeConfig = &storeCfg
	defer mtc.Stop()
	mtc.Start(t, 2)

	leftRange := mtc.stores[0].LookupReplica(roachpb.RKey("a"))

	// Replicate the left range onto the second node. We don't wait since we
	// don't actually care what the second node does. All we want is that the
	// first node isn't surprised by messages from that node.
	mtc.replicateRange(leftRange.RangeID, 1)

	for i := 0; i < 10; i++ {
		errChan := make(chan *roachpb.Error)

		// Closed when the split goroutine is done.
		splitDone := make(chan struct{})

		go func() {
			defer close(splitDone)

			// Split the data range. The split keys are chosen so that they move
			// towards "a" (so that the range being split is always the first
			// range).
			splitKey := roachpb.Key(encoding.EncodeVarintDescending([]byte("a"), int64(i)))
			splitArgs := adminSplitArgs(splitKey)
			_, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs)
			errChan <- pErr
		}()
		go func() {
			defer func() { errChan <- nil }()

			trigger := <-currentTrigger // our own copy
			// Make sure the first node is first for convenience.
			replicas := trigger.RightDesc.InternalReplicas
			if replicas[0].NodeID > replicas[1].NodeID {
				tmp := replicas[1]
				replicas[1] = replicas[0]
				replicas[0] = tmp
			}

			// Send a few vote requests which look like they're from the other
			// node's right hand side of the split. This triggers a race which
			// is discussed in #7600 (briefly, the creation of the right hand
			// side in the split trigger was racing with the uninitialized
			// version for the same group, resulting in clobbered HardState).
			for term := uint64(1); ; term++ {
				if sent := mtc.transport.SendAsync(&kvserver.RaftMessageRequest{
					RangeID:     trigger.RightDesc.RangeID,
					ToReplica:   replicas[0],
					FromReplica: replicas[1],
					Message: raftpb.Message{
						Type: raftpb.MsgVote,
						To:   uint64(replicas[0].ReplicaID),
						From: uint64(replicas[1].ReplicaID),
						Term: term,
					},
				}, rpc.DefaultClass); !sent {
					t.Error("transport failed to send vote request")
				}
				select {
				case <-splitDone:
					return
				case <-time.After(time.Microsecond):
					// If we busy-loop here, we monopolize processRaftMu and the
					// split takes a long time to complete. Sleeping reduces the
					// chance that we hit the race, but it still shows up under
					// stress.
				}
			}
		}()
		for i := 0; i < 2; i++ {
			if pErr := <-errChan; pErr != nil {
				t.Fatal(pErr)
			}
		}
	}
}

// TestLeaderAfterSplit verifies that a raft group created by a split
// elects a leader without waiting for an election timeout.
func TestLeaderAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeConfig := kvserver.TestStoreConfig(nil)
	storeConfig.TestingKnobs.DisableMergeQueue = true
	storeConfig.RaftElectionTimeoutTicks = 1000000
	mtc := &multiTestContext{
		storeConfig: &storeConfig,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	mtc.replicateRange(1, 1, 2)

	leftKey := roachpb.Key("a")
	splitKey := roachpb.Key("m")
	rightKey := roachpb.Key("z")

	splitArgs := adminSplitArgs(splitKey)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], splitArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	incArgs = incrementArgs(rightKey, 2)
	if _, pErr := kv.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

func BenchmarkStoreRangeSplit(b *testing.B) {
	var mtc multiTestContext
	mtc.Start(b, 1)
	defer mtc.Stop()
	store := mtc.Store(0)

	// Perform initial split of ranges.
	sArgs := adminSplitArgs(roachpb.Key("b"))
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), sArgs); err != nil {
		b.Fatal(err)
	}

	// Write some values left and right of the split key.
	aDesc := store.LookupReplica([]byte("a")).Desc()
	bDesc := store.LookupReplica([]byte("c")).Desc()
	kvserver.WriteRandomDataToRange(b, store, aDesc.RangeID, []byte("aaa"))
	kvserver.WriteRandomDataToRange(b, store, bDesc.RangeID, []byte("ccc"))

	// Merge the b range back into the a range.
	mArgs := adminMergeArgs(roachpb.KeyMin)
	if _, err := kv.SendWrapped(context.Background(), store.TestSender(), mArgs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Split the range.
		b.StartTimer()
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), sArgs); err != nil {
			b.Fatal(err)
		}

		// Merge the ranges.
		b.StopTimer()
		if _, err := kv.SendWrapped(context.Background(), store.TestSender(), mArgs); err != nil {
			b.Fatal(err)
		}
	}
}

func writeRandomTimeSeriesDataToRange(
	t testing.TB, store *kvserver.Store, rangeID roachpb.RangeID, keyPrefix []byte,
) (midpoint []byte) {
	src := rand.New(rand.NewSource(0))
	r := ts.Resolution10s
	for i := 0; i < 20; i++ {
		var data []tspb.TimeSeriesData
		for j := int64(0); j <= src.Int63n(5); j++ {
			d := tspb.TimeSeriesData{
				Name:   "test.random.metric",
				Source: "cpu01",
			}
			for k := int64(0); k <= src.Int63n(10); k++ {
				d.Datapoints = append(d.Datapoints, tspb.TimeSeriesDatapoint{
					TimestampNanos: src.Int63n(200) * r.SlabDuration(),
					Value:          src.Float64(),
				})
			}
			data = append(data, d)
		}
		for _, d := range data {
			idatas, err := d.ToInternal(r.SlabDuration(), r.SampleDuration(), false)
			if err != nil {
				t.Fatal(err)
			}
			for _, idata := range idatas {
				var value roachpb.Value
				if err := value.SetProto(&idata); err != nil {
					t.Fatal(err)
				}
				mArgs := roachpb.MergeRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: encoding.EncodeVarintAscending(keyPrefix, idata.StartTimestampNanos),
					},
					Value: value,
				}
				if _, pErr := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{
					RangeID: rangeID,
				}, &mArgs); pErr != nil {
					t.Fatal(pErr)
				}
			}
		}
	}
	// Return approximate midway point (100 is midway between random timestamps in range [0,200)).
	midKey := append([]byte(nil), keyPrefix...)
	midKey = encoding.EncodeVarintAscending(midKey, 100*r.SlabDuration())
	return midKey
}

// TestStoreRangeGossipOnSplits verifies that the store descriptor
// is gossiped on splits up until the point where an additional
// split range doesn't exceed GossipWhenCapacityDeltaExceedsFraction.
func TestStoreRangeGossipOnSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.GossipWhenCapacityDeltaExceedsFraction = 0.5 // 50% for testing
	// We can't properly test how frequently changes in the number of ranges
	// trigger the store to gossip its capacities if we have to worry about
	// changes in the number of leases also triggering store gossip.
	storeCfg.TestingKnobs.DisableLeaseCapacityGossip = true
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	storeCfg.TestingKnobs.DisableScanner = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	storeKey := gossip.MakeStoreKey(store.StoreID())

	// Avoid excessive logging on under-replicated ranges due to our many splits.
	config.TestingSetupZoneConfigHook(stopper)
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(1)
	config.TestingSetZoneConfig(0, zoneConfig)

	var lastSD roachpb.StoreDescriptor
	rangeCountCh := make(chan int32)
	unregister := store.Gossip().RegisterCallback(storeKey, func(_ string, val roachpb.Value) {
		var sd roachpb.StoreDescriptor
		if err := val.GetProto(&sd); err != nil {
			panic(err)
		}
		// Wait for range count to change as this callback is invoked
		// for lease count changes as well.
		if sd.Capacity.RangeCount == lastSD.Capacity.RangeCount {
			return
		}
		lastSD = sd
		rangeCountCh <- sd.Capacity.RangeCount
	})
	defer unregister()

	// Pull the first gossiped range count.
	lastRangeCount := <-rangeCountCh

	splitFunc := func(i int) *roachpb.Error {
		splitKey := roachpb.Key(fmt.Sprintf("%02d", i))
		_, pErr := store.LookupReplica(roachpb.RKey(splitKey)).AdminSplit(
			context.TODO(),
			roachpb.AdminSplitRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: splitKey,
				},
				SplitKey: splitKey,
			},
			"test",
		)
		return pErr
	}

	// Split until we split at least 20 ranges.
	var rangeCount int32
	for i := 0; rangeCount < 20; i++ {
		if pErr := splitFunc(i); pErr != nil {
			// Avoid flakes caused by bad clocks.
			if testutils.IsPError(pErr, "rejecting command with timestamp in the future") {
				log.Warningf(context.TODO(), "ignoring split error: %s", pErr)
				continue
			}
			t.Fatal(pErr)
		}
		select {
		case rangeCount = <-rangeCountCh:
			changeCount := int32(math.Ceil(math.Min(float64(lastRangeCount)*0.5, 3)))
			diff := rangeCount - (lastRangeCount + changeCount)
			if diff < -1 || diff > 1 {
				t.Errorf("gossiped range count %d more than 1 away from expected %d", rangeCount, lastRangeCount+changeCount)
			}
			lastRangeCount = rangeCount
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestStoreTxnWaitQueueEnabledOnSplit verifies that the TxnWaitQueue for
// the right hand side of the split range is enabled after a split.
func TestStoreTxnWaitQueueEnabledOnSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	key := keys.UserTableDataMin
	args := adminSplitArgs(key)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatalf("%q: split unexpected error: %s", key, pErr)
	}

	rhsRepl := store.LookupReplica(roachpb.RKey(keys.UserTableDataMin))
	if !rhsRepl.GetConcurrencyManager().TxnWaitQueue().IsEnabled() {
		t.Errorf("expected RHS replica's push txn queue to be enabled post-split")
	}
}

// TestDistributedTxnCleanup verifies that distributed transactions
// cleanup their txn records after commit or abort.
func TestDistributedTxnCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Split at "a".
	lhsKey := roachpb.Key("a")
	args := adminSplitArgs(lhsKey)
	if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
		t.Fatalf("split at %q: %s", lhsKey, pErr)
	}
	lhs := store.LookupReplica(roachpb.RKey("a"))

	// Split at "b".
	rhsKey := roachpb.Key("b")
	args = adminSplitArgs(rhsKey)
	if _, pErr := kv.SendWrappedWith(context.Background(), store, roachpb.Header{
		RangeID: lhs.RangeID,
	}, args); pErr != nil {
		t.Fatalf("split at %q: %s", rhsKey, pErr)
	}
	rhs := store.LookupReplica(roachpb.RKey("b"))

	if lhs == rhs {
		t.Errorf("LHS == RHS after split: %s == %s", lhs, rhs)
	}

	// Test both commit and abort cases.
	testutils.RunTrueAndFalse(t, "force", func(t *testing.T, force bool) {
		testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
			// Run a distributed transaction involving the lhsKey and rhsKey.
			var txnKey roachpb.Key
			ctx := context.Background()
			txn := kv.NewTxn(ctx, store.DB(), 0 /* gatewayNodeID */)
			txnFn := func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				b.Put(fmt.Sprintf("%s.force=%t,commit=%t", string(lhsKey), force, commit), "lhsValue")
				b.Put(fmt.Sprintf("%s.force=%t,commit=%t", string(rhsKey), force, commit), "rhsValue")
				if err := txn.Run(ctx, b); err != nil {
					return err
				}
				proto := txn.TestingCloneTxn()
				txnKey = keys.TransactionKey(proto.Key, proto.ID)
				// If force=true, we're force-aborting the txn out from underneath.
				// This simulates txn deadlock or a max priority txn aborting a
				// normal or min priority txn.
				if force {
					ba := roachpb.BatchRequest{}
					ba.Timestamp = store.Clock().Now()
					ba.RangeID = lhs.RangeID
					ba.Add(&roachpb.PushTxnRequest{
						RequestHeader: roachpb.RequestHeader{
							Key: proto.Key,
						},
						PusheeTxn: proto.TxnMeta,
						PushType:  roachpb.PUSH_ABORT,
						Force:     true,
					})
					_, pErr := store.Send(ctx, ba)
					if pErr != nil {
						t.Fatalf("failed to abort the txn: %s", pErr)
					}
				}
				if commit {
					return txn.Commit(ctx)
				}
				return errors.New("forced abort")
			}
			if err := txnFn(ctx, txn); err != nil {
				txn.CleanupOnError(ctx, err)
				if !force && commit {
					t.Fatalf("expected success with commit == true; got %v", err)
				}
			}

			// Verify that the transaction record is cleaned up.
			testutils.SucceedsSoon(t, func() error {
				kv, err := store.DB().Get(ctx, txnKey)
				if err != nil {
					return err
				}
				if kv.Value != nil {
					return errors.Errorf("expected txn record %s to have been cleaned", txnKey)
				}
				return nil
			})
		})
	})
}

// TestUnsplittableRange creates an unsplittable range and tests that
// it is handled correctly by the split queue's purgatory. The test:
// 1. creates an unsplittable range that needs to be split
// 2. makes sure that range enters purgatory
// 3. makes sure a purgatory run still fails
// 4. GCs part of the range so that it no longer needs to be split
// 5. makes sure a purgatory run succeeds and the range leaves purgatory
func TestUnsplittableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ttl := 1 * time.Hour
	const maxBytes = 1 << 16

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	manual := hlc.NewManualClock(123)
	splitQueuePurgatoryChan := make(chan time.Time, 1)
	cfg := kvserver.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	cfg.DefaultZoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
	cfg.DefaultZoneConfig.GC = &zonepb.GCPolicy{
		TTLSeconds: int32(ttl.Seconds()),
	}
	cfg.DefaultSystemZoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
	cfg.DefaultSystemZoneConfig.GC = &zonepb.GCPolicy{
		TTLSeconds: int32(ttl.Seconds()),
	}
	cfg.TestingKnobs.SplitQueuePurgatoryChan = splitQueuePurgatoryChan
	cfg.TestingKnobs.DisableMergeQueue = true
	store := createTestStoreWithConfig(t, stopper, cfg)

	// Add a single large row to /Table/14.
	tableKey := keys.MakeTablePrefix(keys.UITableID)
	row1Key := roachpb.Key(encoding.EncodeVarintAscending(append([]byte(nil), tableKey...), 1))
	col1Key := keys.MakeFamilyKey(append([]byte(nil), row1Key...), 0)
	valueLen := 0.9 * maxBytes
	value := bytes.Repeat([]byte("x"), int(valueLen))
	if err := store.DB().Put(ctx, col1Key, value); err != nil {
		t.Fatal(err)
	}

	// Wait for half of the ttl and add another large value in the same row.
	// Together, these two values bump the range over the max range size.
	manual.Increment(ttl.Nanoseconds() / 2)
	value2Len := 0.2 * maxBytes
	value2 := bytes.Repeat([]byte("y"), int(value2Len))
	if err := store.DB().Put(ctx, col1Key, value2); err != nil {
		t.Fatal(err)
	}

	// Ensure that an attempt to split the range will hit an
	// unsplittableRangeError and place the range in purgatory.
	if err := store.ForceSplitScanAndProcess(); err != nil {
		t.Fatal(err)
	}
	if purgLen := store.SplitQueuePurgatoryLength(); purgLen != 1 {
		t.Fatalf("expected split queue purgatory to contain 1 replica, found %d", purgLen)
	}

	// Signal the split queue's purgatory channel and ensure that the purgatory
	// remains occupied because the range still needs to split but can't.
	splitQueuePurgatoryChan <- timeutil.Now()
	if purgLen := store.SplitQueuePurgatoryLength(); purgLen != 1 {
		t.Fatalf("expected split queue purgatory to contain 1 replica, found %d", purgLen)
	}

	// Wait for much longer than the ttl to accumulate GCByteAge.
	manual.Increment(10 * ttl.Nanoseconds())
	// Trigger the GC queue, which should clean up the earlier version of the
	// row. Once the first version of the row is cleaned up, the range should
	// exit the split queue purgatory.
	repl := store.LookupReplica(tableKey)
	if err := store.ManualGC(repl); err != nil {
		t.Fatal(err)
	}

	// Signal the split queue's purgatory channel and ensure that the purgatory
	// removes its now well-sized replica.
	splitQueuePurgatoryChan <- timeutil.Now()
	testutils.SucceedsSoon(t, func() error {
		purgLen := store.SplitQueuePurgatoryLength()
		if purgLen == 0 {
			return nil
		}
		return errors.Errorf("expected split queue purgatory to be empty, found %d", purgLen)
	})
}

// TestTxnWaitQueueDependencyCycleWithRangeSplit verifies that a range
// split which occurs while a dependency cycle is partially underway
// will cause the pending push txns to be retried such that they
// relocate to the appropriate new range.
func TestTxnWaitQueueDependencyCycleWithRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "read2ndPass", func(t *testing.T, read2ndPass bool) {
		var pushCount int32
		firstPush := make(chan struct{})

		storeCfg := kvserver.TestStoreConfig(nil)
		storeCfg.TestingKnobs.DisableSplitQueue = true
		storeCfg.TestingKnobs.DisableMergeQueue = true
		storeCfg.TestingKnobs.EvalKnobs.TestingEvalFilter =
			func(filterArgs storagebase.FilterArgs) *roachpb.Error {
				if _, ok := filterArgs.Req.(*roachpb.PushTxnRequest); ok {
					if atomic.AddInt32(&pushCount, 1) == 1 {
						close(firstPush)
					}
				}
				return nil
			}
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		store := createTestStoreWithConfig(t, stopper, storeCfg)

		lhsKey := roachpb.Key("a")
		rhsKey := roachpb.Key("b")

		// Split at "a".
		args := adminSplitArgs(lhsKey)
		if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
			t.Fatalf("split at %q: %s", lhsKey, pErr)
		}
		lhs := store.LookupReplica(roachpb.RKey("a"))

		var txnACount, txnBCount int32

		txnAWritesA := make(chan struct{})
		txnAProceeds := make(chan struct{})
		txnBWritesB := make(chan struct{})
		txnBProceeds := make(chan struct{})

		// Start txn to write key a.
		txnACh := make(chan error)
		go func() {
			txnACh <- store.DB().Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, lhsKey, "value"); err != nil {
					return err
				}
				if atomic.LoadInt32(&txnACount) == 0 {
					close(txnAWritesA)
					<-txnAProceeds
				}
				atomic.AddInt32(&txnACount, 1)
				return txn.Put(ctx, rhsKey, "value-from-A")
			})
		}()
		<-txnAWritesA

		// Start txn to write key b.
		txnBCh := make(chan error)
		go func() {
			txnBCh <- store.DB().Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
				if err := txn.Put(ctx, rhsKey, "value"); err != nil {
					return err
				}
				if atomic.LoadInt32(&txnBCount) == 0 {
					close(txnBWritesB)
					<-txnBProceeds
				}
				atomic.AddInt32(&txnBCount, 1)
				// Read instead of write key "a" if directed. This caused a
				// PUSH_TIMESTAMP to be issued from txn B instead of PUSH_ABORT.
				if read2ndPass {
					if _, err := txn.Get(ctx, lhsKey); err != nil {
						return err
					}
				} else {
					if err := txn.Put(ctx, lhsKey, "value-from-B"); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		<-txnBWritesB

		// Now, let txnA proceed before splitting.
		close(txnAProceeds)
		// Wait for the push to occur.
		<-firstPush

		// Split at "b".
		args = adminSplitArgs(rhsKey)
		if _, pErr := kv.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: lhs.RangeID,
		}, args); pErr != nil {
			t.Fatalf("split at %q: %s", rhsKey, pErr)
		}

		// Now that we've split, allow txnB to proceed.
		close(txnBProceeds)

		// Verify that both complete.
		for i, ch := range []chan error{txnACh, txnBCh} {
			if err := <-ch; err != nil {
				t.Fatalf("%d: txn failure: %+v", i, err)
			}
		}
	})
}

func TestStoreCapacityAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	manualClock := hlc.NewManualClock(123)
	cfg := kvserver.TestStoreConfig(hlc.NewClock(manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.DisableMergeQueue = true
	s := createTestStoreWithOpts(
		t,
		testStoreOpts{
			// This test was written before the test stores were able to start with
			// more than one range and is not prepared to handle many ranges.
			dontCreateSystemRanges: true,
			cfg:                    &cfg},
		stopper)

	cap, err := s.Capacity(false /* useCached */)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := int32(1), cap.RangeCount; e != a {
		t.Errorf("expected cap.RangeCount=%d, got %d", e, a)
	}
	bpr1 := cap.BytesPerReplica
	if bpr1.P10 <= 0 {
		t.Errorf("expected all bytes-per-replica to be positive, got %+v", bpr1)
	}
	if bpr1.P10 != bpr1.P25 || bpr1.P10 != bpr1.P50 || bpr1.P10 != bpr1.P75 || bpr1.P10 != bpr1.P90 {
		t.Errorf("expected all bytes-per-replica percentiles to be identical, got %+v", bpr1)
	}
	wpr1 := cap.WritesPerReplica
	if wpr1.P10 != wpr1.P25 || wpr1.P10 != wpr1.P50 || wpr1.P10 != wpr1.P75 || wpr1.P10 != wpr1.P90 {
		t.Errorf("expected all writes-per-replica percentiles to be identical, got %+v", wpr1)
	}

	// Increment the manual clock and do a write to increase the qps above zero.
	manualClock.Increment(int64(kvserver.MinStatsDuration))
	key := roachpb.Key("a")
	pArgs := putArgs(key, []byte("aaa"))
	if _, pErr := kv.SendWrapped(context.Background(), s.TestSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	cap, err = s.Capacity(false /* useCached */)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := int32(1), cap.RangeCount; e != a {
		t.Errorf("expected cap.RangeCount=%d, got %d", e, a)
	}
	if e, a := int32(1), cap.LeaseCount; e != a {
		t.Errorf("expected cap.LeaseCount=%d, got %d", e, a)
	}
	if minExpected, a := 1/float64(kvserver.MinStatsDuration/time.Second), cap.WritesPerSecond; minExpected > a {
		t.Errorf("expected cap.WritesPerSecond >= %f, got %f", minExpected, a)
	}
	bpr2 := cap.BytesPerReplica
	if bpr2.P10 <= bpr1.P10 {
		t.Errorf("expected BytesPerReplica to have increased from %+v, but got %+v", bpr1, bpr2)
	}
	if bpr2.P10 != bpr2.P25 || bpr2.P10 != bpr2.P50 || bpr2.P10 != bpr2.P75 || bpr2.P10 != bpr2.P90 {
		t.Errorf("expected all bytes-per-replica percentiles to be identical, got %+v", bpr2)
	}
	wpr2 := cap.WritesPerReplica
	if wpr2.P10 <= wpr1.P10 {
		t.Errorf("expected WritesPerReplica to have increased from %+v, but got %+v", wpr1, wpr2)
	}
	if wpr2.P10 != wpr2.P25 || wpr2.P10 != wpr2.P50 || wpr2.P10 != wpr2.P75 || wpr2.P10 != wpr2.P90 {
		t.Errorf("expected all writes-per-replica percentiles to be identical, got %+v", wpr2)
	}
	if wpr2.P10 != cap.WritesPerSecond {
		t.Errorf("expected WritesPerReplica.percentiles to equal cap.WritesPerSecond, but got %f and %f",
			wpr2.P10, cap.WritesPerSecond)
	}

	// Split the range to verify stats work properly with more than one range.
	sArgs := adminSplitArgs(key)
	if _, pErr := kv.SendWrapped(context.Background(), s.TestSender(), sArgs); pErr != nil {
		t.Fatal(pErr)
	}

	cap, err = s.Capacity(false /* useCached */)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := int32(2), cap.RangeCount; e != a {
		t.Errorf("expected cap.RangeCount=%d, got %d", e, a)
	}
	if e, a := int32(2), cap.LeaseCount; e != a {
		t.Errorf("expected cap.LeaseCount=%d, got %d", e, a)
	}
	{
		bpr := cap.BytesPerReplica
		if bpr.P10 != bpr.P25 {
			t.Errorf("expected BytesPerReplica p10 and p25 to be equal with 2 replicas, got %+v", bpr)
		}
		if bpr.P50 != bpr.P75 || bpr.P50 != bpr.P90 {
			t.Errorf("expected BytesPerReplica p50, p75, and p90 to be equal with 2 replicas, got %+v", bpr)
		}
		if bpr.P10 == bpr.P90 {
			t.Errorf("expected BytesPerReplica p10 and p90 to be different with 2 replicas, got %+v", bpr)
		}
	}
}

// TestRangeLookupAfterMeta2Split verifies that RangeLookup scans succeed even
// when user ranges span the boundary of two split meta2 ranges. We test this
// with forward and reverse ScanRequests so that we test both forward and
// reverse RangeLookups. In the case of both the RangeLookup scan directions,
// the forward part of the scan will need to continue onto a second range to
// find the desired RangeDescriptor (remember that a reverse RangeLookup
// includes an initial forward scan).
func TestRangeLookupAfterMeta2Split(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
			},
		},
	})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	// Create a split at /Table/48 and /Meta2/Table/51. This creates:
	//   meta ranges [/Min-/Meta2/Table/51) and [/Meta2/Table/51-/System)
	//   user ranges [/Table/19-/Table/48)  and [/Table/48-/Max)
	//
	// Note that the two boundaries are offset such that a lookup for key /Table/49
	// will first search for meta(/Table/49) which is on the left meta2 range. However,
	// the user range [/Table/48-/Max) is stored on the right meta2 range, so the lookup
	// will require a scan that continues into the next meta2 range.
	const tableID = keys.MinUserDescID + 1 // 51
	splitReq := adminSplitArgs(keys.MakeTablePrefix(tableID - 3 /* 48 */))
	if _, pErr := kv.SendWrapped(ctx, s.DB().NonTransactionalSender(), splitReq); pErr != nil {
		t.Fatal(pErr)
	}

	metaKey := keys.RangeMetaKey(keys.MakeTablePrefix(tableID)).AsRawKey()
	splitReq = adminSplitArgs(metaKey)
	if _, pErr := kv.SendWrapped(ctx, s.DB().NonTransactionalSender(), splitReq); pErr != nil {
		t.Fatal(pErr)
	}

	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, rev bool) {
		// Clear the RangeDescriptorCache so that no cached descriptors are
		// available from previous lookups.
		s.DistSender().RangeDescriptorCache().Clear()

		// Scan from [/Table/49-/Table/50) both forwards and backwards.
		// Either way, the resulting RangeLookup scan will be forced to
		// perform a continuation lookup.
		scanStart := roachpb.Key(keys.MakeTablePrefix(tableID - 2)) // 49
		scanEnd := scanStart.PrefixEnd()                            // 50
		header := roachpb.RequestHeader{
			Key:    scanStart,
			EndKey: scanEnd,
		}

		var lookupReq roachpb.Request
		if rev {
			// A ReverseScanRequest will trigger a reverse RangeLookup scan.
			lookupReq = &roachpb.ReverseScanRequest{RequestHeader: header}
		} else {
			lookupReq = &roachpb.ScanRequest{RequestHeader: header}
		}
		if _, err := kv.SendWrapped(ctx, s.DB().NonTransactionalSender(), lookupReq); err != nil {
			t.Fatalf("%T %v", err.GoError(), err)
		}
	})
}

// TestStoreSplitRangeLookupRace verifies that a RangeLookup scanning across
// multiple meta2 ranges that races with a split and misses all matching
// descriptors will retry its scan until it succeeds.
//
// This test creates a series of events that result in the injected range
// lookup scan response we see in TestRangeLookupRaceSplits/MissingDescriptor.
// It demonstrates how it is possible for an inconsistent range lookup scan
// that spans multiple ranges to completely miss its desired descriptor.
func TestStoreSplitRangeLookupRace(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The scenario is modeled after:
	// https://gitee.com/kwbasedb/kwbase/issues/19147#issuecomment-336741791
	// See that comment for a description of why a non-transactional scan
	// starting at "/meta2/k" may only see non-matching descriptors when racing
	// with a split.
	//
	// To simulate this situation, we first perform splits at "/meta2/n", "j",
	// and "p". This creates the following structure, where the descriptor for
	// range [j, p) is stored on the second meta2 range:
	//
	//   [/meta2/a,/meta2/n), [/meta2/n,/meta2/z)
	//                     -----^
	//       ...      [j, p)      ...
	//
	// We then initiate a range lookup for key "k". This lookup will begin
	// scanning on the first meta2 range but won't find its desired desriptor. Normally,
	// it would continue scanning onto the second meta2 range and find the descriptor
	// for range [j, p) at "/meta2/p" (see TestRangeLookupAfterMeta2Split). However,
	// because RangeLookup scans are non-transactional, this can race with a split.
	// Here, we split at key "m", which creates the structure:
	//
	//   [/meta2/a,/meta2/n), [/meta2/n,/meta2/z)
	//             ^--        ---^
	//       ...   [j,m), [m,p)      ...
	//
	// If the second half of the RangeLookup scan sees the second meta2 range after
	// this split, it will miss the old descriptor for [j, p) and the new descriptor
	// for [j, m). In this case, the RangeLookup should retry.
	lookupKey := roachpb.Key("k")
	bounds, err := keys.MetaScanBounds(keys.RangeMetaKey(roachpb.RKey(lookupKey)))
	if err != nil {
		t.Fatal(err)
	}

	// The following filter and set of channels is used to block the RangeLookup
	// scan for key "k" after it has scanned over the first meta2 range but not
	// the second.
	blockRangeLookups := make(chan struct{})
	blockedRangeLookups := int32(0)
	rangeLookupIsBlocked := make(chan struct{}, 1)
	unblockRangeLookups := make(chan struct{})
	respFilter := func(ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
		select {
		case <-blockRangeLookups:
			if kv.TestingIsRangeLookup(ba) &&
				ba.Requests[0].GetInner().(*roachpb.ScanRequest).Key.Equal(bounds.Key.AsRawKey()) {

				select {
				case rangeLookupIsBlocked <- struct{}{}:
					atomic.AddInt32(&blockedRangeLookups, 1)
				default:
				}
				<-unblockRangeLookups
			}
		default:
		}
		return nil
	}

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue:     true,
				DisableMergeQueue:     true,
				TestingResponseFilter: respFilter,
				IntentResolverKnobs: storagebase.IntentResolverTestingKnobs{
					ForceSyncIntentResolution: true,
				},
			},
		},
	})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(context.Background())
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}

	mustSplit := func(splitKey roachpb.Key) {
		args := adminSplitArgs(splitKey)

		// Don't use s.DistSender() so that we don't disturb the RangeDescriptorCache.
		rangeID := store.LookupReplica(roachpb.RKey(splitKey)).RangeID
		_, pErr := kv.SendWrappedWith(context.Background(), store, roachpb.Header{
			RangeID: rangeID,
		}, args)
		if pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Perform the initial splits. See above.
	mustSplit(keys.SystemPrefix)
	mustSplit(keys.RangeMetaKey(roachpb.RKey("n")).AsRawKey())
	mustSplit(roachpb.Key("j"))
	mustSplit(roachpb.Key("p"))

	// Launch a goroutine to perform a range lookup for key "k" that will race
	// with a split at key "m".
	rangeLookupErr := make(chan error)
	go func() {
		close(blockRangeLookups)

		// Loop until at-least one range lookup is triggered and blocked.
		// This accommodates for races with in-flight range lookups.
		var err error
		for atomic.LoadInt32(&blockedRangeLookups) == 0 && err == nil {
			// Clear the RangeDescriptorCache to trigger a range lookup when the
			// lookupKey is next accessed. Then immediately access lookupKey.
			s.DistSender().RangeDescriptorCache().Clear()
			_, err = s.DB().Get(context.Background(), lookupKey)
		}
		rangeLookupErr <- err
	}()

	// Wait until the range lookup is blocked after performing a scan of the
	// first range [/meta2/a,/meta2/n) but before performing a scan of the
	// second range [/meta2/n,/meta2/z). Then split at key "m". Finally, let the
	// range lookup finish. The lookup will fail because it won't get consistent
	// results but will eventually succeed after retrying.
	select {
	case <-rangeLookupIsBlocked:
	case err := <-rangeLookupErr:
		// Unexpected early return.
		t.Fatalf("unexpected range lookup error %v", err)
	}
	mustSplit(roachpb.Key("m"))
	close(unblockRangeLookups)

	if err := <-rangeLookupErr; err != nil {
		t.Fatalf("unexpected range lookup error %v", err)
	}
}

// Verify that range lookup operations do not synchronously perform intent
// resolution as doing so can deadlock with the RangeDescriptorCache. See
// #17760.
func TestRangeLookupAsyncResolveIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	blockPushTxn := make(chan struct{})
	defer close(blockPushTxn)

	// Disable async tasks in the intent resolver. All tasks will be synchronous.
	cfg := kvserver.TestStoreConfig(nil)
	cfg.TestingKnobs.IntentResolverKnobs.ForceSyncIntentResolution = true
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.DisableMergeQueue = true
	cfg.TestingKnobs.TestingProposalFilter =
		func(args storagebase.ProposalFilterArgs) *roachpb.Error {
			for _, union := range args.Req.Requests {
				if union.GetInner().Method() == roachpb.PushTxn {
					<-blockPushTxn
					break
				}
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, cfg)

	// Split range 1 at an arbitrary key so that we're not dealing with the
	// first range for the rest of this test. The first range is handled
	// specially by the range descriptor cache.
	key := roachpb.Key("a")
	args := adminSplitArgs(key)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), args); pErr != nil {
		t.Fatal(pErr)
	}

	// Get original meta2 descriptor.
	rs, _, err := kv.RangeLookup(ctx, store.TestSender(), key, roachpb.READ_UNCOMMITTED, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	origDesc := rs[0]

	key2 := roachpb.Key("e")
	newDesc := origDesc
	newDesc.EndKey, err = keys.Addr(key2)
	if err != nil {
		t.Fatal(err)
	}

	// Write the new descriptor as an intent.
	data, err := protoutil.Marshal(&newDesc)
	if err != nil {
		t.Fatal(err)
	}
	txn := roachpb.MakeTransaction("test", key2, 1,
		store.Clock().Now(), store.Clock().MaxOffset().Nanoseconds())
	// Officially begin the transaction. If not for this, the intent resolution
	// machinery would simply remove the intent we write below, see #3020.
	// We send directly to Replica throughout this test, so there's no danger
	// of the Store aborting this transaction (i.e. we don't have to set a high
	// priority).
	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key2)).AsRawKey(), data)
	txn.Sequence++
	pArgs.Sequence = txn.Sequence
	if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: &txn}, pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Clear the range descriptor cache so that any future requests will first
	// need to perform a RangeLookup.
	store.DB().NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender).RangeDescriptorCache().Clear()

	// Now send a request, forcing the RangeLookup. Since the lookup is
	// inconsistent, there's no WriteIntentError, but we'll try to resolve any
	// intents that are found. If the RangeLookup op attempts to resolve the
	// intents synchronously, the operation will block forever.
	//
	// Note that 'a' < 'e'.
	if _, err := store.DB().Get(ctx, key); err != nil {
		t.Fatal(err)
	}
}

// Verify that replicas don't temporarily disappear from the replicas map during
// the splits. See #29144.
func TestStoreSplitDisappearingReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	go kvserver.WatchForDisappearingReplicas(t, store)
	for i := 0; i < 100; i++ {
		key := roachpb.Key(fmt.Sprintf("a%d", i))
		args := adminSplitArgs(key)
		if _, pErr := kv.SendWrapped(context.Background(), store.TestSender(), args); pErr != nil {
			t.Fatalf("%q: split unexpected error: %s", key, pErr)
		}
	}
}

// Regression test for #21146. This verifies the behavior of when the
// application of some split command (part of the lhs's log) is delayed on some
// store and meanwhile the rhs has rebalanced away and back, ending up with a
// larger ReplicaID than the split thinks it will have. Additionally we remove
// the LHS replica on the same store before the split and re-add it after, so
// that when the connectivity restores the LHS will apply a split trigger while
// it is not a part of the descriptor.
//
// Or, in pictures (s3 looks like s1 throughout and is omitted):
//
//     s1:  [----r1@all-------------]
//     s2:  [----r1@all-------------]
// Remove s2:
//     s1:  [----r1@s1s3------------]
//     s2:  [----r1@all-------------] (outdated)
// Split r1:
//     s1:  [-r1@s1s3-|--r2@s1s3----]
//     s2:  [----r1@all-------------] (outdated)
// Add s2:
//     s1:  [-r1@all-|--r2@s1s3-----]
//     s2:  [----r1@all-------------] (outdated)
// Add learner to s2 on r2 (remains uninitialized due to LHS state blocking it):
//     s1:  [-r1@s1s3-|--r2@all-----]
//     s2:  [----r1@all-------------] (outdated), uninitialized replica r2/3
// Remove and re-add learner multiple times: r2/3 becomes r2/100
//     (diagram looks the same except for replacing r2/3)
//
// When connectivity is restored, r1@s2 will start to catch up on the raft log
// after it learns of its new replicaID. It first processes the replication
// change that removes it and switches to a desc that doesn't contain itself as
// a replica. Next it sees the split trigger that once caused a crash because
// the store tried to look up itself and failed. This being handled correctly,
// the split trigger next has to look up the right hand side, which surprisingly
// has a higher replicaID than that seen in the split trigger. This too needs to
// be tolerated.
func TestSplitTriggerMeetsUnexpectedReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	blockPromoteCh := make(chan struct{})
	var skipLearnerSnaps int32
	withoutLearnerSnap := func(fn func()) {
		atomic.StoreInt32(&skipLearnerSnaps, 1)
		fn()
		atomic.StoreInt32(&skipLearnerSnaps, 0)
	}
	knobs := base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
		ReplicaSkipLearnerSnapshot: func() bool {
			return atomic.LoadInt32(&skipLearnerSnaps) != 0
		},
		ReplicaAddStopAfterLearnerSnapshot: func(targets []roachpb.ReplicationTarget) bool {
			if atomic.LoadInt32(&skipLearnerSnaps) != 0 {
				return false
			}
			if len(targets) > 0 && targets[0].StoreID == 2 {
				<-blockPromoteCh
			}
			return false
		},
		ReplicaAddSkipLearnerRollback: func() bool {
			return true
		},
		// We rely on replicas remaining where they are even when they are removed
		// from the range as this lets us set up a split trigger that will apply
		// on a replica that is (at the time of the split trigger) not a member.
		DisableReplicaGCQueue: true,
	}}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{Knobs: knobs},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	k := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, k)

	// Add a replica on n3 which we'll need to achieve quorum while we cut off n2 below.
	tc.AddReplicasOrFatal(t, k, tc.Target(2))

	// First construct a range with a learner replica on the second node (index 1)
	// and split it, ending up with an orphaned learner on each side of the split.
	// After the learner is created, but before the split, block all incoming raft
	// traffic to the learner on the lhs of the split (which is still on the
	// second node).
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		_, err := tc.AddReplicas(k, tc.Target(1))
		return err
	})

	store, _ := getFirstStoreReplica(t, tc.Server(1), k)
	tc.Servers[1].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
		rangeID:            desc.RangeID,
		RaftMessageHandler: store,
	})

	_, kRHS := k, k.Next()
	// Remove the LHS on the isolated store, split the range, and re-add it.
	tc.RemoveReplicasOrFatal(t, k, tc.Target(1))
	descLHS, descRHS := tc.SplitRangeOrFatal(t, kRHS)
	withoutLearnerSnap(func() {
		// NB: can't use AddReplicas since that waits for the target to be up
		// to date, which it won't in this case.
		//
		// We avoid sending a snapshot because that snapshot would include the
		// split trigger and we want that to be processed via the log.
		d, err := tc.Servers[0].DB().AdminChangeReplicas(
			ctx, descLHS.StartKey.AsRawKey(), descLHS, roachpb.MakeReplicationChanges(roachpb.ADD_REPLICA, tc.Target(1)),
		)
		require.NoError(t, err)
		descLHS = *d
	})

	close(blockPromoteCh)
	if err := g.Wait(); !testutils.IsError(err, `descriptor changed`) {
		t.Fatalf(`expected "descriptor changed" error got: %+v`, err)
	}

	// Now repeatedly re-add the learner on the rhs, so it has a
	// different replicaID than the split trigger expects.
	add := func() {
		_, err := tc.AddReplicas(kRHS, tc.Target(1))
		// The "snapshot intersects existing range" error is expected if the store
		// has not heard a raft message addressed to a later replica ID while the
		// "was not found on" error is expected if the store has heard that it has
		// a newer replica ID before receiving the snapshot.
		if !testutils.IsError(err, `snapshot intersects existing range|r[0-9]+ was not found on s[0-9]+`) {
			t.Fatalf(`expected snapshot intersects existing range|r[0-9]+ was not found on s[0-9]+" error got: %+v`, err)
		}
	}
	for i := 0; i < 5; i++ {
		add()
		tc.RemoveReplicasOrFatal(t, kRHS, tc.Target(1))
	}
	add()

	// Normally AddReplicas will return the latest version of the RangeDescriptor,
	// but because we're getting snapshot errors and using the
	// ReplicaAddSkipLearnerRollback hook, we have to look it up again ourselves
	// to find the current replicaID for the RHS learner.
	descRHS = tc.LookupRangeOrFatal(t, kRHS)
	learnerDescRHS, ok := descRHS.GetReplicaDescriptor(store.StoreID())
	require.True(t, ok)

	// Wait for there to be an in-memory, uninitialized learner replica with the
	// latest ReplicaID. Note: it cannot become initialized at this point because
	// it needs a snapshot to do that and (as can be seen in the error check
	// above) snapshots will intersect the lhs replica (which doesn't know about
	// the split because we've blocked its raft traffic, and so it still covers
	// the pre-split keyspace).
	testutils.SucceedsSoon(t, func() error {
		repl, err := store.GetReplica(descRHS.RangeID)
		if err != nil {
			return err
		}
		status := repl.RaftStatus()
		if status == nil {
			return errors.New("raft group not initialized")
		}
		if replicaID := roachpb.ReplicaID(status.ID); replicaID != learnerDescRHS.ReplicaID {
			return errors.Errorf("expected %d got %d", learnerDescRHS.ReplicaID, replicaID)
		}
		return nil
	})

	// Re-enable raft and wait for the lhs to catch up to the post-split
	// descriptor. This used to panic with "raft group deleted".
	tc.Servers[1].RaftTransport().Listen(store.StoreID(), store)
	testutils.SucceedsSoon(t, func() error {
		repl, err := store.GetReplica(descLHS.RangeID)
		if err != nil {
			return err
		}
		if desc := repl.Desc(); desc.IsInitialized() && !descLHS.Equal(desc) {
			require.NoError(t, store.ManualReplicaGC(repl))
			return errors.Errorf("expected %s got %s", &descLHS, desc)
		}
		return nil
	})
}

// TestSplitBlocksReadsToRHS tests that an ongoing range split does not
// interrupt reads to the LHS of the split but does interrupt reads for the RHS
// of the split. The test relies on the fact that EndTxn(SplitTrigger) declares
// read access to the LHS of the split but declares write access to the RHS of
// the split.
func TestSplitBlocksReadsToRHS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyLHS, keySplit, keyRHS := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")
	splitBlocked := make(chan struct{})
	propFilter := func(args storagebase.ProposalFilterArgs) *roachpb.Error {
		if req, ok := args.Req.GetArg(roachpb.EndTxn); ok {
			et := req.(*roachpb.EndTxnRequest)
			if tr := et.InternalCommitTrigger.GetSplitTrigger(); tr != nil {
				if tr.RightDesc.StartKey.Equal(keySplit) {
					// Signal that the split is blocked.
					splitBlocked <- struct{}{}
					// Wait for split to be unblocked.
					<-splitBlocked
				}
			}
		}
		return nil
	}

	storeCfg := kvserver.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	storeCfg.TestingKnobs.DisableMergeQueue = true
	storeCfg.TestingKnobs.TestingProposalFilter = propFilter
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)
	repl := store.LookupReplica(roachpb.RKey(keySplit))
	tsBefore := store.Clock().Now()

	// Begin splitting the range.
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		args := adminSplitArgs(keySplit)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
		return pErr.GoError()
	})

	// Wait until split is underway.
	<-splitBlocked
	tsAfter := store.Clock().Now()

	// Read from the LHS and RHS, both below and above the split timestamp.
	lhsDone, rhsDone := make(chan error, 2), make(chan error, 2)
	for _, keyAndChan := range []struct {
		key   roachpb.Key
		errCh chan error
	}{
		{keyLHS, lhsDone},
		{keyRHS, rhsDone},
	} {
		for _, ts := range []hlc.Timestamp{tsBefore, tsAfter} {
			h := roachpb.Header{Timestamp: ts, RangeID: repl.RangeID}
			args := getArgs(keyAndChan.key)
			errCh := keyAndChan.errCh
			g.GoCtx(func(ctx context.Context) error {
				// Send directly to repl to avoid racing with the
				// split and routing requests to the post-split RHS.
				_, pErr := kv.SendWrappedWith(ctx, repl, h, args)
				errCh <- pErr.GoError()
				return nil
			})
		}
	}

	// Only the LHS reads should succeed. The RHS reads should get
	// blocked waiting to acquire latches.
	for i := 0; i < cap(lhsDone); i++ {
		require.NoError(t, <-lhsDone)
	}
	select {
	case err := <-rhsDone:
		require.NoError(t, err)
		t.Fatal("unexpected read on RHS during split")
	case <-time.After(2 * time.Millisecond):
	}

	// Unblock the split.
	splitBlocked <- struct{}{}

	// The RHS reads should now both hit a RangeKeyMismatchError error.
	for i := 0; i < cap(rhsDone); i++ {
		require.Regexp(t, "outside of bounds of range", <-rhsDone)
	}
	require.Nil(t, g.Wait())
}
