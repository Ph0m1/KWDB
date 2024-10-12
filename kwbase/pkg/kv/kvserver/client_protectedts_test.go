// Copyright 2019 The Cockroach Authors.
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
	"context"
	"regexp"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptstorage"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptverifier"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestamps is an end-to-end test for protected timestamps.
// It works by writing a lot of data and waiting for the GC heuristic to allow
// for GC. Because of this, it's very slow and expensive. It should
// potentially be made cheaper by injecting hooks to force GC.
//
// Probably this test should always be skipped until it is made cheaper,
// nevertheless it's a useful test.
func TestProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	if util.RaceEnabled || testing.Short() {
		t.Skip("this test is too slow to run with race")
	}

	args := base.TestClusterArgs{}
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{DisableGCQueue: true}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)
	s0 := tc.Server(0)

	conn := tc.ServerConn(0)
	_, err := conn.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	require.NoError(t, err)

	_, err = conn.Exec("ALTER TABLE foo CONFIGURE ZONE USING " +
		"gc.ttlseconds = 1, range_max_bytes = 5<<20, range_min_bytes = 1<<20;")
	require.NoError(t, err)

	rRand, _ := randutil.NewPseudoRand()
	upsertUntilBackpressure := func() {
		for {
			_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)",
				randutil.RandBytes(rRand, 1<<15))
			if testutils.IsError(err, "backpressure") {
				break
			}
			require.NoError(t, err)
		}
	}
	const processedPattern = `(?s)shouldQueue=true.*processing replica.*GC score after GC`
	processedRegexp := regexp.MustCompile(processedPattern)

	getTableStartKey := func(table string) roachpb.Key {
		row := conn.QueryRow(
			"SELECT start_key "+
				"FROM kwdb_internal.ranges_no_leases "+
				"WHERE table_name = $1 "+
				"AND database_name = current_database() "+
				"ORDER BY start_key ASC "+
				"LIMIT 1",
			"foo")

		var startKey roachpb.Key
		require.NoError(t, row.Scan(&startKey))
		return startKey
	}

	getStoreAndReplica := func() (*kvserver.Store, *kvserver.Replica) {
		startKey := getTableStartKey("foo")
		// Okay great now we have a key and can go find replicas and stores and what not.
		r := tc.LookupRangeOrFatal(t, startKey)
		l, _, err := tc.FindRangeLease(r, nil)
		require.NoError(t, err)

		lhServer := tc.Server(int(l.Replica.NodeID) - 1)
		return getFirstStoreReplica(t, lhServer, startKey)
	}

	gcSoon := func() {
		testutils.SucceedsSoon(t, func() error {
			upsertUntilBackpressure()
			s, repl := getStoreAndReplica()
			trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, false)
			require.NoError(t, err)
			if !processedRegexp.MatchString(trace.String()) {
				return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
			}
			return nil
		})
	}

	thresholdRE := regexp.MustCompile(`(?s).*Threshold:(?P<threshold>[^\s]*)`)
	thresholdFromTrace := func(trace tracing.Recording) hlc.Timestamp {
		threshStr := string(thresholdRE.ExpandString(nil, "$threshold",
			trace.String(), thresholdRE.FindStringSubmatchIndex(trace.String())))
		thresh, err := hlc.ParseTimestamp(threshStr)
		require.NoError(t, err)
		return thresh
	}

	beforeWrites := s0.Clock().Now()
	gcSoon()

	pts := ptstorage.New(s0.ClusterSettings(), s0.InternalExecutor().(*sql.InternalExecutor))
	ptsWithDB := ptstorage.WithDatabase(pts, s0.DB())
	startKey := getTableStartKey("foo")
	ptsRec := ptpb.Record{
		ID:        uuid.MakeV4(),
		Timestamp: s0.Clock().Now(),
		Mode:      ptpb.PROTECT_AFTER,
		Spans: []roachpb.Span{
			{
				Key:    startKey,
				EndKey: startKey.PrefixEnd(),
			},
		},
	}
	require.NoError(t, ptsWithDB.Protect(ctx, nil /* txn */, &ptsRec))
	upsertUntilBackpressure()
	// We need to be careful choosing a time. We're a little limited because the
	// ttl is defined in seconds and we need to wait for the threshold to be
	// 2x the threshold with the scale factor as time since threshold. The
	// gc threshold we'll be able to set precedes this timestamp which we'll
	// put in the record below.
	afterWrites := s0.Clock().Now().Add(2*time.Second.Nanoseconds(), 0)
	s, repl := getStoreAndReplica()
	// The protectedts record will prevent us from aging the MVCC garbage bytes
	// past the oldest record so shouldQueue should be false. Verify that.
	trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, false /* skipShouldQueue */)
	require.NoError(t, err)
	require.Regexp(t, "(?s)shouldQueue=false", trace.String())

	// If we skipShouldQueue then gc will run but it should only run up to the
	// timestamp of our record at the latest.
	trace, _, err = s.ManuallyEnqueue(ctx, "gc", repl, true /* skipShouldQueue */)
	require.NoError(t, err)
	require.Regexp(t, "(?s)done with GC evaluation for 0 keys", trace.String())
	thresh := thresholdFromTrace(trace)
	require.Truef(t, thresh.Less(ptsRec.Timestamp), "threshold: %v, protected %v %q", thresh, ptsRec.Timestamp, trace)

	// Verify that the record indeed did apply as far as the replica is concerned.
	ptv := ptverifier.New(s0.DB(), pts)
	require.NoError(t, ptv.Verify(ctx, ptsRec.ID))
	ptsRecVerified, err := ptsWithDB.GetRecord(ctx, nil /* txn */, ptsRec.ID)
	require.NoError(t, err)
	require.True(t, ptsRecVerified.Verified)

	// Make a new record that is doomed to fail.
	failedRec := ptsRec
	failedRec.ID = uuid.MakeV4()
	failedRec.Timestamp = beforeWrites
	failedRec.Timestamp.Logical = 0
	require.NoError(t, ptsWithDB.Protect(ctx, nil /* txn */, &failedRec))
	_, err = ptsWithDB.GetRecord(ctx, nil /* txn */, failedRec.ID)
	require.NoError(t, err)

	// Verify that it indeed did fail.
	verifyErr := ptv.Verify(ctx, failedRec.ID)
	require.Regexp(t, "failed to verify protection", verifyErr)

	// Add a new record that is after the old record.
	laterRec := ptsRec
	laterRec.ID = uuid.MakeV4()
	laterRec.Timestamp = afterWrites
	laterRec.Timestamp.Logical = 0
	require.NoError(t, ptsWithDB.Protect(ctx, nil /* txn */, &laterRec))
	require.NoError(t, ptv.Verify(ctx, laterRec.ID))

	// Release the record that had succeeded and ensure that GC eventually
	// happens up to the protected timestamp of the new record.
	require.NoError(t, ptsWithDB.Release(ctx, nil, ptsRec.ID))
	testutils.SucceedsSoon(t, func() error {
		trace, _, err = s.ManuallyEnqueue(ctx, "gc", repl, false)
		require.NoError(t, err)
		if !processedRegexp.MatchString(trace.String()) {
			return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
		}
		thresh := thresholdFromTrace(trace)
		require.Truef(t, ptsRec.Timestamp.Less(thresh), "%v >= %v",
			ptsRec.Timestamp, thresh)
		require.Truef(t, thresh.Less(laterRec.Timestamp), "%v >= %v",
			thresh, laterRec.Timestamp)
		return nil
	})

	// Release the failed record.
	require.NoError(t, ptsWithDB.Release(ctx, nil, failedRec.ID))
	require.NoError(t, ptsWithDB.Release(ctx, nil, laterRec.ID))
	state, err := ptsWithDB.GetState(ctx, nil)
	require.NoError(t, err)
	require.Len(t, state.Records, 0)
	require.Equal(t, int(state.NumRecords), len(state.Records))
}
