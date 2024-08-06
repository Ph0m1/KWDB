// Copyright 2020 The Cockroach Authors.
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

package stmtdiagnostics_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stmtdiagnostics"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	_, err := db.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query.
	registry := s.ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder.(*stmtdiagnostics.Registry)
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	reqRow := db.QueryRow(
		"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
	var completed bool
	var traceID gosql.NullInt64
	require.NoError(t, reqRow.Scan(&completed, &traceID))
	require.False(t, completed)
	require.False(t, traceID.Valid) // traceID should be NULL

	// Run the query.
	_, err = db.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)

	// Check that the row from statement_diagnostics_request was marked as completed.
	checkCompleted := func(reqID int64) {
		traceRow := db.QueryRow(
			"SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests WHERE ID = $1", reqID)
		require.NoError(t, traceRow.Scan(&completed, &traceID))
		require.True(t, completed)
		require.True(t, traceID.Valid)
	}

	checkCompleted(reqID)

	// Check the trace.
	row := db.QueryRow("SELECT jsonb_pretty(trace) FROM system.statement_diagnostics WHERE ID = $1", traceID.Int64)
	var json string
	require.NoError(t, row.Scan(&json))
	require.Contains(t, json, "traced statement")
	require.Contains(t, json, "statement execution committed the txn")

	// Verify that we can handle multiple requests at the same time.
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test")
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _")
	require.NoError(t, err)

	// Run the queries in a different order.
	_, err = db.Exec("SELECT x FROM test")
	require.NoError(t, err)
	checkCompleted(id2)

	_, err = db.Exec("SELECT x FROM test WHERE x > 1")
	require.NoError(t, err)
	checkCompleted(id3)

	_, err = db.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	checkCompleted(id1)
}

// Test that a different node can service a diagnostics request.
func TestDiagnosticsRequestDifferentNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := serverutils.StartTestCluster(t, 2, base.TestClusterArgs{})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	db0 := tc.ServerConn(0)
	db1 := tc.ServerConn(1)
	_, err := db0.Exec("CREATE TABLE test (x int PRIMARY KEY)")
	require.NoError(t, err)

	// Ask to trace a particular query using node 0.
	registry := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).StmtDiagnosticsRecorder.(*stmtdiagnostics.Registry)
	reqID, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	reqRow := db0.QueryRow(
		`SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests
		 WHERE ID = $1`, reqID)
	var completed bool
	var traceID gosql.NullInt64
	require.NoError(t, reqRow.Scan(&completed, &traceID))
	require.False(t, completed)
	require.False(t, traceID.Valid) // traceID should be NULL

	// Repeatedly run the query through node 1 until we get a trace.
	runUntilTraced := func(query string, reqID int64) {
		testutils.SucceedsSoon(t, func() error {
			// Run the query using node 1.
			_, err = db1.Exec(query)
			require.NoError(t, err)

			// Check that the row from statement_diagnostics_request was marked as completed.
			traceRow := db0.QueryRow(
				`SELECT completed, statement_diagnostics_id FROM system.statement_diagnostics_requests
				 WHERE ID = $1`, reqID)
			require.NoError(t, traceRow.Scan(&completed, &traceID))
			if !completed {
				_, err := db0.Exec("DELETE FROM test")
				require.NoError(t, err)
				return fmt.Errorf("not completed yet")
			}
			return nil
		})
		require.True(t, traceID.Valid)

		// Check the trace.
		row := db0.QueryRow(`SELECT jsonb_pretty(trace) FROM system.statement_diagnostics
                         WHERE ID = $1`, traceID.Int64)
		var json string
		require.NoError(t, row.Scan(&json))
		require.Contains(t, json, "traced statement")
	}
	runUntilTraced("INSERT INTO test VALUES (1)", reqID)

	// Verify that we can handle multiple requests at the same time.
	id1, err := registry.InsertRequestInternal(ctx, "INSERT INTO test VALUES (_)")
	require.NoError(t, err)
	id2, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test")
	require.NoError(t, err)
	id3, err := registry.InsertRequestInternal(ctx, "SELECT x FROM test WHERE x > _")
	require.NoError(t, err)

	// Run the queries in a different order.
	runUntilTraced("SELECT x FROM test", id2)
	runUntilTraced("SELECT x FROM test WHERE x > 1", id3)
	runUntilTraced("INSERT INTO test VALUES (2)", id1)
}

// TestChangePollInterval ensures that changing the polling interval takes effect.
func TestChangePollInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We'll inject a request filter to detect scans due to the polling.
	tableStart := roachpb.Key(keys.MakeTablePrefix(keys.StatementDiagnosticsRequestsTableID))
	tableSpan := roachpb.Span{
		Key:    tableStart,
		EndKey: tableStart.PrefixEnd(),
	}
	var scanState = struct {
		syncutil.Mutex
		m map[uuid.UUID]struct{}
	}{
		m: map[uuid.UUID]struct{}{},
	}
	recordScan := func(id uuid.UUID) {
		scanState.Lock()
		defer scanState.Unlock()
		scanState.m[id] = struct{}{}
	}
	numScans := func() int {
		scanState.Lock()
		defer scanState.Unlock()
		return len(scanState.m)
	}
	waitForScans := func(atLeast int) (seen int) {
		testutils.SucceedsSoon(t, func() error {
			if seen = numScans(); seen < atLeast {
				return errors.Errorf("expected at least %d scans, saw %d", atLeast, seen)
			}
			return nil
		})
		return seen
	}
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
					if request.Txn == nil {
						return nil
					}
					for _, req := range request.Requests {
						if scan := req.GetScan(); scan != nil && scan.Span().Overlaps(tableSpan) {
							recordScan(request.Txn.ID)
							return nil
						}
					}
					return nil
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, args)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	require.Equal(t, 1, waitForScans(1))
	time.Sleep(time.Millisecond) // ensure no unexpected scan occur
	require.Equal(t, 1, waitForScans(1))
	_, err := db.Exec("SET CLUSTER SETTING sql.stmt_diagnostics.poll_interval = '200us'")
	require.NoError(t, err)
	waitForScans(10) // ensure several scans occur
}
