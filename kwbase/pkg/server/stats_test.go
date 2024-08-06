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

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func TestTelemetrySQLStatsIndependence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
`); err != nil {
		t.Fatal(err)
	}

	sqlServer := s.(*TestServer).Server.pgServer.SQLServer

	// Flush stats at the beginning of the test.
	sqlServer.ResetSQLStats(ctx)
	sqlServer.ResetReportedStats(ctx)

	// Run some queries mixed with diagnostics, and ensure that the statistics
	// are unnaffected by the calls to report diagnostics.
	before := timeutil.Now()
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 1); err != nil {
		t.Fatal(err)
	}
	s.(*TestServer).maybeReportDiagnostics(ctx, timeutil.Now(), before, time.Second)
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1)`, 2); err != nil {
		t.Fatal(err)
	}
	s.(*TestServer).maybeReportDiagnostics(ctx, timeutil.Now(), before, time.Second)

	// Ensure that our SQL statement data was not affected by the telemetry report.
	stats := sqlServer.GetUnscrubbedStmtStats()
	foundStat := false
	for _, stat := range stats {
		if strings.HasPrefix(stat.Key.Query, "INSERT INTO t.test VALUES") {
			foundStat = true
			if stat.Stats.Count != 2 {
				t.Fatal("expected to find 2 invocations, found", stat.Stats.Count)
			}
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}
}

func TestEnsureSQLStatsAreFlushedForTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Settings = cluster.MakeClusterSettings()
	// Set the SQL stat refresh rate very low so that SQL stats are continuously
	// flushed into the telemetry reporting stats pool.
	sql.SQLStatReset.Override(&params.Settings.SV, 10*time.Millisecond)
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Run some queries against the database.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (x INT PRIMARY KEY);
INSERT INTO t.test VALUES (1);
INSERT INTO t.test VALUES (2);
`); err != nil {
		t.Fatal(err)
	}

	statusServer := s.(*TestServer).status
	sqlServer := s.(*TestServer).Server.pgServer.SQLServer
	testutils.SucceedsSoon(t, func() error {
		// Get the diagnostic info.
		res, err := statusServer.Diagnostics(ctx, &serverpb.DiagnosticsRequest{NodeId: "local"})
		if err != nil {
			t.Fatal(err)
		}

		found := false
		for _, stat := range res.SqlStats {
			// These stats are scrubbed, so look for our scrubbed statement.
			if strings.HasPrefix(stat.Key.Query, "INSERT INTO _ VALUES (_)") {
				found = true
			}
		}

		if !found {
			return errors.New("expected to find query stats, but didn't")
		}

		// We should also not find the stat in the SQL stats pool, since the SQL
		// stats are getting flushed.
		stats := sqlServer.GetScrubbedStmtStats()
		for _, stat := range stats {
			// These stats are scrubbed, so look for our scrubbed statement.
			if strings.HasPrefix(stat.Key.Query, "INSERT INTO _ VALUES (_)") {
				t.Error("expected to not find stat, but did")
			}
		}
		return nil
	})
}

func TestSQLStatCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	sqlServer := s.(*TestServer).Server.pgServer.SQLServer

	// Flush stats at the beginning of the test.
	sqlServer.ResetSQLStats(ctx)
	sqlServer.ResetReportedStats(ctx)

	// Execute some queries against the sqlDB to build up some stats.
	if _, err := sqlDB.Exec(`
	CREATE DATABASE t;
	CREATE TABLE t.test (x INT PRIMARY KEY);
	INSERT INTO t.test VALUES (1);
	INSERT INTO t.test VALUES (2);
	INSERT INTO t.test VALUES (3);
`); err != nil {
		t.Fatal(err)
	}

	// Collect stats from the SQL server and ensure our queries are present.
	stats := sqlServer.GetUnscrubbedStmtStats()
	foundStat := false
	var sqlStatData roachpb.StatementStatistics

	for _, stat := range stats {
		if strings.HasPrefix(stat.Key.Query, "INSERT INTO t.test VALUES") {
			foundStat = true
			sqlStatData = stat.Stats
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}

	const epsilon = 0.00001

	// Reset the SQL statistics, which will dump stats into the
	// reported statistics pool.
	sqlServer.ResetSQLStats(ctx)

	// Query the reported statistics.
	stats = sqlServer.GetUnscrubbedReportingStats()
	foundStat = false
	for _, stat := range stats {
		if strings.HasPrefix(stat.Key.Query, "INSERT INTO t.test VALUES") {
			foundStat = true
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData.String(), "found", stat.Stats.String())
			}
		}
	}

	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}

	// Make another query to the db.
	if _, err := sqlDB.Exec(`
	INSERT INTO t.test VALUES (4);
	INSERT INTO t.test VALUES (5);
	INSERT INTO t.test VALUES (6);
`); err != nil {
		t.Fatal(err)
	}

	// Find and record the stats for our second query.
	stats = sqlServer.GetUnscrubbedStmtStats()
	foundStat = false
	for _, stat := range stats {
		if strings.HasPrefix(stat.Key.Query, "INSERT INTO t.test VALUES") {
			foundStat = true
			// Add into the current stat data the collected data.
			sqlStatData.Add(&stat.Stats)
		}
	}
	if !foundStat {
		t.Fatal("expected to find stats for insert query, but didn't")
	}

	// Flush the SQL stats again.
	sqlServer.ResetSQLStats(ctx)

	// Find our statement stat from the reported stats pool.
	stats = sqlServer.GetUnscrubbedReportingStats()
	foundStat = false
	for _, stat := range stats {
		if strings.HasPrefix(stat.Key.Query, "INSERT INTO t.test VALUES") {
			foundStat = true
			// The new value for the stat should be the aggregate of the previous stat
			// value, and the old stat value. Additionally, zero out the timestamps for
			// the logical plans, as they won't be the same.
			now := timeutil.Now()
			stat.Stats.SensitiveInfo.MostRecentPlanTimestamp, sqlStatData.SensitiveInfo.MostRecentPlanTimestamp = now, now
			if !stat.Stats.AlmostEqual(&sqlStatData, epsilon) {
				t.Fatal("expected stats", sqlStatData, "found", stat.Stats)
			}
		}
	}

	if !foundStat {
		t.Fatal("expected to find stats for insert query in reported pool, but didn't")
	}
}
