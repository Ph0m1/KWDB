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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMaterializedViewClearedAfterRefresh ensures that the old state of the
// view is cleaned up after it is refreshed.
func TestMaterializedViewClearedAfterRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer disableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1), (2);
CREATE MATERIALIZED VIEW t.v AS SELECT x FROM t.t;
`); err != nil {
		t.Fatal(err)
	}

	descBeforeRefresh := sqlbase.GetImmutableTableDescriptor(kvDB, "t", "v")

	// Update the view and refresh it.
	if _, err := sqlDB.Exec(`
INSERT INTO t.t VALUES (3);
REFRESH MATERIALIZED VIEW t.v;
`); err != nil {
		t.Fatal(err)
	}

	// Add a zone config to delete all table data.
	_, err := addImmediateGCZoneConfig(sqlDB, descBeforeRefresh.GetID())
	if err != nil {
		t.Fatal(err)
	}

	// The data should be deleted.
	testutils.SucceedsSoon(t, func() error {
		indexPrefix := sqlbase.MakeIndexKeyPrefix(&descBeforeRefresh.TableDescriptor, descBeforeRefresh.PrimaryIndex.ID)
		indexEnd := roachpb.Key(indexPrefix).PrefixEnd()
		if kvs, err := kvDB.Scan(ctx, indexPrefix, indexEnd, 0); err != nil {
			t.Fatal(err)
		} else if len(kvs) != 0 {
			return errors.Newf("expected 0 kvs, found %d", len(kvs))
		}
		return nil
	})
}

// TestMaterializedViewRefreshVisibility ensures that intermediate results written
// as part of the refresh backfill process aren't visibile until the refresh is done.
func TestMaterializedViewRefreshVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	waitForCommit, waitToProceed, refreshDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeMaterializedViewRefreshCommit: func() error {
				close(waitForCommit)
				<-waitToProceed
				return nil
			},
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Make a materialized view and update the data behind it.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1), (2);
CREATE MATERIALIZED VIEW t.v AS SELECT x FROM t.t;
INSERT INTO t.t VALUES (3);
`); err != nil {
		t.Fatal(err)
	}

	// Start a refresh.
	go func() {
		if _, err := sqlDB.Exec(`REFRESH MATERIALIZED VIEW t.v`); err != nil {
			t.Error(err)
		}
		close(refreshDone)
	}()

	<-waitForCommit

	// Before the refresh commits, we shouldn't see any updated data.
	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.CheckQueryResults(t, "SELECT * FROM t.v ORDER BY x", [][]string{{"1"}, {"2"}})

	// Let the refresh commit.
	close(waitToProceed)
	<-refreshDone
	runner.CheckQueryResults(t, "SELECT * FROM t.v ORDER BY x", [][]string{{"1"}, {"2"}, {"3"}})
}

func TestMaterializedViewCleansUpOnRefreshFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	// Protects shouldError
	var mu syncutil.Mutex
	shouldError := true

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeMaterializedViewRefreshCommit: func() error {
				mu.Lock()
				defer mu.Unlock()
				if shouldError {
					shouldError = false
					return errors.New("boom")
				}
				return nil
			},
		},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer disableGCTTLStrictEnforcement(t, sqlDB)()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1), (2);
CREATE MATERIALIZED VIEW t.v AS SELECT x FROM t.t;
`); err != nil {
		t.Fatal(err)
	}

	descBeforeRefresh := sqlbase.GetImmutableTableDescriptor(kvDB, "t", "v")

	// Add a zone config to delete all table data.
	_, err := addImmediateGCZoneConfig(sqlDB, descBeforeRefresh.GetID())
	if err != nil {
		t.Fatal(err)
	}

	// Attempt (and fail) to refresh the view.
	if _, err := sqlDB.Exec(`REFRESH MATERIALIZED VIEW t.v`); err == nil {
		t.Fatal("expected error, but found nil")
	}

	testutils.SucceedsSoon(t, func() error {
		tableStart := keys.MakeTablePrefix(uint32(descBeforeRefresh.ID))
		tableEnd := roachpb.Key(tableStart).PrefixEnd()
		if kvs, err := kvDB.Scan(ctx, tableStart, tableEnd, 0); err != nil {
			t.Fatal(err)
		} else if len(kvs) != 2 {
			return errors.Newf("expected to find only 2 KVs, but found %d", len(kvs))
		}
		return nil
	})
}

func TestDropMaterializedView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlRaw, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer disableGCTTLStrictEnforcement(t, sqlRaw)()

	sqlDB := sqlutils.SQLRunner{DB: sqlRaw}

	// Create a view with some data.
	sqlDB.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.t (x INT);
INSERT INTO t.t VALUES (1), (2);
CREATE MATERIALIZED VIEW t.v AS SELECT x FROM t.t;
`)
	desc := sqlbase.GetImmutableTableDescriptor(kvDB, "t", "v")
	// Add a zone config to delete all table data.
	_, err := addImmediateGCZoneConfig(sqlRaw, desc.GetID())
	require.NoError(t, err)

	// Now drop the view.
	sqlDB.Exec(t, `DROP MATERIALIZED VIEW t.v`)
	require.NoError(t, err)

	// All of the table data should be cleaned up.
	testutils.SucceedsSoon(t, func() error {
		tableStart := keys.MakeTablePrefix(uint32(desc.ID))
		tableEnd := roachpb.Key(tableStart).PrefixEnd()
		if kvs, err := kvDB.Scan(ctx, tableStart, tableEnd, 0); err != nil {
			t.Fatal(err)
		} else if len(kvs) != 0 {
			return errors.Newf("expected to find 0 KVs, but found %d", len(kvs))
		}
		return nil
	})
}
