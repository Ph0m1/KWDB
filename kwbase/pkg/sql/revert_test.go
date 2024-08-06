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

package sql

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRevertTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	s, sqlDB, kv := serverutils.StartServer(
		t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(context.TODO())

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY, rev INT DEFAULT 0, INDEX (rev))`)

	// Fill a table with some rows plus some revisions to those rows.
	const numRows = 1000
	db.Exec(t, `INSERT INTO test (k) SELECT generate_series(1, $1)`, numRows)
	db.Exec(t, `UPDATE test SET rev = 1 WHERE k % 3 = 0`)
	db.Exec(t, `DELETE FROM test WHERE k % 10 = 0`)
	db.Exec(t, `ALTER TABLE test SPLIT AT VALUES (30), (300), (501), (700)`)

	var ts string
	var before int
	db.QueryRow(t, `SELECT cluster_logical_timestamp(), xor_agg(k # rev) FROM test`).Scan(&ts, &before)
	targetTime, err := ParseHLC(ts)
	require.NoError(t, err)

	t.Run("simple", func(t *testing.T) {
		// Make some more edits: delete some rows and edit others, insert into some of
		// the gaps made between previous rows, edit a large swath of rows and add a
		// large swath of new rows as well.
		db.Exec(t, `UPDATE test SET rev = 2 WHERE k % 4 = 0`)
		db.Exec(t, `DELETE FROM test WHERE k % 5 = 2`)
		db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10`, numRows)
		db.Exec(t, `UPDATE test SET rev = 4 WHERE k > 150 and k < 350`)
		db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+500, 1), 500`, numRows)

		var edited, aost int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&edited)
		require.NotEqual(t, before, edited)
		db.QueryRow(t, fmt.Sprintf(`SELECT xor_agg(k # rev) FROM test AS OF SYSTEM TIME %s`, ts)).Scan(&aost)
		require.Equal(t, before, aost)

		// Revert the table to ts.
		desc := sqlbase.GetTableDescriptor(kv, "test", "test")
		desc.State = sqlbase.TableDescriptor_OFFLINE // bypass the offline check.
		require.NoError(t, RevertTables(context.TODO(), kv, []*sqlbase.TableDescriptor{desc}, targetTime, 10))

		var reverted int
		db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&reverted)
		require.Equal(t, before, reverted, "expected reverted table after edits to match before")
	})

	t.Run("interleaved", func(t *testing.T) {
		db.Exec(t, `CREATE TABLE child (a INT, b INT, rev INT DEFAULT 0, INDEX (rev), PRIMARY KEY (a, b)) INTERLEAVE IN PARENT test (a)`)
		db.Exec(t, `INSERT INTO child (a, b) SELECT generate_series(1, $1, 2), generate_series(2, $1, 2)`, numRows)
		db.Exec(t, `UPDATE child SET rev = 1 WHERE a % 3 = 0`)

		db.QueryRow(t, `SELECT cluster_logical_timestamp() FROM test`).Scan(&ts)
		targetTime, err = ParseHLC(ts)
		require.NoError(t, err)

		var beforeChild int
		db.QueryRow(t, `SELECT xor_agg(a # b # rev) FROM child`).Scan(&beforeChild)

		db.Exec(t, `UPDATE child SET rev = 2 WHERE a % 5 = 0`)
		db.Exec(t, `UPDATE child SET rev = 3 WHERE a > 450 and a < 700`)
		db.Exec(t, `DELETE FROM child WHERE a % 7 = 0`)

		// Revert the table to ts.
		desc := sqlbase.GetTableDescriptor(kv, "test", "test")
		desc.State = sqlbase.TableDescriptor_OFFLINE
		child := sqlbase.GetTableDescriptor(kv, "test", "child")
		child.State = sqlbase.TableDescriptor_OFFLINE
		t.Run("reject only parent", func(t *testing.T) {
			require.Error(t, RevertTables(ctx, kv, []*sqlbase.TableDescriptor{desc}, targetTime, 10))
		})
		t.Run("reject only child", func(t *testing.T) {
			require.Error(t, RevertTables(ctx, kv, []*sqlbase.TableDescriptor{child}, targetTime, 10))
		})

		t.Run("rollback parent and child", func(t *testing.T) {
			require.NoError(t, RevertTables(ctx, kv, []*sqlbase.TableDescriptor{desc, child}, targetTime, RevertTableDefaultBatchSize))

			var reverted, revertedChild int
			db.QueryRow(t, `SELECT xor_agg(k # rev) FROM test`).Scan(&reverted)
			require.Equal(t, before, reverted, "expected reverted table after edits to match before")
			db.QueryRow(t, `SELECT xor_agg(a # b # rev) FROM child`).Scan(&revertedChild)
			require.Equal(t, beforeChild, revertedChild, "expected reverted table after edits to match before")
		})
	})
}
