// Copyright 2018 The Cockroach Authors.
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

package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestStatementReuses(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	initStmts := []string{
		`CREATE DATABASE d`,
		`USE d`,
		`CREATE TABLE a(b INT)`,
		`CREATE VIEW v AS SELECT 1`,
		`CREATE SEQUENCE s`,
		`CREATE INDEX woo ON a(b)`,
		`CREATE USER woo`,
	}

	for _, s := range initStmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	testData := []string{
		// Drop tests are first so that if they incorrectly perform
		// their side effects, the statements below will fail.
		`DROP INDEX a@woo`,
		`DROP TABLE a`,
		`DROP DATABASE d CASCADE`,
		`DROP SEQUENCE s`,
		`DROP VIEW v`,
		`DROP USER woo`,

		// Ditto ALTER first, so that erroneous side effects bork what's
		// below.
		`ALTER DATABASE d RENAME TO e`,
		`ALTER TABLE a RENAME TO x`,
		`ALTER TABLE a ADD COLUMN x INT`,
		`ALTER TABLE a RENAME COLUMN b TO c`,
		`ALTER TABLE a DROP COLUMN b`,
		`ALTER TABLE a CONFIGURE ZONE USING DEFAULT`,
		`ALTER TABLE a SPLIT AT VALUES(1)`,
		`ALTER TABLE a SCATTER`,

		`ALTER INDEX a@woo RENAME TO waa`,
		`ALTER INDEX a@woo CONFIGURE ZONE USING DEFAULT`,
		`ALTER INDEX a@woo SPLIT AT VALUES(1)`,
		`ALTER INDEX a@woo SCATTER`,

		`ALTER VIEW v RENAME TO x`,

		`ALTER SEQUENCE s RENAME TO x`,
		`ALTER SEQUENCE s NO CYCLE`,

		`ALTER RANGE DEFAULT CONFIGURE ZONE USING DEFAULT`,

		`ALTER USER woo WITH PASSWORD 'waa'`,

		`CANCEL JOBS SELECT 1`,
		`CANCEL QUERIES SELECT '1'`,
		`CANCEL SESSIONS SELECT '1'`,

		`CREATE DATABASE d2`,
		`CREATE INDEX c ON a(b)`,
		`CREATE SEQUENCE s2`,
		`CREATE STATISTICS st ON b FROM a`,
		`CREATE TABLE a2 (b INT)`,
		`CREATE VIEW v2 AS SELECT 1`,

		`DELETE FROM a`,
		`INSERT INTO a VALUES (1)`,
		`UPSERT INTO a VALUES (1)`,
		`UPDATE a SET b = 1`,

		`EXPLAIN ANALYZE (DISTSQL) SELECT 1`,
		`EXPLAIN SELECT 1`,

		// TODO(knz): backup/restore planning tests really should be
		// implementable here.
		// `BACKUP a TO 'a'`,
		// `SHOW BACKUP 'woo'`,

		`PAUSE JOBS SELECT 1`,
		`RESUME JOBS SELECT 1`,

		`SHOW ALL CLUSTER SETTINGS`,
		`SHOW CLUSTER SETTING version`,

		`SHOW CREATE a`,
		`SHOW COLUMNS FROM a`,
		`SHOW RANGES FROM TABLE a`,
		`SHOW ALL ZONE CONFIGURATIONS`,
		`SHOW ZONE CONFIGURATION FOR TABLE a`,
		`SHOW CONSTRAINTS FROM a`,
		`SHOW DATABASES`,
		`SHOW INDEXES FROM a`,
		`SHOW JOB 1`,
		`SHOW JOBS`,
		`SHOW ROLES`,
		`SHOW SCHEMAS`,
		`SHOW TABLES`,
		`SHOW USERS`,
		`SHOW database`,
	}

	t.Run("EXPLAIN", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				rows, err := db.Query("EXPLAIN " + test)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("PREPARE", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				if _, err := db.Exec("PREPARE p AS " + test); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("DEALLOCATE p"); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("WITH (cte)", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				rows, err := db.Query("EXPLAIN WITH a AS (" + test + ") TABLE a")
				if err != nil {
					if testutils.IsError(err, "does not return any columns") {
						// This error is acceptable and does not constitute a test failure.
						return
					}
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("PREPARE EXPLAIN", func(t *testing.T) {
		for _, test := range testData {
			t.Run(test, func(t *testing.T) {
				if _, err := db.Exec("PREPARE p AS EXPLAIN " + test); err != nil {
					t.Fatal(err)
				}
				rows, err := db.Query("EXECUTE p")
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("DEALLOCATE p"); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}
