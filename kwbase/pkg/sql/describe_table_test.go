package sql

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDescribe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	db := sqlutils.MakeSQLRunner(rawDB)

	// Workspace.
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `SET DATABASE = test`)

	// Prepare source and destination tables.
	db.Exec(t, `CREATE TABLE like_src (
		id INT PRIMARY KEY,
		a  INT NOT NULL DEFAULT 1,
		b  STRING,
		INDEX like_src_a_idx (a),
		UNIQUE (b)
	)`)
	db.Exec(t, `CREATE TABLE like_dst LIKE like_src`)

	// DESCRIBE behaves consistently with SHOW COLUMNS (at least on column name + type set).
	showRows := db.QueryStr(t, `SELECT column_name, data_type FROM [SHOW COLUMNS FROM like_dst]`)
	descRows := db.QueryStr(t, `DESCRIBE like_dst`)
	if len(descRows) != len(showRows) {
		t.Fatalf("DESCRIBE rows %d != SHOW rows %d", len(descRows), len(showRows))
	}
	// Build a map of expected (name,type) pairs.
	expected := map[string]struct{}{}
	for _, r := range showRows {
		if len(r) >= 2 {
			expected[r[0]+"/"+r[1]] = struct{}{}
		}
	}
	for _, r := range descRows {
		if len(r) < 2 {
			t.Fatalf("unexpected DESCRIBE row shape: %v", r)
		}
		key := r[0] + "/" + r[1]
		if _, ok := expected[key]; !ok {
			t.Fatalf("DESCRIBE mismatch on column/type: %v", r)
		}
	}
}