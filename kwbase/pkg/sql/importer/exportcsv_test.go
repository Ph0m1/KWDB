// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package importer_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/workload/bank"
	"gitee.com/kwbasedb/kwbase/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, string, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir, UseDatabase: "test"}},
	)
	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)
	db.Exec(t, "CREATE DATABASE test")

	wk := bank.FromRows(rows)
	l := workloadsql.InsertsDataLoader{BatchSize: 100, Concurrency: 3}
	if _, err := workloadsql.Setup(ctx, conn, wk, l); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	v, err := tc.Servers[0].DB().Get(context.TODO(), keys.DescIDGenerator)
	if err != nil {
		t.Fatal(err)
	}
	last := uint32(v.ValueInt())
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(5000)
	config.TestingSetZoneConfig(last+1, zoneConfig)
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT count(*) FROM bank]")

	return db, dir, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportEncloseEscape(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "CREATE TABLE t1 (k_timestamp TIMESTAMP NOT NULL, c1 CHAR NULL, c2 CHAR(20) NULL, c3 BYTES NULL, c4 BYTES NULL, c5 NCHAR NULL, c6 NCHAR(20) NULL, c7 VARCHAR(254) NULL, c8 VARCHAR(20) NULL, c9 VARBYTES NULL, c10 VARBYTES NULL)")
	db.Exec(t, "insert into t1 values('2023-12-21 09:00:00.000',',', 'abcdef','a','abcdef',',','abcdef','abcdef','abcdef','abcdef','abcdef');")
	db.Exec(t, `insert into t1 values('2023-12-21 09:00:01.000',',', E'abc"de\\f','a',E'abc"de\f',',',E'abc"de\\f',E'abc"de\\f',E'abc"de\\f',E'abc"de\f',E'abc"de\f');`)
	db.Exec(t, `insert into t1 values('2023-12-21 09:00:03.000',',', E'abc\'de\\f','a',E'abc\'de\f',',',E'abc\'de\\f',E'abc\'de\\f',E'abc\'de\\f',E'abc\'de\f',E'abc\'de\f');`)
	db.Exec(t, "CREATE TABLE t2 (k_timestamp TIMESTAMP NOT NULL, c1 CHAR NULL, c2 CHAR(20) NULL, c3 BYTES NULL, c4 BYTES NULL, c5 NCHAR NULL, c6 NCHAR(20) NULL, c7 VARCHAR(254) NULL, c8 VARCHAR(20) NULL, c9 VARBYTES NULL, c10 VARBYTES NULL)")

	// chunkSize := 13
	enclosedbyList := []rune{'"', '\''}
	escapedbyList := []rune{'"', '\\'}
	for i, enclosed := range enclosedbyList {
		for j, escaped := range escapedbyList {
			withEnclosed := fmt.Sprintf("enclosed = E'%c'", enclosed)
			if enclosed == '\'' {
				withEnclosed = fmt.Sprintf("enclosed = E'\\%c'", enclosed)
			}
			withEscaped := fmt.Sprintf("escaped = E'%c'", escaped)
			if escaped == '\\' {
				withEscaped = fmt.Sprintf("escaped = E'\\%c'", escaped)
			}
			t.Run(fmt.Sprintf("enclosed=%c, escaped=%c", enclosed, escaped), func(t *testing.T) {
				var asOf string
				db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)
				distinctValue := strconv.Itoa(i) + strconv.Itoa(j)
				sqlR := fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://01/%v/t'
				FROM SELECT * FROM t1 WITH %s, %s`, distinctValue, withEnclosed, withEscaped)
				for _, row := range db.QueryStr(t, sqlR) {
					var files []string
					nodeID, err := strconv.Atoi(row[2])
					if err != nil {
						t.Fatal(err)
					}
					fileNum, err := strconv.Atoi(row[3])
					if err != nil {
						t.Fatal(err)
					}
					for i := 0; i < fileNum; i++ {
						fileName := fmt.Sprintf("n%d.%d.csv", nodeID, i)
						files = append(files, fileName)
						f, err := ioutil.ReadFile(filepath.Join(dir, distinctValue, "t", fileName))
						if err != nil {
							t.Fatal(err)
						}
						t.Log(string(f))
					}
					// schema := bank.FromRows(1).Tables()[0].Schema
					basePath := fmt.Sprintf("nodelocal://01/%v/t/", distinctValue)
					sep := fmt.Sprintf("', '%v", basePath)
					fileList := "'" + basePath + strings.Join(files, sep) + "'"
					db.Exec(t, fmt.Sprintf(`IMPORT INTO t2 CSV DATA (%s) WITH %s, %s`, fileList, withEnclosed, withEscaped))

					db.CheckQueryResults(t,
						fmt.Sprintf(`SELECT * FROM t1 ORDER BY k_timestamp`), db.QueryStr(t, `SELECT * FROM t2 ORDER BY k_timestamp`),
					)

					db.Exec(t, "DELETE from t2")
				}
			})

		}
	}
	db.Exec(t, "DROP TABLE t1")
	db.Exec(t, "DROP TABLE t2")
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "UPDATE bank SET payload = payload || '✅' WHERE id = 5")
	db.Exec(t, "UPDATE bank SET payload = NULL WHERE id % 2 = 0")

	chunkSize := 13
	for _, null := range []string{"", "NULL"} {
		nullAs, nullIf := "", ", nullif = ''"
		if null != "" {
			nullAs = fmt.Sprintf(", nullas = '%s'", null)
			nullIf = fmt.Sprintf(", nullif = '%s'", null)
		}
		t.Run("null="+null, func(t *testing.T) {
			var files []string

			var asOf string
			db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)

			for _, row := range db.QueryStr(t,
				fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://01/t'
				FROM SELECT * FROM bank AS OF SYSTEM TIME %s 
				WITH chunk_rows = $1, delimiter = '|' %s`, asOf, nullAs), chunkSize,
			) {
				nodeID, err := strconv.Atoi(row[2])
				if err != nil {
					t.Fatal(err)
				}
				fileNum, err := strconv.Atoi(row[3])
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < fileNum; i++ {
					fileName := fmt.Sprintf("n%d.%d.csv", nodeID, i)
					files = append(files, fileName)
					f, err := ioutil.ReadFile(filepath.Join(dir, "t", fileName))
					if err != nil {
						t.Fatal(err)
					}
					t.Log(string(f))
				}
			}

			schema := bank.FromRows(1).Tables()[0].Schema
			fileList := "'nodelocal://01/t/" + strings.Join(files, "', 'nodelocal://01/t/") + "'"
			db.Exec(t, fmt.Sprintf(`IMPORT TABLE bank2 %s CSV DATA (%s) WITH delimiter = '|'%s`, schema, fileList, nullIf))

			db.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM bank AS OF SYSTEM TIME %s ORDER BY id`, asOf), db.QueryStr(t, `SELECT * FROM bank2 ORDER BY id`),
			)
			db.CheckQueryResults(t,
				`SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank2`, db.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank`),
			)
			db.Exec(t, "DROP TABLE bank2")
		})
	}
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nodes := 5
	exportRows := 100
	db, _, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	maxTries := 10
	// we might need to retry if our table didn't actually scatter enough.
	for tries := 0; tries < maxTries; tries++ {
		chunkSize := 13
		rows := db.Query(t,
			`EXPORT INTO CSV 'nodelocal://01/t' FROM SELECT * FROM bank WHERE id >= $1 and id < $2 WITH chunk_rows = $3`,
			10, 10+exportRows, chunkSize,
		)

		files, totalRows := 0, 0
		nodesSeen := make(map[int]bool)
		for rows.Next() {
			filename, count, nodeID, fileNum := "", 0, 0, 0
			if err := rows.Scan(&filename, &count, &nodeID, &fileNum); err != nil {
				t.Fatal(err)
			}
			files += fileNum
			totalRows += count
			nodesSeen[nodeID] = true
		}
		if totalRows != exportRows {
			t.Fatalf("Expected %d rows, got %d", exportRows, totalRows)
		}
		if expected := exportRows / chunkSize; files < expected {
			t.Fatalf("expected at least %d files, got %d", expected, files)
		}
		if len(nodesSeen) < 2 {
			// table isn't as scattered as we expected, but we can try again.
			if tries < maxTries {
				continue
			}
			t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
		}
		break
	}
}

func TestExportJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://01/join' FROM SELECT * FROM t, t as u`)
}

func TestExportOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int primary key, x int, y int, z int, index (y))`)
	sqlDB.Exec(t, `insert into foo values (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://01/order' from select * from foo order by y asc limit 2`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "order", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://01/show' FROM SELECT * FROM [SHOW DATABASES] ORDER BY database_name`)
	content, err := ioutil.ReadFile(filepath.Join(dir, "show", "n1.0.csv"))
	if err != nil {
		t.Fatal(err)
	}
	var expected [][]string
	wholeString := strings.Split(string(content), "\n")
	wholeString = wholeString[:len(wholeString)-1]
	for _, s := range wholeString {
		expected = append(expected, strings.Split(s, ","))
	}
	sqlDB.CheckQueryResults(t, "SHOW DATABASES", expected)
}

// TestExportVectorized makes sure that SupportsVectorized check doesn't panic
// on CSVWriter processor.
func TestExportVectorized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `SET vectorize_row_count_threshold=0`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'http://0.1:37957/exp_1' FROM TABLE t WITH data_only`)
}

func TestExportNewOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `create table foo (i int8 primary key, x int8, y int8, z int8, index (y))`)
	sqlDB.Exec(t, `insert into foo values (2, 22, 2, 24), (1, 12, 3, 14), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/data' from table foo with data_only`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/meta' from table foo with meta_only`)
	// data_only
	t.Run("data_only", func(t *testing.T) {
		content, err := ioutil.ReadFile(filepath.Join(dir, "data", "n1.0.csv"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = ioutil.ReadFile(filepath.Join(dir, "data", "meta.sql"))
		if _, ok := err.(*os.PathError); !ok && err != nil {
			t.Fatal(err)
		}
		if expected, got := "1,12,3,14\n2,22,2,24\n3,32,1,34\n", string(content); expected != got {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	})

	t.Run("meta_only", func(t *testing.T) {
		schema, err := ioutil.ReadFile(filepath.Join(dir, "meta", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = ioutil.ReadFile(filepath.Join(dir, "meta", "n1.0.csv"))
		if _, ok := err.(*os.PathError); !ok && err != nil {
			t.Fatal(err)
		}
		if expected, got := "CREATE TABLE foo (\n\ti INT8 NOT NULL,\n\tx INT8 NULL,\n\ty INT8 NULL,\n\tz INT8 NULL,\n\tCONSTRAINT \"primary\" PRIMARY KEY (i ASC),\n\tINDEX foo_y_idx (y ASC),\n\tFAMILY \"primary\" (i, x, y, z)\n)", string(schema); expected != got {
			t.Fatalf("expected %q, got %q", expected, got)
		}
	})
}

func TestExportToSpecificNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO t VALUES (1),(2),(3)`)

	t.Run(`node id = self `, func(t *testing.T) {
		_, err := db.Query("EXPORT INTO CSV 'nodelocal://self/t' FROM TABLE t")
		require.EqualError(t, err,
			`pq: please enter a valid node ID (don't use 'self' or '0' as node ID)`)
	})
	t.Run(`node id = 0 `, func(t *testing.T) {
		_, err := db.Query("EXPORT INTO CSV 'nodelocal://0/t' FROM TABLE t")
		require.EqualError(t, err,
			`pq: please enter a valid node ID (don't use 'self' or '0' as node ID)`)
	})
	t.Run(`node id = 000000 `, func(t *testing.T) {
		_, err := db.Query("EXPORT INTO CSV 'nodelocal://000000/t' FROM TABLE t")
		require.EqualError(t, err,
			`pq: please enter a valid node ID (don't use 'self' or '0' as node ID)`)
	})
}
