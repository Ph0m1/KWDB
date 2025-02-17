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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func checkDatabaseSql(t *testing.T, extIoDir, dbName string) {
	dbPath := filepath.Join(extIoDir, dbName)
	metaPath := strings.Join([]string{dbPath, "meta.sql"}, "/")
	content, err := ioutil.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "CREATE DATABASE "+dbName+";" {
		t.Fatal(fmt.Errorf("invalid content of %s.sql", dbName))
	}
}

func checkDatabaseSqlWithComment(t *testing.T, extIoDir, dbName string) {
	dbPath := filepath.Join(extIoDir, dbName)
	metaPath := strings.Join([]string{dbPath, "meta.sql"}, "/")
	content, err := ioutil.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	str := "COMMENT ON"
	if !strings.Contains(string(content), str) {
		t.Fatal(fmt.Errorf("no comment statement was found"))
	}
	dbSQL := "CREATE DATABASE " + dbName + ";\n" + "COMMENT ON DATABASE " + dbName + " IS 'DB for comment';"
	contentStr := string(content)
	if contentStr != dbSQL {
		t.Fatal(fmt.Errorf("invalid content of %s.sql", dbName))
	}
}

func checkSchemaSql(t *testing.T, dbDir, schemaName string) {
	schemaDir := filepath.Join(dbDir, schemaName)
	metaPath := strings.Join([]string{schemaDir, "meta.sql"}, "/")
	content, err := ioutil.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	stmt := "CREATE SCHEMA " + schemaName + ";\n"
	contentStr := string(content)
	if contentStr != stmt {
		t.Fatal(fmt.Errorf("invalid content of schema %s", schemaName))
	}
}

func checkTableSql(t *testing.T, schemaDir, tblName, createStmt string) {
	tblPath := filepath.Join(schemaDir, tblName)
	metaPath := strings.Join([]string{tblPath, "meta.sql"}, "/")
	content, err := ioutil.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	// check meta.sql
	if createStmt != string(content) {
		t.Logf("meta.sql=`%s`, createStmt=`%s`", string(content), createStmt)
	}
}

func checkTableSqlWithComment(
	t *testing.T, schemaDir, tblName, createStmt string, CheckComment bool,
) {
	tblPath := filepath.Join(schemaDir, tblName)
	metaPath := strings.Join([]string{tblPath, "meta.sql"}, "/")
	content, err := ioutil.ReadFile(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if CheckComment {
		str := "COMMENT ON"
		if !strings.Contains(string(content), str) {
			t.Fatal(fmt.Errorf("no comment statement was found"))
		}
	}
	// check meta.sql
	if createStmt+";" != string(content) {
		t.Logf("meta.sql=`%s`, createStmt=`%s`", string(content), createStmt)
	}
}

func checkTableCsv(t *testing.T, schemaDir, tblName string, lines []string) {
	tblPath := filepath.Join(schemaDir, tblName)
	content := ""
	err := filepath.Walk(tblPath, func(path string, info os.FileInfo, err error) error {
		fmt.Printf("path = %s\n", path)
		if info.IsDir() {
			t.Logf("%s is a directory", path)
			return nil
		}
		if strings.Index(info.Name(), ".csv") != -1 {
			infoPath := strings.Join([]string{tblPath, info.Name()}, "/")
			bytes, err := ioutil.ReadFile(infoPath)
			if err != nil {
				t.Fatal(err)
				return err
			}
			content = content + string(bytes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, line := range lines {
		if strings.Index(content, line) == -1 {
			t.Fatal(fmt.Errorf("row data `%s` not found", line))
		}
	}
}

// Export an empty database
func TestExportEmptyDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, fmt.Sprintf(`CREATE DATABASE %s`, database))

	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))
	checkDatabaseSql(t, dir, database)
}

// Export the database of one table
func TestExportDatabaseSingleTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])

	// check csv
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table, lines)
}

// Export the database of two tables
func TestExportDatabaseMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table1 := "t1"
	table2 := "t2"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table1, cols[0], cols[1], cols[2]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table2, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table1, i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table2, i, i*i, i+1))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))

	// check sql
	tblStmt1 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table1))
	tblStmt2 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table2))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table1, tblStmt1[0][0])
	checkTableSql(t, filepath.Join(dir, database, "public"), table2, tblStmt2[0][0])

	// check csv
	rowdata1 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table1))
	var lines []string
	for _, line := range rowdata1 {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table1, lines)

	rowdata2 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table2))
	lines = lines[:0]
	for _, line := range rowdata2 {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table2, lines)
}

// Export database with the 'with meta_only' option
func TestExportDatabaseOnlyMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH meta_only`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])
}

// Export database with the 'with data_only' option
func TestExportDatabaseOnlyData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH data_only`, database, database))

	// check sql
	sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	// check csv
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table, lines)
}

// export database with col_name
func TestExportDatabaseWithColName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH column_name`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])
}

// export database with chunk_rows=0
func TestExportDatabaseWithChunk_rows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH chunk_rows='0'`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])
}

// Export multiple CSV files from the database (with chunk_rows)
func TestExportDatabaseMultiCsv(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 100; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH chunk_rows = '13'`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])

	// check csv
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table, lines)
}

// Create one table in the database before obtaining the table list when exporting the database
func TestCreateTableBeforeExecute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	var sqlDB *sqlutils.SQLRunner
	beforeExec := func(ctx context.Context, stmt string) {
		if strings.Contains(stmt, "EXPORT INTO CSV") &&
			strings.Contains(stmt, "FROM") &&
			strings.Contains(stmt, "DATABASE") &&
			strings.Contains(stmt, database) {
			if sqlDB != nil {
				fmt.Println("callback exec: ", stmt)
				// TODO: create table before getTableNames
				sqlDB.Query(t, fmt.Sprintf("CREATE TABLE t2 (%s INT, %s VARCHAR)", cols[0], cols[1]))
				sqlDB.Query(t, fmt.Sprintf("INSERT INTO t2 VALUES (1, '1')"))
				sqlDB.Query(t, fmt.Sprintf("INSERT INTO t2 VALUES (2, '2')"))
			}

			rows := sqlDB.QueryStr(t, `SHOW TABLES`)
			fmt.Println(rows)
			rows = sqlDB.QueryStr(t, `SELECT * FROM t2`)
			fmt.Println(rows)
		}
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeGetTablesName: beforeExec,
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB = sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// get table create statement
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	// get table data
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}

	time.Sleep(time.Second * 3)
	rows := sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))
	for _, line := range rows {
		for _, col := range line {
			fmt.Println("col =", col)
		}
	}

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])
	checkTableCsv(t, filepath.Join(dir, database, "public"), table, lines)
}

// Delete one table from the database before obtaining the table list when exporting the database
func TestDropTableBeforeExecute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	var sqlDB *sqlutils.SQLRunner
	beforeExec := func(ctx context.Context, stmt string) {
		if strings.Contains(stmt, "EXPORT INTO CSV") &&
			strings.Contains(stmt, "FROM") &&
			strings.Contains(stmt, "DATABASE") &&
			strings.Contains(stmt, database) {
			if sqlDB != nil {
				fmt.Println("callback exec: ", stmt)
				// TODO: drop table before getTableNames
				sqlDB.Query(t, fmt.Sprintf("DROP TABLE %s", table))
			}

			rows := sqlDB.QueryStr(t, `SHOW TABLES`)
			fmt.Println(rows)
		}
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeGetTablesName: beforeExec,
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB = sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// get table create statement
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	// get table data
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}

	time.Sleep(time.Second * 3)
	rows := sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))
	for _, line := range rows {
		for _, col := range line {
			fmt.Println("col =", col)
		}
	}

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0])
	checkTableCsv(t, filepath.Join(dir, database, "public"), table, lines)
}

// Constructing an exception while exporting the database caused one of the tables to fail export
func TestPartTableExportFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	tables := []string{"t1", "t2"}
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[0], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[1], cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[0], i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[1], i, i*i, i+1))
	}

	var sqlDB *sqlutils.SQLRunner
	injectError := func(ctx context.Context, name string) error {
		if name == tables[0] {
			fmt.Println("error: failed to export table", name)
			return fmt.Errorf("injected error for table: %s", name)
		}
		return nil
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				InjectErrorInConstructExport: injectError,
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB = sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// get table create statement
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, tables[1]))

	// get table data
	rowdata := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, tables[1]))
	var lines []string
	for _, line := range rowdata {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}

	time.Sleep(time.Second * 3)
	rows := sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))
	for _, line := range rows {
		for _, col := range line {
			fmt.Printf("%s\t", col)
		}
		fmt.Println("")
	}

	checkDatabaseSql(t, dir, database)
	checkTableSql(t, filepath.Join(dir, database, "public"), tables[1], tblStmt[0][0])
	checkTableCsv(t, filepath.Join(dir, database, "public"), tables[1], lines)
}

// Constructing an exception while exporting the database resulted in the failure of exporting all tables
func TestAllTableExportFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	tables := []string{"t1", "t2"}
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[0], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[1], cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[0], i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[1], i, i*i, i+1))
	}

	var sqlDB *sqlutils.SQLRunner
	injectError := func(ctx context.Context, name string) error {
		if name == tables[0] || name == tables[1] {
			fmt.Println("error: failed to export table", name)
			return fmt.Errorf("injected error for table: %s", name)
		}
		return nil
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				InjectErrorInConstructExport: injectError,
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB = sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	time.Sleep(time.Second * 3)
	rows := sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))
	for _, line := range rows {
		for _, col := range line {
			fmt.Printf("%s\t", col)
		}
		fmt.Println("")
	}

	checkDatabaseSql(t, dir, database)
}

// The exported database contains two schemas, public and schema1, each containing one table
func TestExportDatabaseWithNonpublicSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	schema := "schema1"
	table1 := "t1"
	table2 := "t2"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s (%s INT, %s VARCHAR, %s INT)`, table1, cols[0], cols[1], cols[2]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, schema, table2, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s VALUES (%d, '%d', %d)`, table1, i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, schema, table2, i, i*i, i+1))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))

	// check sql
	tblStmt1 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table1))
	tblStmt2 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s.%s]`, schema, table2))

	checkDatabaseSql(t, dir, database)
	// There will be no meta.sql of public schema
	checkSchemaSql(t, filepath.Join(dir, database), schema)
	checkTableSql(t, filepath.Join(dir, database, "public"), table1, tblStmt1[0][0])
	checkTableSql(t, filepath.Join(dir, database, schema), table2, tblStmt2[0][0])

	// check csv
	rowdata1 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s LIMIT 10`, table1))
	var lines []string
	for _, line := range rowdata1 {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, "public"), table1, lines)

	rowdata2 := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT * FROM %s.%s LIMIT 10`, schema, table2))
	lines = lines[:0]
	for _, line := range rowdata2 {
		l := ""
		for _, col := range line {
			l = l + col + ","
		}
		lines = append(lines, l[:len(l)-1])
	}
	checkTableCsv(t, filepath.Join(dir, database, schema), table2, lines)
}

// The exported database contains three schemas: public and schema1/schema2, all of which do not contain any tables
func TestExportDatabaseWithMultiEmptySchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	schema1 := "schema1"
	schema2 := "schema2"
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema1),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema2),
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s`, database, database))

	checkDatabaseSql(t, dir, database)
	// There will be no meta.sql of empty schema
	// checkSchemaSql(t, filepath.Join(dir, database), schema1)
	// checkSchemaSql(t, filepath.Join(dir, database), schema2)
}

// Import files with schema for testing
func TestImportSchemaRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	schema := "sc"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`USE %s`, database),
		fmt.Sprintf(`CREATE SCHEMA %s`, schema),
		fmt.Sprintf(`SET SEARCH_PATH TO %s`, schema),
		fmt.Sprintf(`CREATE TABLE %s (%s INT, %s VARCHAR, %s INT)`, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s VALUES (%d, '%d', %d)`, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database with comment for DB
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM TABLE %s`, database, table))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop table %s`, table))
	_, err := sqlDB.DB.ExecContext(context.Background(), fmt.Sprintf(`import table create using "nodelocal://1/%s/meta.sql" csv 
  data ("nodelocal://1/%s");`, database, database))
	str := "pq: expected a file but \"testdb\" is a directory"
	if err.Error() != str {
		t.Fatal(fmt.Errorf("err is not expected, err is  %s", err.Error()))
	}
}

// Export the database of one table with comment
func TestExportDBWithCommentForDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`COMMENT ON DATABASE %s IS 'DB for comment'`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database with comment for DB
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH COMMENT`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSqlWithComment(t, dir, database)
	checkTableSqlWithComment(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0], false)
	sqlDB.QueryStr(t, fmt.Sprintf(`use defaultdb`))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop database %s`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT DATABASE CSV DATA ('nodelocal://1/%s') with comment`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW DATABASES WITH COMMENT;`))
	str := "DB for comment"
	if !strings.Contains(string(tblStmt[3][2]), str) {
		t.Fatal(fmt.Errorf("no comment statement was found %s", tblStmt))
	}
}

// Export the database of one table with comment
func TestExportDBWithComment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`COMMENT ON DATABASE %s IS 'DB for comment'`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
		fmt.Sprintf(`comment on table %s.%s is 'table for comment'`, database, table),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, table, cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database with comment for DB
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH COMMENT`, database, database))

	// check sql
	tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))

	checkDatabaseSqlWithComment(t, dir, database)
	checkTableSqlWithComment(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0], true)
	sqlDB.QueryStr(t, fmt.Sprintf(`use defaultdb`))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop database %s`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT DATABASE CSV DATA ('nodelocal://1/%s') WITH COMMENT`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW DATABASES WITH COMMENT;`))
	str := "DB for comment"
	if !strings.Contains(string(tblStmt[3][2]), str) {
		t.Fatal(fmt.Errorf("no comment statement was found %s", tblStmt))
	}
	str = "COMMENT ON"
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
	if !strings.Contains(tblStmt[0][0], str) {
		t.Fatal(fmt.Errorf("no comment statement was found"))
	}
}

// Export the database of one table with comment
func TestExportDBWithCommentForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	tables := []string{"t1", "t2"}
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[0], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`comment on table %s.%s is 'table for comment'`, database, tables[0]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[1], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, tables[1], cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[0], i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[1], i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export database with comment for table
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH COMMENT`, database, database))

	// check sql
	checkDatabaseSql(t, dir, database)
	for _, table := range tables {
		tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		checkTableSqlWithComment(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0], true)
	}
	sqlDB.QueryStr(t, fmt.Sprintf(`use defaultdb`))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop database %s`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT DATABASE CSV DATA ('nodelocal://1/%s') WITH COMMENT`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	str := "COMMENT ON"
	for _, table := range tables {
		tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		if !strings.Contains(tblStmt[0][0], str) {
			t.Fatal(fmt.Errorf("no comment statement was found"))
		}
	}
}

// Export one table with comment
func TestExportTableWithCommentForTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	database := "testdb"
	//table := "t1"
	tables := []string{"t1", "t2"}
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE DATABASE %s`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[0], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`comment on table %s.%s is 'table for comment'`, database, tables[0]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[1], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, tables[1], cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[0], i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[1], i+1, (i+1)*(i+1), i))
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
		UseDatabase:   database,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export table with comment for table and column
	str := "COMMENT ON"
	for _, table := range tables {
		sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM TABLE %s WITH COMMENT`, table, table))
		// check sql
		tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		checkTableSqlWithComment(t, filepath.Join(dir), table, tblStmt[0][0], true)
		sqlDB.QueryStr(t, fmt.Sprintf(`drop table %s`, table))
		sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT TABLE CREATE USING 'nodelocal://1/%s/meta.sql' CSV DATA ('nodelocal://1/%s/n1.0.csv') with comment`, table, table))
		tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		if !strings.Contains(tblStmt[0][0], str) {
			t.Fatal(fmt.Errorf("no comment statement was found"))
		}
	}
}

// Export one table with comment in the cluster
func TestExpDBWithCommentForCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sqlDB, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	database := "test"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`COMMENT ON DATABASE %s IS 'DB for comment'`, database),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
		fmt.Sprintf(`comment on table %s.%s is 'table for comment'`, database, table),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, table, cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}
	// export database with comment for DB
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH COMMENT`, database, database))

	// check sql
	checkDatabaseSqlWithComment(t, dir, database)
	var tblStmt [][]string
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
	checkTableSqlWithComment(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0], true)

	sqlDB.QueryStr(t, fmt.Sprintf(`use defaultdb`))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop database %s cascade`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT DATABASE CSV DATA ('nodelocal://1/%s') WITH COMMENT`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW DATABASES WITH COMMENT;`))
	str := "DB for comment"
	if !strings.Contains(string(tblStmt[3][2]), str) {
		t.Fatal(fmt.Errorf("no comment statement was found %s", tblStmt))
	}
	str = "COMMENT ON"
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
	if !strings.Contains(tblStmt[0][0], str) {
		t.Fatal(fmt.Errorf("no comment statement was found"))
	}
}

// Export one table with comment in the cluster
func TestExpDBWithCommentForClusterAndCol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sqlDB, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	database := "test"
	table := "t1"
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, table, cols[0], cols[1], cols[2]),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, table, cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, table, i+1, (i+1)*(i+1), i))
	}

	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}
	// export database with comment for DB
	sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM DATABASE %s WITH COMMENT`, database, database))

	// check sql
	var tblStmt [][]string
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
	checkTableSqlWithComment(t, filepath.Join(dir, database, "public"), table, tblStmt[0][0], true)

	sqlDB.QueryStr(t, fmt.Sprintf(`use defaultdb`))
	sqlDB.QueryStr(t, fmt.Sprintf(`drop database %s cascade`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT DATABASE CSV DATA ('nodelocal://1/%s') WITH COMMENT`, database))
	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SHOW DATABASES WITH COMMENT;`))
	str := "COMMENT ON"
	tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
	if !strings.Contains(tblStmt[0][0], str) {
		t.Fatal(fmt.Errorf("no comment statement was found"))
	}
}

// Export one table with comment in the cluster
func TestExpTblWithCommentForCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sqlDB, dir, cleanup := setupExportableBank(t, 3, 100)
	defer cleanup()

	database := "test"
	tables := []string{"t1", "t2"}
	cols := []string{"num", "square", "extra"}
	stmt := []string{
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[0], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`comment on table %s.%s is 'table for comment'`, database, tables[0]),
		fmt.Sprintf(`CREATE TABLE %s.%s (%s INT, %s VARCHAR, %s INT)`, database, tables[1], cols[0], cols[1], cols[2]),
		fmt.Sprintf(`COMMENT ON COLUMN %s.%s.%s IS 'first column for comment'`, database, tables[1], cols[0]),
	}
	for i := 0; i < 10; i++ {
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[0], i+1, (i+1)*(i+1), i))
		stmt = append(stmt, fmt.Sprintf(`INSERT INTO %s.%s VALUES (%d, '%d', %d)`, database, tables[1], i+1, (i+1)*(i+1), i))
	}

	sqlDB.QueryStr(t, fmt.Sprintf(`use %s`, database))
	for _, s := range stmt {
		fmt.Println("exec: ", s)
		sqlDB.Exec(t, s)
	}

	// export DB with comment for column
	str := "COMMENT ON"
	for _, table := range tables {
		row := sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%s' FROM TABLE %s WITH COMMENT`, table, table))
		nodeID, err := strconv.Atoi(row[0][2])
		if err != nil {
			t.Fatal(err)
		}
		// check sql
		tblStmt := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		checkTableSqlWithComment(t, filepath.Join(dir), table, tblStmt[0][0], true)
		sqlDB.QueryStr(t, fmt.Sprintf(`drop table %s`, table))
		sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT TABLE CREATE USING 'nodelocal://1/%s/meta.sql' CSV DATA ('nodelocal://1/%s/n%d.0.csv') with comment`, table, table, nodeID))
		tblStmt = sqlDB.QueryStr(t, fmt.Sprintf(`SELECT create_statement FROM [SHOW CREATE %s]`, table))
		if !strings.Contains(tblStmt[0][0], str) {
			t.Fatal(fmt.Errorf("no comment statement was found"))
		}
	}
}
