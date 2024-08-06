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

package test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestAuditStrategy(t *testing.T) {

	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	strategyLen0 := len(ae0.AuditsOfWithOption(context.TODO(), nil))

	setAuditStmt := "set cluster setting audit.enabled = true;"
	createAuditStmt := "create audit createDatabase ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createDatabase enable;"

	createStmt0 := "create table t0(a int)"
	createStmt1 := "create table t1(a int)"
	createStmt2 := "create table t2(a int)"

	audDB.Exec(t, setAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)
	audDB.Exec(t, createStmt0)

	// test CREATE DATABASE on node1

	strategyExpr := strategyLen0 + 1
	time.Sleep(1000)
	strategyLen0 = len(ae0.AuditsOfWithOption(context.TODO(), nil))
	if strategyExpr != strategyLen0 {
		t.Errorf("expected audit strategy count:%d, but got %d", strategyExpr, strategyLen0)
	}

	audDB1 := sqlutils.MakeSQLRunner(testCluster.ServerConn(1))

	strategyLen1 := len(ae1.AuditsOfWithOption(context.TODO(), nil))
	createAuditStmt = "create audit createtable ON table FOR CREATE TO root;"
	enableAuditStmt = "ALTER AUDIT createtable enable;"

	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)
	audDB1.Exec(t, createStmt1)

	time.Sleep(1000)
	strategyExpr = strategyLen1 + 1
	strategyLen1 = len(ae1.AuditsOfWithOption(context.TODO(), nil))

	if strategyExpr != strategyLen1 {
		t.Errorf("expected audit strategy count:%d, but got %d", strategyExpr, strategyLen1)
	}

	audDB2 := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))
	strategyLen2 := len(ae2.AuditsOfWithOption(context.TODO(), nil))
	createAuditStmt = "create audit createview ON view FOR CREATE TO root;"
	enableAuditStmt = "ALTER AUDIT createview enable;"

	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)
	audDB2.Exec(t, createStmt2)
	time.Sleep(1000)
	strategyExpr = strategyLen2 + 1
	strategyLen2 = len(ae2.AuditsOfWithOption(context.TODO(), nil))
	if strategyExpr != strategyLen2 {
		t.Errorf("expected audit strategy count:%d, but got %d", strategyExpr, strategyLen2)
	}
}

func TestDatabaseClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)
	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createDatabaseMetric0 := ae0.GetMetric(target.ObjectDatabase)
	createDatabaseMetric1 := ae1.GetMetric(target.ObjectDatabase)
	createDatabaseMetric2 := ae2.GetMetric(target.ObjectDatabase)
	createDatabaseExpect := createDatabaseMetric0.Count(target.Create)
	createDatabaseExpect1 := createDatabaseMetric1.Count(target.Create)
	createDatabaseExpect2 := createDatabaseMetric2.Count(target.Create)

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	openAuditLogStmt := "SET CLUSTER SETTING audit.log.enabled=true"
	createAuditStmt := "create audit createDatabase ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createDatabase enable;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, openAuditLogStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)

	time.Sleep(1 * time.Second)
	// test CREATE DATABASE on node1
	createDatabaseExpect++
	if _, err := sqlDB0.Exec("CREATE DATABASE test1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	//todo: audit add condition need to fix below operation
	frequency := createDatabaseMetric0.Count(target.Create)
	if frequency != createDatabaseExpect {
		t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
	}

	// test CREATE DATABASE on node2
	createDatabaseExpect1++

	if _, err := sqlDB1.Exec("CREATE DATABASE test2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	createDatabaseMetric1 = ae1.GetMetric(target.ObjectDatabase)
	frequency = createDatabaseMetric1.Count(target.Create)

	if frequency != createDatabaseExpect1 {
		t.Errorf("expected db count:%d, but got %d", createDatabaseExpect1, frequency)
	}

	// test CREATE DATABASE on node3
	createDatabaseExpect2++

	if _, err := sqlDB2.Exec("CREATE DATABASE test3"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	createDatabaseMetric2 = ae2.GetMetric(target.ObjectDatabase)
	frequency = createDatabaseMetric2.Count(target.Create)
	if frequency != createDatabaseExpect2 {
		t.Errorf("expected db count:%d, but got %d", createDatabaseExpect, frequency)
	}

	audDB.Exec(t, closeAuditStmt)
}

func TestTableClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)
	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createTableMetric0 := ae0.GetMetric(target.ObjectTable)
	createTableMetric1 := ae1.GetMetric(target.ObjectTable)
	createTableMetric2 := ae2.GetMetric(target.ObjectTable)

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"

	createAuditStmt := "create audit createTable ON TABLE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createTable enable;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)

	// test CREATE TABLE on node1
	time.Sleep(1 * time.Second)
	createTableExpect0 := createTableMetric0.Count(target.Create)
	createTableExpect0++
	if _, err := sqlDB0.Exec("CREATE TABLE ttable1 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency := createTableMetric0.Count(target.Create)
	if frequency != createTableExpect0 {
		t.Errorf("expected table count:%d, but got %d", createTableExpect0, frequency)
	}

	// test CREATE TABLE on node2
	createTableExpect1 := createTableMetric1.Count(target.Create)
	createTableExpect1++
	if _, err := sqlDB1.Exec("CREATE TABLE ttable2 (id INT)"); err != nil {
		t.Fatal(err)
	}
	frequency = createTableMetric1.Count(target.Create)
	if frequency != createTableExpect1 {
		t.Errorf("expected db count:%d, but got %d", createTableExpect1, frequency)
	}

	// test CREATE TABLE on node3
	createTableExpect2 := createTableMetric2.Count(target.Create)
	createTableExpect2++
	if _, err := sqlDB2.Exec("CREATE TABLE ttable3 (id INT)"); err != nil {
		t.Fatal(err)
	}

	frequency = createTableMetric2.Count(target.Create)
	if frequency != createTableExpect2 {
		t.Errorf("expected table count:%d, but got %d", createTableExpect2, frequency)
	}

	audDB.Exec(t, closeAuditStmt)
}

func TestIndexClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	createIndexMetric0 := ae0.GetMetric(target.ObjectIndex)
	createIndexMetric1 := ae1.GetMetric(target.ObjectIndex)
	createIndexMetric2 := ae2.GetMetric(target.ObjectIndex)

	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"
	createAuditStmt := "create audit createIndex ON INDEX FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createIndex enable;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)
	// prepare table
	if _, err := sqlDB0.Exec("CREATE TABLE ttable (id INT, gender STRING, name STRING, address STRING, tel STRING)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	// test CREATE INDEX on node1
	createIndexExpect0 := createIndexMetric0.Count(target.Create)
	createIndexExpect0++
	if _, err := sqlDB0.Exec("CREATE INDEX ON ttable (id)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency := createIndexMetric0.Count(target.Create)
	if frequency != createIndexExpect0 {
		t.Errorf("expected db count:%d, but got %d", createIndexExpect0, frequency)
	}

	// test CREATE INDEX on node2
	createIndexExpect1 := createIndexMetric1.Count(target.Create)
	createIndexExpect1++
	if _, err := sqlDB1.Exec("CREATE INDEX ON ttable (gender)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency = createIndexMetric1.Count(target.Create)
	if frequency != createIndexExpect1 {
		t.Errorf("expected db count:%d, but got %d", createIndexExpect1, frequency)
	}

	// test CREATE INDEX on node3
	createIndexExpect2 := createIndexMetric2.Count(target.Create)
	createIndexExpect2++
	if _, err := sqlDB2.Exec("CREATE INDEX ON ttable (name)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency = createIndexMetric2.Count(target.Create)
	if frequency != createIndexExpect2 {
		t.Errorf("expected db count:%d, but got %d", createIndexExpect2, frequency)
	}
	audDB.Exec(t, closeAuditStmt)
}

func TestViewClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"
	createAuditStmt := "create audit createView ON VIEW FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createView enable;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)

	createViewMetric0 := ae0.GetMetric(target.ObjectView)
	createViewMetric1 := ae1.GetMetric(target.ObjectView)
	createViewMetric2 := ae2.GetMetric(target.ObjectView)

	// prepare table
	if _, err := sqlDB0.Exec("CREATE TABLE customers (id INT, name STRING, age INT, address STRING, salary INT)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	createViewExpect0 := createViewMetric0.Count(target.Create)
	createViewExpect0++
	if _, err := sqlDB0.Exec("CREATE VIEW customers_view1 AS SELECT id FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency := createViewMetric0.Count(target.Create)
	if frequency != createViewExpect0 {
		t.Errorf("expected db count:%d, but got %d", createViewExpect0, frequency)
	}

	createViewExpect1 := createViewMetric1.Count(target.Create)
	createViewExpect1++
	if _, err := sqlDB1.Exec("CREATE VIEW customers_view2 AS SELECT name FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency = createViewMetric1.Count(target.Create)
	if frequency != createViewExpect1 {
		t.Errorf("expected db count:%d, but got %d", createViewExpect1, frequency)
	}

	createViewExpect2 := createViewMetric2.Count(target.Create)
	createViewExpect2++
	if _, err := sqlDB2.Exec("CREATE VIEW customers_view3 AS SELECT age FROM customers"); err != nil {
		t.Fatal(err)
	}
	frequency = createViewMetric2.Count(target.Create)
	if frequency != createViewExpect2 {
		t.Errorf("expected db count:%d, but got %d", createViewExpect2, frequency)
	}
	audDB.Exec(t, closeAuditStmt)
}

func TestSequenceClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	sqlDB0 := testCluster.ServerConn(0)
	sqlDB1 := testCluster.ServerConn(1)
	sqlDB2 := testCluster.ServerConn(2)
	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"

	createAuditStmt := "create audit createSequence ON SEQUENCE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createSequence enable;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)

	time.Sleep(1 * time.Second)

	createSequenceMetric0 := ae0.GetMetric(target.ObjectSequence)
	createSequenceMetric1 := ae1.GetMetric(target.ObjectSequence)
	createSequenceMetric2 := ae2.GetMetric(target.ObjectSequence)

	createSequenceExpect0 := createSequenceMetric0.Count(target.Create)
	createSequenceExpect0++
	if _, err := sqlDB0.Exec("CREATE SEQUENCE customer_seq1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency := createSequenceMetric0.Count(target.Create)
	if frequency != createSequenceExpect0 {
		t.Errorf("expected db count:%d, but got %d", createSequenceExpect0, frequency)
	}
	createSequenceExpect1 := createSequenceMetric1.Count(target.Create)
	createSequenceExpect1++
	if _, err := sqlDB1.Exec("CREATE SEQUENCE customer_seq2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency = createSequenceMetric1.Count(target.Create)
	if frequency != createSequenceExpect1 {
		t.Errorf("expected db count:%d, but got %d", createSequenceExpect1, frequency)
	}

	createSequenceExpect2 := createSequenceMetric2.Count(target.Create)
	createSequenceExpect2++
	if _, err := sqlDB2.Exec("CREATE SEQUENCE customer_seq3"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	frequency = createSequenceMetric2.Count(target.Create)
	if frequency != createSequenceExpect2 {
		t.Errorf("expected db count:%d, but got %d", createSequenceExpect2, frequency)
	}
	audDB.Exec(t, closeAuditStmt)
}

func TestRoleClusterMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	server0 := testCluster.Server(0)
	server1 := testCluster.Server(1)
	server2 := testCluster.Server(2)

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	ae1 := (server1.AuditServer().GetHandler()).(*event.AuditEvent)
	ae2 := (server2.AuditServer().GetHandler()).(*event.AuditEvent)

	audDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	secDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	secDB1 := sqlutils.MakeSQLRunner(testCluster.ServerConn(1))
	secDB2 := sqlutils.MakeSQLRunner(testCluster.ServerConn(2))

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	closeAuditStmt := "SET CLUSTER SETTING audit.enabled=false;"

	createAuditStmt := "create audit createRole ON ROLE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createRole enable;"

	audDB.Exec(t, openAuditStmt)
	audDB.Exec(t, createAuditStmt)
	audDB.Exec(t, enableAuditStmt)

	time.Sleep(3 * time.Second)

	createRoleMetric0 := ae0.GetMetric(target.ObjectRole)
	createRoleMetric1 := ae1.GetMetric(target.ObjectRole)
	createRoleMetric2 := ae2.GetMetric(target.ObjectRole)
	createRoleExpect0 := createRoleMetric0.Count(target.Create)
	createRoleExpect1 := createRoleMetric1.Count(target.Create)
	createRoleExpect2 := createRoleMetric2.Count(target.Create)

	createRoleExpect0++
	secDB.Exec(t, "CREATE ROLE r1")

	time.Sleep(1 * time.Second)
	createRoleMetric0 = ae0.GetMetric(target.ObjectRole)
	frequency := createRoleMetric0.Count(target.Create)
	if frequency != createRoleExpect0 {
		t.Errorf("expected db count:%d, but got %d", createRoleExpect0, frequency)
	}

	createRoleExpect1++
	secDB1.Exec(t, "CREATE ROLE r2")
	time.Sleep(1 * time.Second)
	frequency = createRoleMetric1.Count(target.Create)
	if frequency != createRoleExpect1 {
		t.Errorf("expected db count:%d, but got %d", createRoleExpect1, frequency)
	}

	createRoleExpect2++
	secDB2.Exec(t, "CREATE ROLE r3")
	time.Sleep(1 * time.Second)
	frequency = createRoleMetric2.Count(target.Create)
	if frequency != createRoleExpect2 {
		t.Errorf("expected db count:%d, but got %d", createRoleExpect2, frequency)
	}

	audDB.Exec(t, closeAuditStmt)
}
