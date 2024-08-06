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
	"fmt"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	auditSever "gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	_ "gitee.com/kwbasedb/kwbase/pkg/sql/gcjob"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

func TestAuditServer_test(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	//ac := log.AmbientContext{Tracer: s.ClusterSettings().Tracer}
	//_, _ := ac.AnnotateCtxWithSpan(context.Background(), "test")

	if _, err := db.Exec("CREATE DATABASE db1"); err != nil {
		t.Fatal(err)
	}
}

// Test function IsAudit
func TestAuditServer_IsAudit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec("set cluster setting audit.enabled = true;"); err != nil {
		t.Fatal(err)
	}
	res, err := db.Exec("CREATE DATABASE db1")
	result := auditSever.ExecSuccess
	if err != nil {
		result = auditSever.ExecFail
	}
	db1id, _ := res.LastInsertId()
	rowsAff, _ := res.RowsAffected()
	defer s.Stopper().Stop(context.TODO())
	audTest := auditSever.AuditInfo{
		EventTime: timeutil.Now(),
		User: &auditSever.User{
			Username: security.RootUser,
			Roles:    []auditSever.Role{{Name: "admin"}},
		},
		Event: target.Create,
		Level: 1,
		Client: &auditSever.ClientInfo{
			AppName: "testServer",
			Address: "127.0.0.1:20000",
		},
		Target: &auditSever.TargetInfo{
			Typ: target.ObjectDatabase,
			Targets: map[uint32]auditSever.Target{
				uint32(db1id): {ID: uint32(db1id), Name: "db1"},
			},
		},
		Result:   &auditSever.ResultInfo{Status: result, RowsAffected: int(rowsAff)},
		Command:  &auditSever.Command{Cmd: "CREATE DATABASE db1"},
		Reporter: &auditSever.Reporter{},
	}

	audTest.SetAuditLevel(0)
	if !s.AuditServer().IsAudit(&audTest) {
		t.Fatalf("expected %s audit for AUDIT SERVER , got %s", "true", "false")
	}
	//stmt audit
	audTest.SetAuditLevel(1)
	if s.AuditServer().IsAudit(&audTest) {
		t.Fatalf("expected %s audit for AUDIT SERVER , got %s", "false", "true")
	}
	//object audit
	audTest.SetAuditLevel(2)
	if s.AuditServer().IsAudit(&audTest) {
		t.Fatalf("expected %s audit for AUDIT SERVER , got %s", "false", "true")
	}

	createAuditStmt := "create audit stmttest ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT stmttest enable;"

	if _, err := db.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	audTest.SetAuditLevel(1)
	audTest.User = &auditSever.User{
		Username: security.RootUser,
		Roles:    []auditSever.Role{{Name: "admin"}},
	}
	time.Sleep(1 * time.Second)
	if !s.AuditServer().IsAudit(&audTest) {
		t.Fatalf("expected %s audit for AUDIT SERVER , got %s", "true", "false")
	}
}

func TestDatabaseMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// start test server
	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// TODO need to find a better way
	// waiting for server complete initialization

	// get event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//prepare audit
	createAuditStmt := "create audit stmttest ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT stmttest enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	// prepare table
	if _, err := DB.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	// test rename database
	createDBMetric := ae.GetMetric(target.ObjectDatabase)
	//renameDBMetric := ae.GetMetric(sql.EventLogRenameDatabase)[0].RegisterMetric().(*events.RenameDBMetrics)
	countDBExpect := createDBMetric.Count(target.Create) + 1

	if _, err := DB.Exec("CREATE DATABASE test2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	if createDBMetric.Count(target.Create) != countDBExpect {
		t.Errorf("expected db count:%d, but got %d", countDBExpect, createDBMetric.Count(target.Create))
	}

}

func TestSchemaMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// start test server
	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// TODO need to find a better way
	// waiting for server complete initialization

	// get event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//prepare audit
	createAuditStmt := "create audit schematest ON SCHEMA FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT schematest enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	// prepare schema
	if _, err := DB.Exec("CREATE SCHEMA test"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	createSchemaMetric := ae.GetMetric(target.ObjectSchema)
	countSchemaExpect := createSchemaMetric.Count(target.Create)
	if createSchemaMetric.Count(target.Create) != countSchemaExpect {
		t.Errorf("expected db count:%d, but got %d", countSchemaExpect, createSchemaMetric.Count(target.Create))
	}

	if _, err := DB.Exec("CREATE SCHEMA test2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	countSchemaExpect = createSchemaMetric.Count(target.Create)
	if createSchemaMetric.Count(target.Create) != countSchemaExpect {
		t.Errorf("expected db count:%d, but got %d", countSchemaExpect, createSchemaMetric.Count(target.Create))
	}

	createAuditStmt = "create audit dropTB ON TABLE FOR DROP TO root;"
	enableAuditStmt = "ALTER AUDIT dropTB enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	// test drop schema

	if _, err := DB.Exec("DROP SCHEMA test"); err != nil {
		t.Fatal(err)
	}

	dropSchemaMetric := ae.GetMetric(target.ObjectSchema)
	dropSchemaExpect := dropSchemaMetric.Count(target.Drop)
	if dropSchemaMetric.Count(target.Drop) != dropSchemaExpect {
		t.Errorf("expected db count:%d, but got %d", dropSchemaExpect, dropSchemaMetric.Count(target.Drop))
	}

	if _, err := DB.Exec("DROP SCHEMA test2"); err != nil {
		t.Fatal(err)
	}

	dropSchemaExpect = dropSchemaMetric.Count(target.Drop)
	if dropSchemaMetric.Count(target.Drop) != dropSchemaExpect {
		t.Errorf("expected db count:%d, but got %d", dropSchemaExpect, dropSchemaMetric.Count(target.Drop))
	}

}

// TODO(xz): waiting for implementation of operations for tables
func TestTableMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit alterTB ON TABLE FOR ALTER TO root;"
	enableAuditStmt := "ALTER AUDIT alterTB enable;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	s.AuditServer().TestSyncConfig()
	time.Sleep(1 * time.Second)

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	if _, err := DB.Exec("CREATE TABLE ttable (id INT)"); err != nil {
		t.Fatal(err)
	}

	// test rename table
	renameTbMetric := ae.GetMetric(target.ObjectTable)
	renameTbExpect := renameTbMetric.Count(target.Alter) + 1

	if _, err := DB.Exec("ALTER TABLE ttable RENAME TO test"); err != nil {
		t.Fatal(err)
	}

	if renameTbMetric.Count(target.Alter) != renameTbExpect {
		t.Errorf("expected db count:%d, but got %d", renameTbExpect, renameTbMetric.Count(target.Alter))
	}

	// test alter table add column
	alterTbMetric := ae.GetMetric(target.ObjectTable)
	alterTbExpect := alterTbMetric.Count(target.Alter) + 1

	if _, err := DB.Exec("ALTER TABLE test ADD COLUMN names STRING"); err != nil {
		t.Fatal(err)
	}

	if alterTbMetric.Count(target.Alter) != alterTbExpect {
		t.Errorf("expected db count:%d, but got %d", alterTbExpect, alterTbMetric.Count(target.Alter))
	}

	createAuditStmt = "create audit truncateTB ON TABLE FOR TRUNCATE TO root;"
	enableAuditStmt = "ALTER AUDIT truncateTB enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	// test truncate table
	truncateTbMetric := ae.GetMetric(target.ObjectTable)
	truncateTbExpect := truncateTbMetric.Count(target.Truncate) + 1

	if _, err := DB.Exec("TRUNCATE test"); err != nil {
		t.Fatal(err)
	}

	if truncateTbMetric.Count(target.Truncate) != truncateTbExpect {
		t.Errorf("expected db count:%d, but got %d", truncateTbExpect, truncateTbMetric.Count(target.Truncate))
	}

	createAuditStmt = "create audit dropTB ON TABLE FOR DROP TO root;"
	enableAuditStmt = "ALTER AUDIT dropTB enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	// test drop table
	dropTbMetric := ae.GetMetric(target.ObjectTable)
	dropTbExpect := dropTbMetric.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP TABLE test"); err != nil {
		t.Fatal(err)
	}

	if dropTbMetric.Count(target.Drop) != dropTbExpect {
		t.Errorf("expected db count:%d, but got %d", dropTbExpect, dropTbMetric.Count(target.Drop))
	}
}

func TestIndexMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit alterIDX ON INDEX FOR ALTER TO root;"
	enableAuditStmt := "ALTER AUDIT alterIDX enable;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	s.AuditServer().TestSyncConfig()
	time.Sleep(1 * time.Second)

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// prepare table and index
	if _, err := DB.Exec("CREATE TABLE ttable (id INT, gender STRING, name STRING, address STRING, tel STRING)"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE INDEX ON ttable (id)"); err != nil {
		t.Fatal(err)
	}

	// test rename index
	renameIDXMetric := ae.GetMetric(target.ObjectIndex)
	renameIDXExpect := renameIDXMetric.Count(target.Alter) + 1

	if _, err := DB.Exec("ALTER INDEX ttable@primary RENAME TO ttable_primary"); err != nil {
		t.Fatal(err)
	}

	if renameIDXMetric.Count(target.Alter) != renameIDXExpect {
		t.Errorf("expected db count:%d, but got %d", renameIDXExpect, renameIDXMetric.Count(target.Alter))
	}

	// drop index
	createAuditStmt = "create audit dropIDX ON INDEX FOR DROP TO root;"
	enableAuditStmt = "ALTER AUDIT dropIDX enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	dropIDXMetric := ae.GetMetric(target.ObjectIndex)
	dropIDXExpect := dropIDXMetric.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP INDEX ttable@ttable_id_idx"); err != nil {
		t.Fatal(err)
	}

	if dropIDXMetric.Count(target.Drop) != dropIDXExpect {
		t.Errorf("expected db count:%d, but got %d", dropIDXExpect, dropIDXMetric.Count(target.Drop))
	}
}

func TestViewMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	createAuditStmt := "create audit dropView ON VIEW FOR DROP TO root;"
	enableAuditStmt := "ALTER AUDIT dropView enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	// prepare table and view
	if _, err := DB.Exec("CREATE TABLE customers (id INT, name STRING, age INT, address STRING, salary INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE VIEW customers_view AS SELECT id FROM customers"); err != nil {
		t.Fatal(err)
	}

	// test drop view
	dropViewMetric := ae.GetMetric(target.ObjectView)
	dropViewExpect := dropViewMetric.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP VIEW customers_view"); err != nil {
		t.Fatal(err)
	}

	if dropViewMetric.Count(target.Drop) != dropViewExpect {
		t.Errorf("expected db count:%d, but got %d", dropViewExpect, dropViewMetric.Count(target.Drop))
	}

}

func TestSequenceMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	createAuditStmt := "create audit alterSequence ON SEQUENCE FOR ALTER TO root;"
	enableAuditStmt := "ALTER AUDIT alterSequence enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	s.AuditServer().TestSyncConfig()
	time.Sleep(1 * time.Second)

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	// prepare sequence
	createSequenceExpect := int64(0)

	createSequenceExpect++
	if _, err := DB.Exec("CREATE SEQUENCE customer_seq"); err != nil {
		t.Fatal(err)
	}

	// test alter sequence
	alterSequenceMetric := ae.GetMetric(target.ObjectSequence)
	alterSequenceExpect := alterSequenceMetric.Count(target.Alter) + 1
	if _, err := DB.Exec("ALTER SEQUENCE customer_seq INCREMENT 2"); err != nil {
		t.Fatal(err)
	}
	if alterSequenceMetric.Count(target.Alter) != alterSequenceExpect {
		t.Errorf("expected db count:%d, but got %d", alterSequenceExpect, alterSequenceMetric.Count(target.Alter))
	}

	// test drop sequence
	createAuditStmt = "create audit dropSequence ON SEQUENCE FOR DROP TO root;"
	enableAuditStmt = "ALTER AUDIT dropSequence enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	dropSequenceMetric := ae.GetMetric(target.ObjectSequence)
	dropSequenceExpect := dropSequenceMetric.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP SEQUENCE customer_seq"); err != nil {
		t.Fatal(err)
	}

	if dropSequenceMetric.Count(target.Drop) != dropSequenceExpect {
		t.Errorf("expected db count:%d, but got %d", dropSequenceExpect, dropSequenceMetric.Count(target.Drop))
	}

}

func TestDurationCreateTB(t *testing.T) {
	//var err error
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, audDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := audDB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	s.AuditServer().TestSyncConfig()

	time.Sleep(1 * time.Second)

	tbtd := fmt.Sprintf("auditlog%s", timeutil.Now().Format("20060102"))
	tbtm := fmt.Sprintf("auditlog%s", timeutil.Now().Add(24*time.Hour).Format("20060102"))

	rows, err := audDB.Query("SHOW TABLES FROM auditdb")
	if err != nil {
		return
	}
	defer rows.Close()
	tb1 := false
	tb2 := false
	namel := []string{}
	for rows.Next() {
		var (
			name  string
			owner string
		)
		if err := rows.Scan(&name, &owner); err != nil {
			t.Fatal(err)
		}
		namel = append(namel, name)
		if name == tbtd {
			tb1 = true
			break
		}
	}

	if !tb1 {
		t.Errorf("expect table %s, but got %s", tbtd, namel)
	}

	rows2, err := audDB.Query("SHOW TABLES FROM auditdb")
	if err != nil {
		return
	}
	defer rows2.Close()

	namel = []string{}
	for rows2.Next() {
		var (
			name  string
			owner string
		)
		if err := rows2.Scan(&name, &owner); err != nil {
			t.Fatal(err)
		}
		namel = append(namel, name)
		if name == tbtm {
			tb2 = true
			break
		}
	}
	if !tb2 {
		t.Errorf("expect table %s, but got %s", tbtm, namel)
	}

}

func TestCancelQueryMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// cancel query test

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit cancelQuery ON Query FOR CANCEL TO root;"
	enableAuditStmt := "ALTER AUDIT cancelQuery enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	CancelQueryMetric := ae.GetMetric(target.ObjectQuery)
	CancelQueryExpect := CancelQueryMetric.Count(target.Cancel) + 1

	go func() {
		_, _ = DB.Exec("select pg_sleep(30);")
	}()

	if _, err := DB.Exec("cancel query (select query_id from [show cluster queries] where query ='SELECT pg_sleep(30)');"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if CancelQueryMetric.Count(target.Cancel) != CancelQueryExpect {
		t.Errorf("expected db count:%d, but got %d", CancelQueryExpect, CancelQueryMetric.Count(target.Cancel))
	}

}

func TestRoleMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit createRole ON ROLE FOR CREATE TO secroot;"
	enableAuditStmt := "ALTER AUDIT createRole enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	// prepare Role
	time.Sleep(1 * time.Second)
	RoleMetrics := ae.GetMetric(target.ObjectRole)
	createRoleExpect := RoleMetrics.Count(target.Create) + 1

	//TODO the role operation is contain in user ,tree type error
	if _, err := DB.Exec("CREATE ROLE dev_ops"); err != nil {
		//TODO(yhq)role and user share common tree type so there will be tree type error now
		err = nil
	}

	if RoleMetrics.Count(target.Create) != createRoleExpect {
		t.Errorf("expected db count:%d, but got %d", createRoleExpect, RoleMetrics.Count(target.Create))
	}
	// test drop Role
	dropRoleExpect := RoleMetrics.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP ROLE dev_ops"); err != nil {
		err = nil
	}

	if RoleMetrics.Count(target.Drop) != dropRoleExpect {
		t.Errorf("expected db count:%d, but got %d", dropRoleExpect, RoleMetrics.Count(target.Drop))
	}

}

func TestUserMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()
	time.Sleep(1 * time.Second)

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit createUser ON USER FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT createUser enable;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	UserMetric := ae.GetMetric(target.ObjectUser)
	// prepare User
	createUserExpect := UserMetric.Count(target.Create) + 1

	if _, err := DB.Exec("CREATE USER 'u1'  WITH PASSWORD '111111'"); err != nil {
		t.Fatal(err)
	}
	if UserMetric.Count(target.Create) != createUserExpect {
		t.Errorf("expected db count:%d, but got %d", createUserExpect, UserMetric.Count(target.Create))
	}
	//test alter User
	alterAuditStmt := "create audit alterUser ON USER FOR ALTER TO root;"
	enableAuditStmt = "ALTER AUDIT alterUser enable;"

	if _, err := DB.Exec(alterAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	alterUserExpect := UserMetric.Count(target.Alter) + 1
	if _, err := DB.Exec("ALTER USER 'u1'  WITH PASSWORD '11aaAA^^'"); err != nil {
		t.Fatal(err)
	}
	if UserMetric.Count(target.Alter) != alterUserExpect {
		t.Errorf("expected db count:%d, but got %d", alterUserExpect, UserMetric.Count(target.Alter))
	}

	// test drop User
	dropAuditStmt := "create audit dropUser ON USER FOR DROP TO root;"
	enableAuditStmt = "ALTER AUDIT dropUser enable;"

	if _, err := DB.Exec(dropAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	dropUserExpect := UserMetric.Count(target.Drop) + 1

	if _, err := DB.Exec("DROP USER 'u1'"); err != nil {
		t.Fatal(err)
	}

	if UserMetric.Count(target.Drop) != dropUserExpect {
		t.Errorf("expected db count:%d, but got %d", dropUserExpect, UserMetric.Count(target.Drop))
	}

}

func TestSplitAtMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()
	time.Sleep(1 * time.Second)
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit splitTable ON TABLE FOR ALTER TO root;"
	enableAuditStmt := "ALTER AUDIT splitTable enable;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	//SPLIT AT operation preparation
	if _, err := DB.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled=false;"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE TABLE t (k1 INT, k2 INT, v INT, w INT, PRIMARY KEY (k1, k2))"); err != nil {
		t.Fatal(err)
	}

	// test split at
	tableMetric := ae.GetMetric(target.ObjectTable)
	alterSplitAtExpect := tableMetric.Count(target.Alter) + 1
	if _, err := DB.Exec("ALTER TABLE t SPLIT AT VALUES (5,1), (5,2), (5,3);"); err != nil {
		t.Fatal(err)
	}
	if tableMetric.Count(target.Alter) != alterSplitAtExpect {
		t.Errorf("expected db count:%d, but got %d", alterSplitAtExpect, tableMetric.Count(target.Alter))
	}

}

func TestSetClusterSettingMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	// test SetClusterSetting
	SetClusterSettingMetric := ae.GetMetric(target.ObjectClusterSetting)
	SetClusterSettingExpect := SetClusterSettingMetric.Count(target.Set) + 1
	if _, err := DB.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled=false;"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if SetClusterSettingMetric.Count(target.Set) != SetClusterSettingExpect {
		t.Errorf("expected db count:%d, but got %d", SetClusterSettingExpect, SetClusterSettingMetric.Count(target.Set))
	}

}

func TestControlJobsMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	// test Job_control
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit pauseJob ON JOB FOR PAUSE TO root;"
	enableAuditStmt := "ALTER AUDIT pauseJob enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}
	dropJobsStmt := "delete from system.jobs where true;"
	// Delete jobs, otherwise there will be an error message when
	// executing pause job with status succeeded cannot be requested to be paused
	if _, err := DB.Exec(dropJobsStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	//PAUSE JOBS
	JobsMetric := ae.GetMetric(target.ObjectJob)
	ControlJobsExpect := JobsMetric.Count(target.Pause) + 1
	time.Sleep(1 * time.Second)
	if _, err := DB.Exec("PAUSE JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	if JobsMetric.Count(target.Pause) != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, JobsMetric.Count(target.Pause))
	}

	//RESUME JOBS
	createAuditStmt = "create audit resumeJob ON JOB FOR RESUME TO root;"
	enableAuditStmt = "ALTER AUDIT resumeJob enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {

		t.Fatal(err)
	}
	// Delete jobs, otherwise there will be an error message when executing pause
	// job with status succeeded cannot be requested to be RESUME
	if _, err := DB.Exec(dropJobsStmt); err != nil {
		t.Fatal(err)
	}
	ControlJobsExpect = JobsMetric.Count(target.Resume) + 1
	if _, err := DB.Exec("RESUME JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if JobsMetric.Count(target.Resume) != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, JobsMetric.Count(target.Resume))
	}

	//CANCEL JOBS
	createAuditStmt = "create audit cancelJob ON JOB FOR CANCEL TO root;"
	enableAuditStmt = "ALTER AUDIT cancelJob enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	// Delete jobs, otherwise there will be an error message when executing pause
	// job with status succeeded cannot be requested to be CANCEL
	if _, err := DB.Exec(dropJobsStmt); err != nil {
		t.Fatal(err)
	}
	ControlJobsExpect = JobsMetric.Count(target.Cancel) + 1
	if _, err := DB.Exec("CANCEL JOBS (SELECT job_id FROM [SHOW JOBS] WHERE user_name = 'root');"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	if JobsMetric.Count(target.Cancel) != ControlJobsExpect {
		t.Errorf("expected db count:%d, but got %d", ControlJobsExpect, JobsMetric.Count(target.Cancel))
	}
}

func TestSetZoneConfigMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//Create test database and table
	if _, err := DB.Exec("CREATE DATABASE TEST"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE TABLE t (i Int)"); err != nil {
		t.Fatal(err)
	}

	// test Set zone config
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit setZone ON RANGE FOR ALTER TO root;"
	enableAuditStmt := "ALTER AUDIT setZone enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	SetZoneConfigMetric := ae.GetMetric(target.ObjectRange)
	SetZoneConfigExpect := SetZoneConfigMetric.Count(target.Alter) + 1
	//range
	if _, err := DB.Exec("ALTER RANGE default CONFIGURE ZONE USING num_replicas = 5, gc.ttlseconds = 100000;"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.Count(target.Alter) != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.Count(target.Alter))
	}
	//database
	createAuditStmt = "create audit setZonedb ON DATABASE FOR ALTER TO root;"
	enableAuditStmt = "ALTER AUDIT setZonedb enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	SetZoneConfigMetric = ae.GetMetric(target.ObjectDatabase)
	SetZoneConfigExpect = SetZoneConfigMetric.Count(target.Alter) + 1
	if _, err := DB.Exec("ALTER DATABASE test CONFIGURE ZONE USING num_replicas = 5, gc.ttlseconds = 100000;"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.Count(target.Alter) != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.Count(target.Alter))
	}
	//table
	createAuditStmt = "create audit setZonetb ON TABLE FOR ALTER TO root;"
	enableAuditStmt = "ALTER AUDIT setZonetb enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	SetZoneConfigMetric = ae.GetMetric(target.ObjectTable)
	SetZoneConfigExpect = SetZoneConfigMetric.Count(target.Alter) + 1
	SetZoneConfigExpect = SetZoneConfigMetric.Count(target.Alter) + 1
	if _, err := DB.Exec("ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 90000, gc.ttlseconds = 89999, num_replicas = 4, constraints = '[-region=west]';"); err != nil {
		t.Fatal(err)
	}
	if SetZoneConfigMetric.Count(target.Alter) != SetZoneConfigExpect {
		t.Errorf("expected db count:%d, but got %d", SetZoneConfigExpect, SetZoneConfigMetric.Count(target.Alter))
	}
}

func TestGrandRevokeMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit grantPrivilege ON PRIVILEGE FOR GRANT TO root;"
	enableAuditStmt := "ALTER AUDIT grantPrivilege enable;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	//Create test database and user
	if _, err := DB.Exec("CREATE DATABASE db1"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE DATABASE db2"); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec("CREATE USER u1;"); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE USER u2;"); err != nil {
		t.Fatal(err)
	}

	//test GrandPrivileges
	time.Sleep(1 * time.Second)
	PrivilegesMetric := ae.GetMetric(target.ObjectPrivilege)
	GrandPrivilegesExpect := PrivilegesMetric.Count(target.Grant) + 1
	if _, err := DB.Exec("GRANT CREATE ON DATABASE db1, db2 TO u1, u2;"); err != nil {
		t.Fatal(err)
	}
	if PrivilegesMetric.Count(target.Grant) != GrandPrivilegesExpect {
		t.Errorf("expected db count:%d, but got %d", GrandPrivilegesExpect, PrivilegesMetric.Count(target.Grant))
	}

	//test RevokePrivileges
	createAuditStmt = "create audit revokePrivilege ON PRIVILEGE FOR REVOKE TO root;"
	enableAuditStmt = "ALTER AUDIT revokePrivilege enable;"

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	RevokePrivilegesExpect := PrivilegesMetric.Count(target.Revoke) + 1
	if _, err := DB.Exec("REVOKE CREATE ON DATABASE db1, db2 FROM u1, u2;"); err != nil {
		t.Fatal(err)
	}
	if PrivilegesMetric.Count(target.Revoke) != RevokePrivilegesExpect {
		t.Errorf("expected db count:%d, but got %d", RevokePrivilegesExpect, PrivilegesMetric.Count(target.Revoke))
	}

}

func TestSetSessionMetric(t *testing.T) {

	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	SetSessionMetric := ae.GetMetric(target.ObjectSession)
	SetSessionExpect := SetSessionMetric.Count(target.Set) + 1

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit SetSession ON SESSION FOR SET TO root;"
	enableAuditStmt := "ALTER AUDIT SetSession enable;"

	if _, err := db.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("create database db1;"); err != nil {
		t.Fatal(err)
	}

	sqlStmt := "set database = db1;"
	if _, err := db.Exec(sqlStmt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if SetSessionMetric.Count(target.Set) != SetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", SetSessionExpect, SetSessionMetric.Count(target.Set))
	}
	sqlStmt = "SET extra_float_digits = -10;"
	if _, err := db.Exec(sqlStmt); err != nil {
		t.Fatal(err)
	}
	SetSessionExpect++
	if SetSessionMetric.Count(target.Set) != SetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", SetSessionExpect, SetSessionMetric.Count(target.Set))
	}

}

func TestReSetSessionMetric(t *testing.T) {

	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	ReSetSessionMetric := ae.GetMetric(target.ObjectSession)
	ReSetSessionExpect := ReSetSessionMetric.Count(target.Reset) + 1

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit SetSession ON SESSION FOR SET TO root;"
	enableAuditStmt := "ALTER AUDIT SetSession enable;"

	if _, err := db.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}

	sqlStmt := "SET search_path = DEFAULT;"
	if _, err := db.Exec(sqlStmt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if ReSetSessionMetric.Count(target.Reset) != ReSetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", ReSetSessionExpect, ReSetSessionMetric.Count(target.Reset))
	}

	sqlStmt = "SET extra_float_digits = -10;"
	if _, err := db.Exec(sqlStmt); err != nil {
		t.Fatal(err)
	}

	sqlStmt = "RESET extra_float_digits;"
	if _, err := db.Exec(sqlStmt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	ReSetSessionExpect++
	if ReSetSessionMetric.Count(target.Reset) != ReSetSessionExpect {
		t.Errorf("expected db count:%d, but got %d", ReSetSessionExpect, ReSetSessionMetric.Count(target.Reset))
	}

}

func TestCancelSessionMetric(t *testing.T) {

	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true //Insecure mode conn
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	s.AuditServer().TestSyncConfig()

	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	CancelSessionMetric := ae.GetMetric(target.ObjectSession)
	CancelSessionExpect := CancelSessionMetric.Count(target.Cancel) + 1

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"
	createAuditStmt := "create audit cancelSession ON SESSION FOR CANCEL TO root;"
	enableAuditStmt := "ALTER AUDIT cancelSession enable;"
	createUser := "create user u1;"

	if _, err := db.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(createUser); err != nil {
		t.Fatal(err)
	}
	db2, _ := serverutils.ServerConn(t, params, s, "u1")

	go func() {
		_, _ = db2.Exec("select pg_sleep(30)")
	}()
	time.Sleep(500 * time.Millisecond)

	var sessionID string
	//if err := db.QueryRow("select session_id from kwdb_internal.node_sessions where active_queries=$1", "SELECT pg_sleep(30.0)").Scan(&sessionID); err != nil {
	//	t.Fatal(err)
	//}
	if err := db.QueryRow("select session_id from kwdb_internal.node_sessions where  user_name = 'u1';").Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("cancel session $1", sessionID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if CancelSessionMetric.Count(target.Cancel) != CancelSessionExpect {
		t.Errorf("expected db count:%d, but got %d", CancelSessionExpect, CancelSessionMetric.Count(target.Cancel))
	}

}

func TestNodeJoinMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)

	//waiting for server complete initialization
	time.Sleep(1 * time.Second)

	//enable force sync
	s.AuditServer().TestSyncConfig()
	audDB := tc.ServerConn(0)

	//defer cleanupFunc()
	defer audDB.Close()

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := audDB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	//get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)

	//node join test
	NodeJoinMetric := ae.GetMetric(target.ObjectNode)
	NodeJoinExpect := NodeJoinMetric.Count(target.Join)
	if NodeJoinMetric.Count(target.Join) != NodeJoinExpect {
		t.Errorf("expected db count:%d, but got %d", NodeJoinExpect, NodeJoinMetric.Count(target.Join))
	}
	//node restart
	s.Stop()
	s = tc.Server(0).(*server.TestServer)
	//s = tc.Server(0)
	time.Sleep(1 * time.Second)
	s.AuditServer().TestSyncConfig()
	ae2 := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	NodeRestartMetric := ae2.GetMetric(target.ObjectNode)
	NodeRestartExpect := NodeRestartMetric.Count(target.Restart)
	if NodeRestartMetric.Count(target.Restart) != NodeRestartExpect {
		t.Errorf("expected db count:%d, but got %d", NodeRestartExpect, NodeRestartMetric.Count(target.Restart))
	}

}

func TestNodeDecommissionMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)

	//waiting for server complete initialization

	//enable force sync
	s.AuditServer().TestSyncConfig()

	//get a event handler
	ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	audDB := tc.ServerConn(0)
	defer audDB.Close()

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := audDB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	//node decommission test
	NodeDecommissionMetric := ae.GetMetric(target.ObjectNode)
	NodeDecommissionExpect := NodeDecommissionMetric.Count(target.Decommission) + 1

	decommissioningNodeID := s.NodeID()
	if err := s.Decommission(ctx, true, []roachpb.NodeID{decommissioningNodeID}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	NodeDecommissionMetric = ae.GetMetric(target.ObjectNode)
	if NodeDecommissionMetric.Count(target.Decommission) != NodeDecommissionExpect {
		t.Errorf("expected db count:%d, but got %d", NodeDecommissionExpect, NodeDecommissionMetric.Count(target.Decommission))
	}

	NodeRecommissionMetric := ae.GetMetric(target.ObjectNode)
	NodeRecommissionExpect := NodeRecommissionMetric.Count(target.Recommission) + 1
	NodeRecommissionNodeID := s.NodeID()
	if err := s.Decommission(ctx, false, []roachpb.NodeID{NodeRecommissionNodeID}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	NodeRecommissionMetric = ae.GetMetric(target.ObjectNode)
	if NodeRecommissionMetric.Count(target.Recommission) != NodeRecommissionExpect {
		t.Errorf("expected db count:%d, but got %d", NodeRecommissionExpect, NodeRecommissionMetric.Count(target.Recommission))
	}
}

func TestCreateAuditStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)

	//enable force sync
	s.AuditServer().TestSyncConfig()

	//get a event handler
	//ae := (s.AuditServer().GetHandler()).(*event.AuditEvent)
	audDB := tc.ServerConn(0)
	defer audDB.Close()

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := audDB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1000)

	var auditStmtType = map[string][]string{
		"DATABASE":   {"ALTER", "CREATE", "DROP"},
		"SCHEMA":     {"ALTER", "CREATE", "DROP", "DUMP", "LOAD"},
		"TABLE":      {"ALTER", "CREATE", "DROP", "DUMP", "FLASHBACK", "LOAD", "TRUNCATE"},
		"INDEX":      {"ALTER", "CREATE", "DROP"},
		"VIEW":       {"ALTER", "CREATE", "DROP"},
		"SEQUENCE":   {"ALTER", "CREATE", "DROP"},
		"JOB":        {"CANCEL", "PAUSE", "RESUME"},
		"USER":       {"ALTER", "CREATE", "DROP"},
		"ROLE":       {"CREATE", "DROP", "GRANT", "REVOKE"},
		"PRIVILEGE":  {"GRANT", "REVOKE"},
		"QUERY":      {"CANCEL", "EXPLAIN"},
		"RANGE":      {"ALTER"},
		"STATISTICS": {"CREATE"},
		"SESSION":    {"CANCEL"},
		"AUDIT":      {"ALTER", "CREATE", "DROP"},
	}

	for obj, ops := range auditStmtType {
		for _, op := range ops {
			auditName := fmt.Sprintf("test%s%s", op, obj)
			createAuditStmt := fmt.Sprintf("create audit %s ON %s FOR %s TO %s;", auditName, obj, op, security.RootUser)
			fmt.Printf("createAuditStmt:%s\n", createAuditStmt)
			//enableAuditStmt := fmt.Sprintf("ALTER AUDIT %s enable;",name)
			_, err := audDB.Exec(createAuditStmt)
			if err != nil {
				t.Errorf("create audit %s fail: ", createAuditStmt)
			}
		}
	}

}

func TestCreateAuditObj(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	ctx := context.TODO()
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0).(*server.TestServer)
	//waiting for server complete initialization

	s.AuditServer().TestSyncConfig()

	DB := tc.ServerConn(0)
	defer DB.Close()

	//prepare obj

	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	ctstmt := "CREATE TABLE defaultdb.public.t(i int);"
	cvstmt := "CREATE VIEW defaultdb.public.v AS SELECT 1 AS x;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(ctstmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(cvstmt); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	var auditObj = map[string]string{
		"TABLE": "defaultdb.public.t",
		"VIEW":  "defaultdb.public.v",
	}

	var auditObjectType = map[string][]string{
		"TABLE": {"DELETE", "INSERT", "SELECT", "UPDATE"},
		"VIEW":  {"DELETE", "INSERT", "SELECT", "UPDATE"},
	}

	for obj, ops := range auditObjectType {
		for _, op := range ops {
			auditName := fmt.Sprintf("test%s%s", op, obj)
			createAuditStmt := fmt.Sprintf("create audit %s ON %s FOR %s TO %s;", auditName, obj, op, security.RootUser)

			_, err := DB.Exec(createAuditStmt)
			if err == nil {
				t.Errorf("create audit %s success,expect fail", createAuditStmt)
			}
		}
	}
	for obj, ops := range auditObjectType {
		targetName := auditObj[obj]
		for _, op := range ops {
			auditName := fmt.Sprintf("test%s%s", op, obj)
			createAuditStmt := fmt.Sprintf("create audit %s ON %s %s FOR %s TO %s;", auditName, obj, targetName, op, security.RootUser)
			_, err := DB.Exec(createAuditStmt)
			if err != nil {
				t.Errorf("create audit %s fail,expect success", createAuditStmt)
			}
		}
	}

}

func TestUserLoginMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCluster := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.TODO())

	server0 := testCluster.Server(0).(*server.TestServer)
	server0.AuditServer().TestSyncConfig()

	DB0 := testCluster.ServerConn(0)
	defer DB0.Close()

	ae0 := (server0.AuditServer().GetHandler()).(*event.AuditEvent)
	userloginMetric1 := ae0.GetMetric(target.ObjectConn)
	userloginExpect := userloginMetric1.Count(target.Login)

	createAuditStmt := "create audit createDB ON ROLE FOR CREATE TO root;"
	if _, err := DB0.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	userloginExpect++
	time.Sleep(1 * time.Second)
	frequency := userloginMetric1.Count(target.Login)
	if frequency != userloginExpect {
		t.Errorf("expected db count:%d, but got %d", userloginExpect, frequency)
	}
}
