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

package sql

//import (
//	"context"
//	"testing"
//
//	"gitee.com/kwbasedb/kwbase/pkg/kv"
//	"gitee.com/kwbasedb/kwbase/pkg/security"
//	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec/execbuilder"
//	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
//	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
//	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
//	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
//)
//
//func TestMakeTSSpans(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	// TODO: fix unit test failed
//	t.Skip()
//	ctx := context.Background()
//	params, _ := tests.CreateTestServerParams()
//	s, _, db := serverutils.StartServer(t, params)
//	defer s.Stopper().Stop(ctx)
//
//	execCfg := s.ExecutorConfig().(ExecutorConfig)
//	internalPlanner, cleanup := NewInternalPlanner(
//		"test",
//		kv.NewTxn(ctx, db, s.NodeID()),
//		security.RootUser,
//		&MemoryMetrics{},
//		&execCfg,
//	)
//	defer cleanup()
//	p := internalPlanner.(*planner)
//
//	initStmts := []string{
//		"create tenant t1",
//		"create portal t1.p1 memcapacity 128 diskcapacity 1024 dirpath '/data' wal enabled file split type 10 day",
//		"CREATE DEVICE t1.p1.d1 (e1 int,e2 float)",
//	}
//
//	for _, stmt := range initStmts {
//		if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(ctx, "test create kwdb object", p.txn, stmt); err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	testCases := []struct {
//		in     string
//		num    int
//		from   []uint64
//		to     []uint64
//		lEmpty []bool
//		rEmpty []bool
//	}{
//		{"select * unionfrom t1.p1.d1 where e1.timestamp<'2022-12-10 12:12:12'", 1, []uint64{0}, []uint64{1670674331}, []bool{true}, []bool{false}},
//		{"select * unionfrom t1.p1.d1 where e1.timestamp>'2022-12-10 12:12:12'", 1, []uint64{1670674332}, []uint64{0}, []bool{false}, []bool{true}},
//		{"select * unionfrom t1.p1.d1 where e1.timestamp<'2022-12-10 12:12:12' and e1.timestamp>'2022-12-1 12:12:12'", 1, []uint64{1669896732}, []uint64{1670674331}, []bool{false}, []bool{false}},
//		{"select * unionfrom t1.p1.d1 where e1.timestamp<'2022-12-10 12:12:12' or e1.timestamp>'2022-12-11 12:12:12'", 2, []uint64{0, 1670760732}, []uint64{1670674331, 0}, []bool{true, false}, []bool{false, true}},
//		{"select * unionfrom t1.p1.d1 where e1.timestamp<'2022-12-10 12:12:12' or '2022-12-11 12:12:12' < e1.timestamp and e1.timestamp < '2022-12-12 12:12:12' or e1.timestamp>'2022-12-13 12:12:12'", 3, []uint64{0, 1670760732, 1670933532}, []uint64{1670674331, 1670847131, 0}, []bool{true, false, false}, []bool{false, false, true}},
//	}
//
//	for _, tc := range testCases {
//		stmt, err := parser.ParseOne(tc.in)
//		if err != nil {
//			t.Fatal(err)
//		}
//		p.stmt = &Statement{Statement: stmt}
//		opc := &p.optPlanningCtx
//		opc.reset()
//
//		execMemo, layerType, err := opc.buildExecMemo(ctx, false)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		root := execMemo.RootExpr()
//		execFactory := makeExecFactory(p)
//		bld := execbuilder.New(&execFactory, execMemo, &opc.catalog, root, p.EvalContext())
//		bld.PhysType = layerType
//		plan, err := bld.Build()
//		if err != nil {
//			t.Fatal(err)
//		}
//		switch ty := plan.(type) {
//		case *planTop:
//			if r, ok := ty.plan.(*renderNode); ok {
//				switch ts := r.source.plan.(type) {
//				case *tsScanNode:
//					if len(ts.tsSpans) == tc.num {
//						for i, v := range ts.tsSpans {
//							if (v.FromTimeStamp == 0) != tc.lEmpty[i] {
//								t.Fatalf("expect From is nil but found not nil")
//							} else if v.FromTimeStamp != 0 {
//								if v.FromTimeStamp != tc.from[i] {
//									t.Fatalf("test case %v: expect %v but found %v", i, tc.from[i], v.FromTimeStamp)
//								}
//							}
//
//							if (v.ToTimeStamp == 1<<64-1) != tc.rEmpty[i] {
//								t.Fatalf("expect To is nil but found not nil")
//							} else if v.ToTimeStamp != 1<<64-1 {
//								if v.ToTimeStamp != tc.to[i] {
//									t.Fatalf("test case %v: expect %v but found %v", i, tc.to[i], v.ToTimeStamp)
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//	}
//}
