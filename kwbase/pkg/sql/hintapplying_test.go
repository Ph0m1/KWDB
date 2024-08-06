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

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func GetScanPrivate(RelExpr memo.RelExpr) (ScanPrivates []memo.ScanPrivate) {
	switch Expr := RelExpr.(type) {
	case *memo.ProjectExpr:
		ScanPrivates = append(ScanPrivates, GetScanPrivate(Expr.Input)...)
		return ScanPrivates

	case *memo.InnerJoinExpr:
		ScanPrivates = append(ScanPrivates, GetScanPrivate(Expr.Left)...)
		ScanPrivates = append(ScanPrivates, GetScanPrivate(Expr.Right)...)
		return ScanPrivates

	case *memo.SelectExpr:
		ScanPrivates = append(ScanPrivates, GetScanPrivate(Expr.Input)...)
		return ScanPrivates

	case *memo.ScanExpr:
		ScanPrivates = append(ScanPrivates, Expr.ScanPrivate)
		return ScanPrivates
	}
	return ScanPrivates
}

func TestAccessHintApplying(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		TestHintForest tree.HintForest
	}{
		{"NoScanHint", []tree.HintTree{}},
		{"UseTableScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
		}},
		{"IgnoreTableScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.IgnoreTableScan, TableName: "a"},
		}},
		{"ForceTableScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "a"},
		}},
		{"UseIndexScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"key1"}},
		}},
		{"IgnoreIndexScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.IgnoreIndexScan, TableName: "a", IndexName: []string{"key2"}},
		}},
		{"ForceIndexScan", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"key1"}},
		}},
		{"UseIndexOnly", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.UseIndexOnly, TableName: "a", IndexName: []string{"key2"}},
		}},
		{"IgnoreIndexOnly", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.IgnoreIndexOnly, TableName: "a", IndexName: []string{"key2"}},
		}},
		{"ForceIndexOnly", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.ForceIndexOnly, TableName: "a", IndexName: []string{"key1"}},
		}},
	}

	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE a (a1 INT PRIMARY KEY, a2 INT, a3 STRING, INDEX key1 (a2,a3), INDEX key2 (a3))")
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	//sql := "SELECT a3 FROM a"
	sql := "SELECT a2, a3 FROM a"
	//Scan Hint
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintforest := tt.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			switch s := stmt.AST.(type) {
			case *tree.Select:
				switch T := s.Select.(type) {
				case *tree.SelectClause:
					T.HintForest = append(T.HintForest, hintforest...)
				}
			}

			o.Init(&evalCtx, catalog)
			b := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			if err := b.Build(); err != nil {
				t.Fatal(err)
			}

			if _, err := o.Optimize(); err != nil {
				t.Error(err)
			}

			root := o.Memo().RootExpr().(memo.RelExpr)
			fmt.Println(root.String())
			scanPrivates := GetScanPrivate(root)
			for _, scanPrivate := range scanPrivates {
				if scanPrivate.Flags.HintType == keys.NoScanHint {
					continue
				}
				if (scanPrivate.Flags.HintType == keys.ForceTableScan || scanPrivate.Flags.HintType == keys.UseTableScan) &&
					scanPrivate.Index != cat.PrimaryIndex {
					t.Fatalf("Failed to use Access Method Hint: %v", scanPrivate.Flags.HintType)
				}
				if scanPrivate.Flags.HintType == keys.IgnoreTableScan && scanPrivate.Index == cat.PrimaryIndex {
					t.Fatalf("Failed to use Access Method Hint: %v", scanPrivate.Flags.HintType)
				}
				if (scanPrivate.Flags.HintType == keys.ForceIndexScan || scanPrivate.Flags.HintType == keys.ForceIndexOnly) &&
					scanPrivate.Index != scanPrivate.Flags.HintIndexes[0] {
					t.Fatalf("Failed to use Access Method Hint: %v", scanPrivate.Flags.HintType)
				}
				if scanPrivate.Flags.HintType == keys.UseIndexScan || scanPrivate.Flags.HintType == keys.UseIndexOnly {
					find := false
					for _, index := range scanPrivate.Flags.HintIndexes {
						if scanPrivate.Index == index {
							find = true
							break
						}
					}
					if !find {
						t.Fatalf("Failed to use Access Method Hint: %v", scanPrivate.Flags.HintType)
					}
				}

				if scanPrivate.Flags.HintType == keys.IgnoreIndexScan || scanPrivate.Flags.HintType == keys.IgnoreIndexOnly {
					for _, index := range scanPrivate.Flags.HintIndexes {
						if scanPrivate.Index == index {
							t.Fatalf("Failed to use Access Method Hint: %v", scanPrivate.Flags.HintType)
						}
					}
				}

			}

		})
	}
}

func TestJoinOrderHintApplying(t *testing.T) {
	//defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		TestHintForest tree.HintForest
	}{
		{"NoJoinHint", nil},
		{"join(ab)", []tree.HintTree{
			&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
				RealJoinOrder: true,
				Left:          &tree.HintTreeScanNode{TableName: "a"},
				Right:         &tree.HintTreeScanNode{TableName: "b"}},
		}},
		{"join((ab),(cd))", []tree.HintTree{
			&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
				RealJoinOrder: true,
				Left:          &tree.HintTreeScanNode{TableName: "a"},
				Right:         &tree.HintTreeScanNode{TableName: "b"}},
			&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
				RealJoinOrder: true,
				Left:          &tree.HintTreeScanNode{TableName: "c"},
				Right:         &tree.HintTreeScanNode{TableName: "d"},
			},
		}},
		{"join(abc)", []tree.HintTree{
			&tree.HintTreeJoinNode{
				HintType:      keys.NoJoinHint,
				RealJoinOrder: true,
				Left:          &tree.HintTreeScanNode{TableName: "a"},
				Right: &tree.HintTreeJoinNode{
					HintType:      keys.NoJoinHint,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "b"},
					Right:         &tree.HintTreeScanNode{TableName: "c"},
				},
			},
		}},
	}

	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE a (a1 INT PRIMARY KEY, a2 INT, a3 STRING, INDEX a_key1 (a2,a3), INDEX a_key2 (a3))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = catalog.ExecuteDDL("CREATE TABLE b (b1 INT PRIMARY KEY, b2 INT, b3 STRING, b4 INT, INDEX b_key1 (b2), INDEX b_key2 (b4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE c (c1 INT PRIMARY KEY, c2 INT, c3 STRING, c4 INT, INDEX c_key1 (c2), INDEX c_key2 (c4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE d (d1 INT PRIMARY KEY, d2 INT, d3 STRING, d4 INT, INDEX d_key1 (d2), INDEX d_key2 (d4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE e (e1 INT PRIMARY KEY, e2 INT, e3 STRING, e4 INT, INDEX e_key1 (e2), INDEX e_key2 (e4))")
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	//sql := "SELECT a3 FROM a"
	sql := "SELECT a1, b1, c1, d1, e1 FROM a, b, c, d, e"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintforest := tt.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			switch s := stmt.AST.(type) {
			case *tree.Select:
				switch T := s.Select.(type) {
				case *tree.SelectClause:
					T.HintForest = append(T.HintForest, hintforest...)
				}
			}

			o.Init(&evalCtx, catalog)
			b := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			if err := b.Build(); err != nil {
				t.Fatal(err)
			}

			if _, err := o.Optimize(); err != nil {
				t.Error(err)
			}
			root := o.Memo().RootExpr().(memo.RelExpr)
			fmt.Println(root.String())
		})
	}

}

func TestJoinMethodHintApplying(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		sqlstm         string
		TestHintForest tree.HintForest
	}{
		{"NoJoinHint",
			"SELECT * FROM abc, xyz WHERE x=a",
			nil,
		},
		{"force hash",
			"SELECT a,b,m FROM small, abcd WHERE a=m AND b>1", //lookupjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abcd", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "small", IndexName: []string{}},
				},
			},
		},
		{"disallow hash",
			"SELECT * FROM abc, xyz WHERE a=z", //innerjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowHash,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abc", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "xyz", IndexName: []string{}}},
			},
		},
		{"force merge",
			"SELECT a,b,m FROM small, abcd WHERE a=m AND b>1", //lookupjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceMerge,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abcd", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "small", IndexName: []string{}},
				},
			},
		},

		{"disallow merge",
			"SELECT * FROM abc, xyz WHERE x=a", //mergejoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abc", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "xyz", IndexName: []string{}},
				},
			},
		},

		{"force lookup",
			"SELECT * FROM abc, xyz WHERE x=a", //mergejoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abc", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "xyz", IndexName: []string{}},
				},
			},
		},

		{"ab(force lookup with index_mn)",
			"SELECT a,b,m FROM small, abcd WHERE a=m AND b>1", //lookupjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup,
					IndexName:     "mn",
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "abcd", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "small", IndexName: []string{}}},
			}},
		{"ab(force lookup with index_abc)",
			"SELECT a,b,m FROM small, abcd WHERE a=m AND b>1", //lookupjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup,
					IndexName:     "abc",
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "small", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "abcd", IndexName: []string{}}},
			}},

		{"disallow lookup",
			"SELECT a,b,m FROM small, abcd WHERE a=m AND b>1", //lookupjoin
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowLookup,
					RealJoinOrder: true,
					Left:          &tree.HintTreeScanNode{TableName: "abcd", IndexName: []string{}},
					Right:         &tree.HintTreeScanNode{TableName: "small", IndexName: []string{}},
				},
			},
		},
	}

	defer leaktest.AfterTest(t)()
	catalog := testcat.New()

	_, err := catalog.ExecuteDDL("CREATE TABLE stu ( s INT, t INT, u INT, PRIMARY KEY (s,t,u), INDEX uts (u,t,s))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE den ( d INT, e INT, n INT, PRIMARY KEY (d,e,n), INDEX ned (n,e,d))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE abc(a INT, b INT, c INT, INDEX ab (a,b) STORING (c), INDEX bc (b,c) STORING (a))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE xyz(x INT, y INT, z INT, INDEX xy (x,y) STORING (z), INDEX yz (y,z) STORING (x))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE abcd (a INT, b INT, c INT, INDEX abc(a,b,c), INDEX ab(a,b))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE small (m INT, n INT, INDEX mn(m,n), INDEX m(m))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("ALTER TABLE small INJECT STATISTICS '[{\"columns\": [\"m\"], \"created_at\": \"2018-01-01 1:00:00.00000+00:00\",  \"row_count\": 10, \"distinct_count\": 10}]'")
	if err != nil {
		t.Fatal(err)
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintforest := tt.TestHintForest
			sql := tt.sqlstm
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}

			switch e := stmt.AST.(type) {
			case *tree.Select:
				switch T := e.Select.(type) {
				case *tree.SelectClause:
					T.HintForest = append(T.HintForest, hintforest...)
				}
			case *tree.Explain:
				switch s := e.Statement.(type) {
				case *tree.Select:
					switch T := s.Select.(type) {
					case *tree.SelectClause:
						T.HintForest = append(T.HintForest, hintforest...)
					}
				}
			}

			o.Init(&evalCtx, catalog)
			b := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			if err := b.Build(); err != nil {
				t.Fatal(err)
			}

			if _, err := o.Optimize(); err != nil {
				t.Error(err)
			}
			root := o.Memo().RootExpr().(memo.RelExpr)
			fmt.Println(root.String())
		})
	}
}

func TestCardinalityHintApplying(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		sqlstm         string
		TestHintForest tree.HintForest
	}{

		{"scan",
			"SELECT k FROM a WHERE k > 1",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "a", TotalCardinality: 3},
			},
		},
		{"select_scan",
			"SELECT k FROM a WHERE u=v",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "a", TotalCardinality: 3},
			},
		},
		{"indexJoin_scan",
			"SELECT * FROM b WHERE v >= 1 AND v <= 10",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "b", TotalCardinality: 3},
			},
		},

		{"indexJoin_select_scan",
			"SELECT * FROM b WHERE v >= 1 AND v <= 10 AND k > 5",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "b", TotalCardinality: 3},
			},
		},
		{"select_indexJoin_scan",
			"SELECT * FROM b WHERE (u, k, v) > (1, 2, 3) AND (u, k, v) < (8, 9, 10)",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "b", TotalCardinality: 3},
			},
		},

		{"join_one_table",
			"SELECT * FROM c, d, e, f, g where c4 > 1",
			[]tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "c", TotalCardinality: 3},
			},
		},

		{"join_fg",
			"SELECT * FROM e, f, g where f4 = g4 and e4 > 1 and f4 > 2",
			[]tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
					Left:                 &tree.HintTreeScanNode{TableName: "f", TotalCardinality: 2, EstimatedCardinality: 1000},
					Right:                &tree.HintTreeScanNode{TableName: "g"},
					TotalCardinality:     3,
					EstimatedCardinality: 9801,
				},
				&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "e", TotalCardinality: 2, EstimatedCardinality: 10002},
			},
		},
	}

	defer leaktest.AfterTest(t)()
	catalog := testcat.New()

	_, err := catalog.ExecuteDDL("CREATE TABLE a(k INT PRIMARY KEY, u INT, v INT, INDEX u(u) STORING (v), UNIQUE INDEX v(v) STORING (u))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE b(k INT PRIMARY KEY, u INT, v INT, j JSONB, INDEX u(u), UNIQUE INDEX v(v), INVERTED INDEX inv_idx(j))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE c (c1 INT PRIMARY KEY, c2 INT, c3 STRING, c4 INT, INDEX c_key1 (c2), INDEX c_key2 (c4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE d (d1 INT PRIMARY KEY, d2 INT, d3 STRING, d4 INT, INDEX d_key1 (d2), INDEX d_key2 (d4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE e (e1 INT PRIMARY KEY, e2 INT, e3 STRING, e4 INT, INDEX e_key1 (e2), INDEX e_key2 (e4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE f (f1 INT PRIMARY KEY, f2 INT, f3 STRING, f4 INT, INDEX f_key1 (f2), INDEX f_key2 (f4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE g (g1 INT PRIMARY KEY, g2 INT, g3 STRING, g4 INT, INDEX g_key1 (g2), INDEX g_key2 (g4))")
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintforest := tt.TestHintForest
			sql := tt.sqlstm

			for numCount := 0; numCount < 1; numCount++ {
				stmt, err := parser.ParseOne(sql)
				if err != nil {
					t.Fatal(err)
				}
				if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
					t.Fatal(err)
				}
				if numCount == 0 {
					switch e := stmt.AST.(type) {
					case *tree.Select:
						switch T := e.Select.(type) {
						case *tree.SelectClause:
							T.HintForest = append(T.HintForest, hintforest...)
						}
					case *tree.Explain:
						switch s := e.Statement.(type) {
						case *tree.Select:
							switch T := s.Select.(type) {
							case *tree.SelectClause:
								T.HintForest = append(T.HintForest, hintforest...)
							}
						}
					}
				}

				o.Init(&evalCtx, catalog)
				b := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
				if err := b.Build(); err != nil {
					t.Fatal(err)
				}

				if _, err := o.Optimize(); err != nil {
					t.Error(err)
				}
				root := o.Memo().RootExpr().(memo.RelExpr)
				fmt.Println(root.String())
			}

		})
	}
}

func TestLeadingTableHintApplying(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		name           string
		TestHintForest tree.HintForest
	}{
		{"NoJoinHint", nil},
		{"LeadingTable_False(b)", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "b", LeadingTable: false},
		}},
		{"LeadingTable_True(b)", []tree.HintTree{
			&tree.HintTreeScanNode{HintType: keys.NoScanHint, TableName: "b", LeadingTable: true},
		}},
		{"LeadingTables(a,b)", []tree.HintTree{
			&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
				Left:         &tree.HintTreeScanNode{TableName: "a", LeadingTable: true},
				Right:        &tree.HintTreeScanNode{TableName: "b", LeadingTable: true},
				LeadingTable: true,
			},
		}},
		{"LeadingTables((a,b),c)", []tree.HintTree{
			&tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
				Left: &tree.HintTreeJoinNode{HintType: keys.NoJoinHint,
					Left:         &tree.HintTreeScanNode{TableName: "a", LeadingTable: true},
					Right:        &tree.HintTreeScanNode{TableName: "b", LeadingTable: true},
					LeadingTable: true,
				},
				Right:        &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
				LeadingTable: true,
			},
		}},
	}

	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE a (a1 INT PRIMARY KEY, a2 INT, a3 STRING, INDEX a_key1 (a2,a3), INDEX a_key2 (a3))")
	if err != nil {
		t.Fatal(err)
	}

	_, err = catalog.ExecuteDDL("CREATE TABLE b (b1 INT PRIMARY KEY, b2 INT, b3 STRING, b4 INT, INDEX b_key1 (b2), INDEX b_key2 (b4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE c (c1 INT PRIMARY KEY, c2 INT, c3 STRING, c4 INT, INDEX c_key1 (c2), INDEX c_key2 (c4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE d (d1 INT PRIMARY KEY, d2 INT, d3 STRING, d4 INT, INDEX d_key1 (d2), INDEX d_key2 (d4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE e (e1 INT PRIMARY KEY, e2 INT, e3 STRING, e4 INT, INDEX e_key1 (e2), INDEX e_key2 (e4))")
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	evalCtx.SessionData.ReorderJoinsLimit = 4
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	//sql := "SELECT a3 FROM a"
	sql := "SELECT a1, b1, c1  FROM a, b, c where a1 = b1 and b1 = 3"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hintforest := tt.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			switch s := stmt.AST.(type) {
			case *tree.Select:
				switch T := s.Select.(type) {
				case *tree.SelectClause:
					T.HintForest = append(T.HintForest, hintforest...)
				}
			}

			o.Init(&evalCtx, catalog)
			b := optbuilder.New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			if err := b.Build(); err != nil {
				t.Fatal(err)
			}
			root := o.Memo().RootExpr().(memo.RelExpr)
			left := root.(*memo.InnerJoinExpr).Left
			fmt.Println(left.String())
			fmt.Println("################################################")
			fmt.Println(root.String())
			fmt.Println("****************************************************************************************")

			if _, err := o.Optimize(); err != nil {
				t.Error(err)
			}
			root = o.Memo().RootExpr().(memo.RelExpr)
			//		left = root.(*memo.InnerJoinExpr).Left
			fmt.Println(left.String())
			fmt.Println("################################################")
			fmt.Println(root.String())
		})
	}
	fmt.Println("****************************************************************************************")
	fmt.Println("****************************************************************************************")
}
