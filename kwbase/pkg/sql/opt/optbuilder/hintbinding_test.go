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

package optbuilder

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
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

func CountScanNodeFromHintTree(htree tree.HintTree) (l int) {
	switch htnode := htree.(type) {
	case *tree.HintTreeJoinNode:
		l += CountScanNodeFromHintTree(htnode.Left) + CountScanNodeFromHintTree(htnode.Right)
		return l

	case *tree.HintTreeScanNode:
		l++
		return l

	default:
		panic(fmt.Sprintf("unexpected hintnode"))
	}
}

func CountJoinNodeFromHintTree(htree tree.HintTree) (l int) {
	switch htnode := htree.(type) {
	case *tree.HintTreeJoinNode:
		l++
		l += CountJoinNodeFromHintTree(htnode.Left) + CountJoinNodeFromHintTree(htnode.Right)
		return l

	case *tree.HintTreeScanNode:
		return 0

	default:
		panic(fmt.Sprintf("unexpected hintnode"))
	}
}

type JPName map[tree.Name]memo.JoinPrivate
type JEName map[tree.Name]*memo.InnerJoinExpr

func GetJoinPrivateAndName(
	RelExpr memo.RelExpr, jpname JPName, jename JEName, md *opt.Metadata,
) (Name tree.Name) {
	switch Expr := RelExpr.(type) {
	case *memo.InnerJoinExpr:
		JoinPrivate := Expr.JoinPrivate
		leftname := GetJoinPrivateAndName(Expr.Left, jpname, jename, md)
		rightname := GetJoinPrivateAndName(Expr.Right, jpname, jename, md)
		joinname := "(" + leftname + "," + rightname + ")"

		jpname[joinname] = JoinPrivate
		jename[joinname] = Expr
		return joinname

	case *memo.ScanExpr:
		tabMeta := md.TableMeta(Expr.Table)
		tab := tabMeta.Table
		return tab.Name()

	case *memo.SelectExpr:
		name := GetJoinPrivateAndName(Expr.Input, jpname, jename, md)
		return name
	}
	return
}

func GetJoinNodeNameFromHintTree(htree tree.HintTree) (Name tree.Name) {
	switch htnode := htree.(type) {
	case *tree.HintTreeJoinNode:
		leftname := GetJoinNodeNameFromHintTree(htnode.Left)
		rightname := GetJoinNodeNameFromHintTree(htnode.Right)
		Name = "(" + leftname + "," + rightname + ")"
		return

	case *tree.HintTreeScanNode:
		Name = htnode.TableName
	}
	return
}

func (b *Builder) JudgeWhetherJoinHintApplied(
	htree tree.HintTree, jpname JPName, jename JEName, t *testing.T, md *opt.Metadata,
) {
	switch htnode := htree.(type) {
	case *tree.HintTreeJoinNode:
		JoinName := GetJoinNodeNameFromHintTree(htnode)
		var ok bool
		var joinprivate memo.JoinPrivate
		var joinexpr *memo.InnerJoinExpr
		if joinprivate, ok = jpname[JoinName]; ok {
			delete(jpname, JoinName)
		}
		if ok == false {
			t.Fatal("Failed to bind join hint (joinprivate)")
		}
		if joinexpr, ok = jename[JoinName]; !ok {
			t.Fatal("Failed to bind join hint (joinexpr)")
		}

		//judge whether join method / selectivity / real order are applied
		var IsUse, DisallowMergeJoin, DisallowLookupJoin, DisallowHashJoin bool
		idx := -1
		switch htnode.HintType {
		case keys.UseHash:
			IsUse = true
			DisallowMergeJoin = true
			DisallowLookupJoin = true

		case keys.DisallowHash:
			DisallowHashJoin = true

		case keys.ForceHash:
			DisallowMergeJoin = true
			DisallowLookupJoin = true

		case keys.UseMerge:
			IsUse = true
			DisallowHashJoin = true
			DisallowLookupJoin = true

		case keys.DisallowMerge:
			DisallowMergeJoin = true

		case keys.ForceMerge:
			DisallowHashJoin = true
			DisallowLookupJoin = true

		case keys.UseLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:
				panic(fmt.Sprintf("the right of joinhintnode for lookup join must be scan/select"))
			}
			IsUse = true
			DisallowHashJoin = true
			DisallowMergeJoin = true

		case keys.DisallowLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:

			}

			DisallowLookupJoin = true

		case keys.ForceLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:
				panic(fmt.Sprintf("the right of joinhintnode for lookup join must be scan/select"))
			}

			DisallowHashJoin = true
			DisallowMergeJoin = true
		}
		if !(true == joinprivate.HintInfo.FromHintTree) {
			t.Fatal("Failed to bind join hint")
		}
		if !(IsUse == joinprivate.HintInfo.IsUse) || !(DisallowHashJoin == joinprivate.HintInfo.DisallowHashJoin) ||
			!(DisallowMergeJoin == joinprivate.HintInfo.DisallowMergeJoin) || !(DisallowLookupJoin == joinprivate.HintInfo.DisallowLookupJoin) ||
			!(idx == joinprivate.HintInfo.HintIndex) {
			t.Fatal("Failed to bind join method hint")
		}
		if !(htnode.RealJoinOrder == joinprivate.HintInfo.RealOrder) {
			t.Fatal("Failed to bind join order hint")
		}
		if !(htnode.EstimatedCardinality == joinprivate.HintInfo.EstimatedCardinality) && !(htnode.TotalCardinality == htnode.TotalCardinality) {
			t.Fatal("Failed to bind join selectivity hint")
		}
		b.JudgeWhetherJoinHintApplied(htnode.Left, jpname, jename, t, md)
		b.JudgeWhetherJoinHintApplied(htnode.Right, jpname, jename, t, md)

	case *tree.HintTreeScanNode:
	}
}

func (b *Builder) JudgeWhetherHintApplied(
	htree tree.HintTree,
	jpname JPName,
	jename JEName,
	scanprivates *[]memo.ScanPrivate,
	t *testing.T,
	md *opt.Metadata,
) {
	switch htnode := htree.(type) {
	case *tree.HintTreeJoinNode:
		JoinName := GetJoinNodeNameFromHintTree(htnode)
		var ok bool
		var joinprivate memo.JoinPrivate
		var joinexpr *memo.InnerJoinExpr
		if joinprivate, ok = jpname[JoinName]; ok {
			delete(jpname, JoinName)
		}
		if ok == false {
			t.Fatal("Failed to bind join hint (joinprivate)")
		}
		if joinexpr, ok = jename[JoinName]; !ok {
			t.Fatal("Failed to bind join hint (joinexpr)")
		}

		//judge whether join method / selectivity / real order are applied
		var IsUse, DisallowMergeJoin, DisallowLookupJoin, DisallowHashJoin bool
		idx := -1
		switch htnode.HintType {
		case keys.UseHash:
			IsUse = true
			DisallowMergeJoin = true
			DisallowLookupJoin = true

		case keys.DisallowHash:
			DisallowHashJoin = true

		case keys.ForceHash:
			DisallowMergeJoin = true
			DisallowLookupJoin = true

		case keys.UseMerge:
			IsUse = true
			DisallowHashJoin = true
			DisallowLookupJoin = true

		case keys.DisallowMerge:
			DisallowMergeJoin = true

		case keys.ForceMerge:
			DisallowHashJoin = true
			DisallowLookupJoin = true

		case keys.UseLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:
				panic(fmt.Sprintf("the right of joinhintnode for lookup join must be scan/select"))
			}
			IsUse = true
			DisallowHashJoin = true
			DisallowMergeJoin = true

		case keys.DisallowLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:

			}

			DisallowLookupJoin = true

		case keys.ForceLookup:
			switch rightexpr := joinexpr.Right.(type) {
			case *memo.ScanExpr:
				if htnode.IndexName != "" {
					tabID := rightexpr.ScanPrivate.Table
					tabMeta := md.TableMeta(tabID)
					tab := tabMeta.Table
					for i := 0; i < tab.IndexCount(); i++ {
						if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
							idx = i
						}
					}
				}

			case *memo.SelectExpr:
				switch inputexpr := rightexpr.Input.(type) {
				case *memo.ScanExpr:
					if htnode.IndexName != "" {
						tabID := inputexpr.ScanPrivate.Table
						tabMeta := md.TableMeta(tabID)
						tab := tabMeta.Table
						for i := 0; i < tab.IndexCount(); i++ {
							if tab.Index(i).Name() == tree.Name(htnode.IndexName) {
								idx = i
							}
						}
					}
				}

			default:
				panic(fmt.Sprintf("the right of joinhintnode for lookup join must be scan/select"))
			}

			DisallowHashJoin = true
			DisallowMergeJoin = true
		}
		if !(true == joinprivate.HintInfo.FromHintTree) {
			t.Fatal("Failed to bind join hint")
		}
		if !(IsUse == joinprivate.HintInfo.IsUse) || !(DisallowHashJoin == joinprivate.HintInfo.DisallowHashJoin) ||
			!(DisallowMergeJoin == joinprivate.HintInfo.DisallowMergeJoin) || !(DisallowLookupJoin == joinprivate.HintInfo.DisallowLookupJoin) ||
			!(idx == joinprivate.HintInfo.HintIndex) {
			t.Fatal("Failed to bind join method hint")
		}
		if !(htnode.RealJoinOrder == joinprivate.HintInfo.RealOrder) {
			t.Fatal("Failed to bind join order hint")
		}
		if !(htnode.EstimatedCardinality == joinprivate.HintInfo.EstimatedCardinality) && !(htnode.TotalCardinality == htnode.TotalCardinality) {
			t.Fatal("Failed to bind join selectivity hint")
		}
		b.JudgeWhetherHintApplied(htnode.Left, jpname, jename, scanprivates, t, md)
		b.JudgeWhetherHintApplied(htnode.Right, jpname, jename, scanprivates, t, md)

	case *tree.HintTreeScanNode:
		for n := 0; n < len(*scanprivates); n++ {
			tabMeta := md.TableMeta((*scanprivates)[n].Table)
			tab := tabMeta.Table
			if tab.Name() == htnode.TableName {
				var indexhintapply bool
				if len(htnode.IndexName) == 0 && len((*scanprivates)[n].Flags.IndexName) == 0 {
					indexhintapply = true
				}
				if len(htnode.IndexName) != 0 {
					if htnode.HintType == keys.UseIndexScan || htnode.HintType == keys.UseIndexOnly ||
						htnode.HintType == keys.IgnoreIndexScan || htnode.HintType == keys.IgnoreIndexOnly {
						for _, sidx := range (*scanprivates)[n].Flags.IndexName {
							for _, hidx := range htnode.IndexName {
								if sidx == tree.Name(hidx) {
									indexhintapply = true
									break
								}
							}
							if indexhintapply == true {
								break
							}
						}
					}
					if htnode.HintType == keys.ForceIndexScan || htnode.HintType == keys.ForceIndexOnly {
						if len((*scanprivates)[n].Flags.IndexName) == 1 &&
							(*scanprivates)[n].Flags.IndexName[0] == tree.Name(htnode.IndexName[0]) {
							indexhintapply = true
						}
					}
				}

				if !((*scanprivates)[n].Flags.HintType == htnode.HintType) ||
					!indexhintapply ||
					!((*scanprivates)[n].Flags.TotalCardinality == htnode.TotalCardinality) ||
					!((*scanprivates)[n].Flags.EstimatedCardinality == htnode.EstimatedCardinality) ||
					!((*scanprivates)[n].Flags.LeadingTable == htnode.LeadingTable) {
					t.Fatal("Failed to bind Access Method Hint")
				} else {
					*scanprivates = append((*scanprivates)[:n], (*scanprivates)[n+1:]...)
					n--
				}
			}
		}

	default:
		panic(fmt.Sprintf("unexpected hintnode"))
	}
}

// Check if there is a leading table hint in the current HintForest, and if so, obtain the leading table
func IfLeadingInHintForest(hinttree tree.HintTree) (bool, tree.Name) {
	switch htnode := hinttree.(type) {
	case *tree.HintTreeJoinNode:
		ifleading, leadingtable := IfLeadingInHintForest(htnode.Left)
		return ifleading, leadingtable

	case *tree.HintTreeScanNode:
		if htnode.LeadingTable == true {
			return true, htnode.TableName
		}
		return false, ""

	default:
		return false, ""
	}
}

// Check if the leading table hint is successfully bound
func JugdeWhetherLeadingHintBound(
	relexpr memo.RelExpr, leadingtable tree.Name, t *testing.T, md *opt.Metadata, isright bool,
) {
	switch expr := relexpr.(type) {
	case *memo.ProjectExpr:
		JugdeWhetherLeadingHintBound(expr.Input, leadingtable, t, md, isright)

	case *memo.SelectExpr:
		JugdeWhetherLeadingHintBound(expr.Input, leadingtable, t, md, isright)

	case *memo.InnerJoinExpr:
		if isright == false {
			if expr.HintInfo.LeadingTable == false {
				t.Fatal("Failed to bind Leading Table Hint")
			} else {
				JugdeWhetherLeadingHintBound(expr.Left, leadingtable, t, md, false)
				JugdeWhetherLeadingHintBound(expr.Right, leadingtable, t, md, true)
			}
		} else {
			if expr.HintInfo.LeadingTable == true {
				t.Fatal("Failed to bind Leading Table Hint")
			} else {
				JugdeWhetherLeadingHintBound(expr.Left, leadingtable, t, md, true)
				JugdeWhetherLeadingHintBound(expr.Right, leadingtable, t, md, true)
			}
		}

	case *memo.ScanExpr:
		tabMeta := md.TableMeta(expr.Table)
		tab := tabMeta.Table
		if isright == true {
			if tab.Name() == leadingtable {
				t.Fatal("The right of join should not has leading")
			}
		} else {
			if tab.Name() != leadingtable {
				t.Fatal("Failed to bind Leading Table Hint")
			}
		}
	}
}

func TestHintBinding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE a (a1 INT PRIMARY KEY, a2 INT, a3 STRING, a4 INT, INDEX a2 (a2), INDEX a4 (a4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE b (b1 INT PRIMARY KEY, b2 INT, b3 STRING, b4 INT, INDEX b2 (b2), INDEX b4 (b4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE c (c1 INT PRIMARY KEY, c2 INT, c3 STRING, c4 INT, INDEX c2 (c2), INDEX c4 (c4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE d (d1 INT PRIMARY KEY, d2 INT, d3 STRING, d4 INT, INDEX d2 (d2), INDEX b4 (d4))")
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalog.ExecuteDDL("CREATE TABLE e (e1 INT PRIMARY KEY, e2 INT, e3 STRING, e4 INT, INDEX e2 (e2), INDEX e4 (e4))")
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	var o xform.Optimizer
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	//Scan Hint
	t.Run("Scan Hint", func(t *testing.T) {
		type Test struct {
			TestSQL        string
			TestHintForest tree.HintForest
		}
		tests := []Test{
			//one non-exist index
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"aaa", "a4"}},
			}},
			//single table + no filter
			{"SELECT * FROM a", []tree.HintTree{}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			//single table + filter
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			//two tables + no filter + single table hint
			{"SELECT * FROM a,b ", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			//two tables + no filter + two tables hint
			{"SELECT * FROM a,b ", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			//two tables + filter + single table hint
			{"SELECT * FROM a,b WHERE b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			//two tables + filter + two tables hint
			{"SELECT * FROM a,b WHERE b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			//three tables
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"a2"}, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			{"SELECT * FROM a,b,c WHERE a1>1 AND b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, IndexName: []string{"c2"}, TableName: "c"},
			}},
		}

		for i, test := range tests {
			sql := test.TestSQL
			hintforest := test.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			o.Init(&evalCtx, catalog)
			b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			inScope := b.allocScope()

			switch stmt := b.stmt.(type) {
			case *tree.Select:
				locking := noRowLocking
				orderBy := stmt.OrderBy
				locking.apply(stmt.Locking)

				switch T := stmt.Select.(type) {
				case *tree.SelectClause:
					T.HintForest = append(T.HintForest, hintforest...)
					outScope := b.buildSelectClause(T, orderBy, nil, locking, nil, inScope)
					scanprivates := GetScanPrivate(outScope.expr)
					ls := len(scanprivates)
					if scanprivates == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					md := b.factory.Metadata()
					for n := 0; n < len(scanprivates); n++ {
						tabMeta := md.TableMeta(scanprivates[n].Table)
						tab := tabMeta.Table
						for _, hintnode := range hintforest {
							switch node := hintnode.(type) {
							case *tree.HintTreeScanNode:
								if tab.Name() == node.TableName {
									var indexhintapply bool
									if len(node.IndexName) == 0 && len(scanprivates[n].Flags.IndexName) == 0 {
										indexhintapply = true
									}
									if len(node.IndexName) != 0 {
										if node.HintType == keys.UseIndexScan || node.HintType == keys.UseIndexOnly ||
											node.HintType == keys.IgnoreIndexScan || node.HintType == keys.IgnoreIndexOnly {
											for _, sidx := range scanprivates[n].Flags.IndexName {
												for _, hidx := range node.IndexName {
													if sidx == tree.Name(hidx) {
														indexhintapply = true
														break
													}
												}
												if indexhintapply == true {
													break
												}
											}
										}
										if node.HintType == keys.ForceIndexScan || node.HintType == keys.ForceIndexOnly {
											if len(scanprivates[n].Flags.IndexName) == 1 &&
												scanprivates[n].Flags.IndexName[0] == tree.Name(node.IndexName[0]) {
												indexhintapply = true
											}
										}
									}

									if !(scanprivates[n].Flags.HintType == node.HintType) ||
										!indexhintapply ||
										!(scanprivates[n].Flags.TotalCardinality == node.TotalCardinality) ||
										!(scanprivates[n].Flags.EstimatedCardinality == node.EstimatedCardinality) {
										t.Fatal("Failed to bind Access Method Hint")
									} else {
										scanprivates = append(scanprivates[:n], scanprivates[n+1:]...)
										n--
									}
								}
							default:
								panic(fmt.Sprintf("unexpected hintnode for sql%d", i))
							}
						}
					}

					var lhs int
					for _, htree := range hintforest {
						lhs += CountScanNodeFromHintTree(htree)
					}
					if len(scanprivates) != (ls - lhs) {
						t.Fatal("Exist non-applied scan hint")
					}

					for _, scanprivate := range scanprivates {
						if !(scanprivate.Flags.HintType == keys.NoScanHint) ||
							!(scanprivate.Flags.IndexName == nil) ||
							!(scanprivate.Flags.TotalCardinality == 0) ||
							!(scanprivate.Flags.EstimatedCardinality == 0) {
							t.Fatal("Failed for no hint scan private")
						}
					}

				}
			default:
				panic(fmt.Sprintf("unexpected query type for sql%d", i))
			}
			fmt.Println("PASS SQL", i)
		}

	})

	//Join Hint
	t.Run("Join Hint", func(t *testing.T) {
		type Test struct {
			TestSQL        string
			TestHintForest tree.HintForest
		}
		tests := []Test{
			//one join order
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			//one join method
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			//one join order
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			//one join selectivity
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//join method + order + selectivity
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//nested join node
			{"SELECT * FROM a,b,c where a1>1 AND b2>2 AND c1=1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "c"},
					Right: &tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
						Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true}, RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//no join hint
			{"SELECT * FROM a,b,c WHERE a1>1 AND b2 = 1 ", []tree.HintTree{}},
			//nested join node
			{"SELECT * FROM a,b,c,d,e WHERE a1>1 AND b2=1 AND c2 >1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "d"},
					Right: &tree.HintTreeScanNode{TableName: "e"}, RealJoinOrder: true},
					Right: &tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
						Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true}, RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//two join node
			{"SELECT * FROM a,b,c,d,e WHERE a1>1 AND b2=1 AND c2 >1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "d"},
					Right: &tree.HintTreeScanNode{TableName: "e"}, RealJoinOrder: true},
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			//test for rbo rules
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true, TotalCardinality: 20, EstimatedCardinality: 10},
			}},
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true, IndexName: "b2", TotalCardinality: 20, EstimatedCardinality: 10},
			}},
		}

		for i, test := range tests {
			sql := test.TestSQL
			hintforest := test.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			o.Init(&evalCtx, catalog)
			b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			inScope := b.allocScope()

			switch stmt := b.stmt.(type) {
			case *tree.Select:
				locking := noRowLocking
				orderBy := stmt.OrderBy
				locking.apply(stmt.Locking)

				switch T := stmt.Select.(type) {
				case *tree.SelectClause:
					jpname := make(JPName, 0)
					jename := make(JEName, 0)
					T.HintForest = append(T.HintForest, hintforest...)
					outScope := b.buildSelectClause(T, orderBy, nil, locking, nil, inScope)
					md := b.factory.Metadata()
					GetJoinPrivateAndName(outScope.expr, jpname, jename, md)
					ljp := len(jpname)
					lje := len(jename)
					if ljp != lje {
						panic(fmt.Sprintf("The number of joinprivate does not match the number of joinexpr for sql%d", i))
					}

					if jpname == nil || jename == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}

					for _, hintnode := range hintforest {
						b.JudgeWhetherJoinHintApplied(hintnode, jpname, jename, t, md)
					}

					var lhj int
					for _, htree := range hintforest {
						lhj += CountJoinNodeFromHintTree(htree)
					}
					if len(jpname) != (ljp - lhj) {
						t.Fatal("Exist non-applied join hint")
					}

					for _, jprivate := range jpname {
						if !(jprivate.HintInfo.FromHintTree == false) || !(jprivate.HintInfo.IsUse == false) || !(jprivate.HintInfo.DisallowMergeJoin == false) || !(jprivate.HintInfo.DisallowHashJoin == false) ||
							!(jprivate.HintInfo.DisallowLookupJoin == false) || !(jprivate.HintInfo.HintIndex == -1) || !(jprivate.HintInfo.RealOrder == false) ||
							!(jprivate.HintInfo.EstimatedCardinality == 0) || !(jprivate.HintInfo.TotalCardinality == 0) {
							t.Fatal("Failed for no hint join private")
						}
					}

				}
			default:
				panic(fmt.Sprintf("unexpected query type for sql%d", i))
			}
			fmt.Println("PASS SQL", i)
		}

	})

	//Join + Scan Hint
	t.Run("Join + Scan Hint", func(t *testing.T) {
		type Test struct {
			TestSQL        string
			TestHintForest tree.HintForest
		}
		tests := []Test{
			//Join+Scan
			{"SELECT * FROM a,b,c,d,e WHERE a1>1 AND b2=1 AND c2 >1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "d"},
					Right: &tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "e"}, RealJoinOrder: true},
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
					Right: &tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "b", IndexName: []string{"b2"}}, IndexName: "b2", RealJoinOrder: true},
			}},
			//single table + no filter

			{"SELECT * FROM a", []tree.HintTree{}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			//single table + filter
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceTableScan, TableName: "a"},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexScan, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.IgnoreIndexOnly, TableName: "a", IndexName: []string{"a2", "a4"}},
			}},
			{"SELECT * FROM a WHERE a2 > 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, TableName: "a", IndexName: []string{"a2"}},
			}},
			//two tables + no filter + single table hint
			{"SELECT * FROM a,b ", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			//two tables + no filter + two tables hint
			{"SELECT * FROM a,b ", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			//two tables + filter + single table hint
			{"SELECT * FROM a,b WHERE b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
			}},
			//two tables + filter + two tables hint
			{"SELECT * FROM a,b WHERE b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			//three tables
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"a2"}, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
			}},
			{"SELECT * FROM a,b,c WHERE a1>1 AND b2 = 1", []tree.HintTree{
				&tree.HintTreeScanNode{HintType: keys.UseTableScan, TableName: "a"},
				&tree.HintTreeScanNode{HintType: keys.UseIndexScan, IndexName: []string{"b2"}, TableName: "b"},
				&tree.HintTreeScanNode{HintType: keys.ForceIndexScan, IndexName: []string{"c2"}, TableName: "c"},
			}},
			//one join method
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceMerge, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.UseLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			//one join order
			{"SELECT * FROM a,b,c", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			//one join cardinality
			{"SELECT * FROM a,b,c WHERE a1=b1  ", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//join method + order + cardinality
			{"SELECT * FROM a,b,c WHERE a2=b2", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//nested join node
			{"SELECT * FROM a,b,c where b1=c1 AND a1>1 AND b2>2 AND c1=1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "c"},
					Right: &tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
						Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true}, RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//no join hint
			{"SELECT * FROM a,b,c WHERE a1>1 AND b2 = 1 ", []tree.HintTree{}},
			//nested join node
			{"SELECT * FROM a,b,c,d,e WHERE a1=b1 AND a1>1 AND b2=1 AND c2 >1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "d"},
					Right: &tree.HintTreeScanNode{TableName: "e"}, RealJoinOrder: true},
					Right: &tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
						Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true}, RealJoinOrder: true, TotalCardinality: 10, EstimatedCardinality: 1},
			}},
			//two join node
			{"SELECT * FROM a,b,c,d,e WHERE a1>1 AND b2=1 AND c2 >1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.DisallowMerge, Left: &tree.HintTreeScanNode{TableName: "d"},
					Right: &tree.HintTreeScanNode{TableName: "e"}, RealJoinOrder: true},
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, IndexName: "b2", RealJoinOrder: true},
			}},
			//test for rbo rules
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceHash, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true},
			}},
			{"SELECT * FROM a,b,c WHERE a1=b1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true, TotalCardinality: 20, EstimatedCardinality: 10},
			}},
			{"SELECT * FROM a,b,c WHERE b1=c1", []tree.HintTree{
				&tree.HintTreeJoinNode{HintType: keys.ForceLookup, Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, RealJoinOrder: true, IndexName: "b2", TotalCardinality: 20, EstimatedCardinality: 10},
			}},
		}

		for i, test := range tests {
			sql := test.TestSQL
			hintforest := test.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err = semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			o.Init(&evalCtx, catalog)
			b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			inScope := b.allocScope()

			switch stmt := b.stmt.(type) {
			case *tree.Select:
				locking := noRowLocking
				orderBy := stmt.OrderBy
				locking.apply(stmt.Locking)

				switch T := stmt.Select.(type) {
				case *tree.SelectClause:
					jpname := make(JPName, 0)
					jename := make(JEName, 0)
					T.HintForest = append(T.HintForest, hintforest...)
					outScope := b.buildSelectClause(T, orderBy, nil, locking, nil, inScope)
					md := b.factory.Metadata()
					GetJoinPrivateAndName(outScope.expr, jpname, jename, md)
					ljp := len(jpname)
					lje := len(jename)
					if ljp != lje {
						panic(fmt.Sprintf("The number of joinprivate does not match the number of joinexpr for sql%d", i))
					}

					if jpname == nil || jename == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					scanprivates := GetScanPrivate(outScope.expr)
					ls := len(scanprivates)
					if scanprivates == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					for _, hintnode := range hintforest {
						b.JudgeWhetherHintApplied(hintnode, jpname, jename, &scanprivates, t, md)
					}

					var lhs, lhj int
					for _, htree := range hintforest {
						lhs += CountScanNodeFromHintTree(htree)
						lhj += CountJoinNodeFromHintTree(htree)
					}
					if len(scanprivates) != (ls-lhs) || len(jpname) != (ljp-lhj) {
						t.Fatal("Exist non-applied hint")
					}

					for _, jprivate := range jpname {
						if !(jprivate.HintInfo.FromHintTree == false) || !(jprivate.HintInfo.IsUse == false) || !(jprivate.HintInfo.DisallowMergeJoin == false) || !(jprivate.HintInfo.DisallowHashJoin == false) ||
							!(jprivate.HintInfo.DisallowLookupJoin == false) || !(jprivate.HintInfo.HintIndex == -1) || !(jprivate.HintInfo.RealOrder == false) ||
							!(jprivate.HintInfo.EstimatedCardinality == 0) || !(jprivate.HintInfo.TotalCardinality == 0) {
							t.Fatal("Failed for no hint join private")
						}
					}
					for _, scanprivate := range scanprivates {
						if !(scanprivate.Flags.HintType == keys.NoScanHint) ||
							!(scanprivate.Flags.IndexName == nil) ||
							!(scanprivate.Flags.TotalCardinality == 0) ||
							!(scanprivate.Flags.EstimatedCardinality == 0) {
							t.Fatal("Failed for no hint scan private")
						}
					}

				}
			default:
				panic(fmt.Sprintf("unexpected query type for sql%d", i))
			}
			fmt.Println("PASS SQL", i)
		}

	})

	//leading table hint
	t.Run("leading table hint", func(t *testing.T) {
		type Test struct {
			TestSQL        string
			TestHintForest tree.HintForest
		}
		tests := []Test{
			//leading(one scan node) + no filter
			{"SELECT * FROM a,b,c,d,e", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
			}},
			//leading(one scan node) + other scan nodes + no filter
			{"SELECT * FROM a,b,c,d,e", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "a", HintType: keys.UseTableScan},
				&tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
				&tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan},
			}},
			//leading(one scan node) + other join nodes
			{"SELECT * FROM a,b,c,d,e where a.a1 = b.b1", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, RealJoinOrder: true},
			}},
			//leading(one scan node) + other join nodes
			{"SELECT * FROM a,b,c,d,e where a.a1 = b.b1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, RealJoinOrder: true},
				&tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
			}},
			//leading(one scan node) + other join nodes
			{"SELECT * FROM a,b,c,d,e where a.a1 = b.b1 and c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, RealJoinOrder: true},
				&tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
			}},
			//leading(one join node) + no filter
			{"SELECT * FROM a,b,c,d,e", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
			}},
			//leading(one join node)
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
			}},
			//leading(one join node) + other scan nodes
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
				&tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan},
			}},
			//leading(one join node) + other scan nodes
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
			}},
			//leading(one join node) + other join nodes
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "d", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
			}},
			// leading(b,c)
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "d", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan, LeadingTable: true}, TotalCardinality: 10, EstimatedCardinality: 100, LeadingTable: true},
			}},
			// leading((c,b),a)
			{"SELECT * FROM a,b,c,d,e where c.c1 = e.e1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "d", HintType: keys.UseTableScan},
					Right: &tree.HintTreeScanNode{TableName: "e", HintType: keys.UseTableScan}, TotalCardinality: 10, EstimatedCardinality: 100},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeJoinNode{
					Left:  &tree.HintTreeScanNode{TableName: "c", LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "b", HintType: keys.UseTableScan, LeadingTable: true}, TotalCardinality: 10, EstimatedCardinality: 100, LeadingTable: true},
					Right: &tree.HintTreeScanNode{TableName: "a", HintType: keys.UseTableScan, LeadingTable: true}, TotalCardinality: 10, EstimatedCardinality: 100, LeadingTable: true},
			}},
		}

		for i, test := range tests {
			sql := test.TestSQL
			hintforest := test.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err = semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			o.Init(&evalCtx, catalog)
			b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			inScope := b.allocScope()

			switch stmt := b.stmt.(type) {
			case *tree.Select:
				locking := noRowLocking
				orderBy := stmt.OrderBy
				locking.apply(stmt.Locking)

				switch T := stmt.Select.(type) {
				case *tree.SelectClause:
					jpname := make(JPName, 0)
					jename := make(JEName, 0)
					T.HintForest = append(T.HintForest, hintforest...)
					outScope := b.buildSelectClause(T, orderBy, nil, locking, nil, inScope)
					md := b.factory.Metadata()
					GetJoinPrivateAndName(outScope.expr, jpname, jename, md)
					ljp := len(jpname)
					lje := len(jename)
					if ljp != lje {
						panic(fmt.Sprintf("The number of joinprivate does not match the number of joinexpr for sql%d", i))
					}

					if jpname == nil || jename == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					scanprivates := GetScanPrivate(outScope.expr)
					ls := len(scanprivates)
					if scanprivates == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					for _, hintnode := range hintforest {
						b.JudgeWhetherHintApplied(hintnode, jpname, jename, &scanprivates, t, md)
					}

					// Check the leading table to ensure that it is in the bottom left corner of the initial memo
					// and that the LeadingTable property of JoinFlags in all JoinPrivates containing the leading table is true
					var ifleading bool
					var leadingtable tree.Name
					for _, htree := range hintforest {
						ifleading, leadingtable = IfLeadingInHintForest(htree)
						if ifleading == true {
							break
						}
					}
					if ifleading == true {
						JugdeWhetherLeadingHintBound(outScope.expr, leadingtable, t, md, false)
					}

					//check for no hint private
					var lhs, lhj int
					for _, htree := range hintforest {
						lhs += CountScanNodeFromHintTree(htree)
						lhj += CountJoinNodeFromHintTree(htree)
					}
					if len(scanprivates) != (ls-lhs) || len(jpname) != (ljp-lhj) {
						t.Fatal("Exist non-applied hint")
					}

					for _, jprivate := range jpname {
						if jprivate.HintInfo.FromHintTree == true {
							if jprivate.HintInfo.LeadingTable == true && ifleading {
								if !(jprivate.HintInfo.IsUse == false) || !(jprivate.HintInfo.DisallowMergeJoin == false) || !(jprivate.HintInfo.DisallowHashJoin == false) ||
									!(jprivate.HintInfo.DisallowLookupJoin == false) || !(jprivate.HintInfo.HintIndex == -1) || !(jprivate.HintInfo.RealOrder == false) ||
									!(jprivate.HintInfo.EstimatedCardinality == 0) || !(jprivate.HintInfo.TotalCardinality == 0) {
									t.Fatal("Failed for no hint join private")
								}
							} else {
								t.Fatal("Failed for no hint join private")
							}
						} else {
							if !(jprivate.HintInfo.IsUse == false) || !(jprivate.HintInfo.DisallowMergeJoin == false) || !(jprivate.HintInfo.DisallowHashJoin == false) ||
								!(jprivate.HintInfo.DisallowLookupJoin == false) || !(jprivate.HintInfo.HintIndex == -1) || !(jprivate.HintInfo.RealOrder == false) ||
								!(jprivate.HintInfo.EstimatedCardinality == 0) || !(jprivate.HintInfo.TotalCardinality == 0) || !(jprivate.HintInfo.LeadingTable == false) {
								t.Fatal("Failed for no hint join private")
							}
						}
					}
					for _, scanprivate := range scanprivates {
						if !(scanprivate.Flags.HintType == keys.NoScanHint) ||
							!(scanprivate.Flags.IndexName == nil) ||
							!(scanprivate.Flags.TotalCardinality == 0) ||
							!(scanprivate.Flags.EstimatedCardinality == 0) ||
							!(scanprivate.Flags.LeadingTable == false) {
							t.Fatal("Failed for no hint scan private")
						}
					}

				}
			default:
				panic(fmt.Sprintf("unexpected query type for sql%d", i))
			}
			fmt.Println("PASS SQL", i)
		}

	})
	//Cardinality Hint
	t.Run("Cardinality Hint", func(t *testing.T) {
		type Test struct {
			TestSQL        string
			TestHintForest tree.HintForest
		}
		tests := []Test{
			//Scan cardinality hint
			//single table
			{"SELECT * FROM a WHERE a1>1", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "a", TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//single table
			{"SELECT * FROM a,b WHERE a1<1 and b1=1", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "a", TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//two tables
			{"SELECT * FROM a,b WHERE a1>1 and b1=1", []tree.HintTree{
				&tree.HintTreeScanNode{TableName: "a", TotalCardinality: 100, EstimatedCardinality: 10},
				//	&tree.HintTreeScanNode{TableName: "b", TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//Join cardinality hint
			//one join
			{"SELECT * FROM a,b,c,d WHERE a1=b1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//one join
			{"SELECT * FROM a,b,c,d WHERE a1=b1 and c1=d1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//two joins
			{"SELECT * FROM a,b,c,d WHERE a1=b1 and c1=d1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
					Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 100, EstimatedCardinality: 10},
				&tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "c"},
					Right: &tree.HintTreeScanNode{TableName: "d"}, TotalCardinality: 100, EstimatedCardinality: 10},
			}},
			//nested join cardinality hint
			{"SELECT * FROM a,b,c,d,e WHERE a1=b1 AND c1=d1 AND a1=d1", []tree.HintTree{
				&tree.HintTreeJoinNode{Left: &tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "d"},
					Right: &tree.HintTreeScanNode{TableName: "c"}, TotalCardinality: 100, EstimatedCardinality: 10},
					Right: &tree.HintTreeJoinNode{Left: &tree.HintTreeScanNode{TableName: "a"},
						Right: &tree.HintTreeScanNode{TableName: "b"}, TotalCardinality: 100, EstimatedCardinality: 10}, RealJoinOrder: true, TotalCardinality: 100, EstimatedCardinality: 1},
			}},
		}

		for i, test := range tests {
			sql := test.TestSQL
			hintforest := test.TestHintForest
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			if err = semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
				t.Fatal(err)
			}
			semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
			o.Init(&evalCtx, catalog)
			b := New(ctx, &semaCtx, &evalCtx, catalog, o.Factory(), stmt.AST)
			inScope := b.allocScope()

			switch stmt := b.stmt.(type) {
			case *tree.Select:
				locking := noRowLocking
				orderBy := stmt.OrderBy
				locking.apply(stmt.Locking)

				switch T := stmt.Select.(type) {
				case *tree.SelectClause:
					jpname := make(JPName, 0)
					jename := make(JEName, 0)
					T.HintForest = append(T.HintForest, hintforest...)
					outScope := b.buildSelectClause(T, orderBy, nil, locking, nil, inScope)
					md := b.factory.Metadata()
					GetJoinPrivateAndName(outScope.expr, jpname, jename, md)
					ljp := len(jpname)
					lje := len(jename)
					if ljp != lje {
						panic(fmt.Sprintf("The number of joinprivate does not match the number of joinexpr for sql%d", i))
					}

					if jpname == nil || jename == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					scanprivates := GetScanPrivate(outScope.expr)
					ls := len(scanprivates)
					if scanprivates == nil {
						panic(fmt.Sprintf("unexpected RelExpr in sql%d", i))
					}
					for _, hintnode := range hintforest {
						b.JudgeWhetherHintApplied(hintnode, jpname, jename, &scanprivates, t, md)
					}

					var lhs, lhj int
					for _, htree := range hintforest {
						lhs += CountScanNodeFromHintTree(htree)
						lhj += CountJoinNodeFromHintTree(htree)
					}
					if len(scanprivates) != (ls-lhs) || len(jpname) != (ljp-lhj) {
						t.Fatal("Exist non-applied hint")
					}

					for _, jprivate := range jpname {
						if !(jprivate.HintInfo.FromHintTree == false) || !(jprivate.HintInfo.IsUse == false) || !(jprivate.HintInfo.DisallowMergeJoin == false) || !(jprivate.HintInfo.DisallowHashJoin == false) ||
							!(jprivate.HintInfo.DisallowLookupJoin == false) || !(jprivate.HintInfo.HintIndex == -1) || !(jprivate.HintInfo.RealOrder == false) ||
							!(jprivate.HintInfo.EstimatedCardinality == 0) || !(jprivate.HintInfo.TotalCardinality == 0) {
							t.Fatal("Failed for no hint join private")
						}
					}
					for _, scanprivate := range scanprivates {
						if !(scanprivate.Flags.HintType == keys.NoScanHint) ||
							!(scanprivate.Flags.IndexName == nil) ||
							!(scanprivate.Flags.TotalCardinality == 0) ||
							!(scanprivate.Flags.EstimatedCardinality == 0) {
							t.Fatal("Failed for no hint scan private")
						}
					}

				}
			default:
				panic(fmt.Sprintf("unexpected query type for sql%d", i))
			}
			fmt.Println("PASS SQL", i)
		}

	})
}
