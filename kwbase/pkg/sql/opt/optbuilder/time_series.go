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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// Interpolate symbol
const Interpolate = "interpolate"

// Gapfill symbol
const Gapfill = "time_bucket_gapfill"

// Gapfillinternal symbol
const Gapfillinternal = "time_bucket_gapfill_internal"

// buildTimeSeriesScan builds a memo group for a TSScanOp expression on the
// given table.
//
// Parameters:
//   - indexFlags: only use when it is hint select for show tag values.
//     This table access mode is only tag.
func (b *Builder) buildTimeSeriesScan(
	tabMeta *opt.TableMeta, indexFlags *tree.IndexFlags, tabName *tree.TableName, inScope *scope,
) (outScope *scope) {
	b.PhysType = tree.TS
	b.factory.Memo().SetFlag(opt.IncludeTSTable)
	tab := tabMeta.Table
	tabID := tabMeta.MetaID
	colCount := tabMeta.Table.ColumnCount()

	var tabColIDs opt.ColSet
	outScope = inScope.push()
	outScope.cols = make([]scopeColumn, 0, colCount)
	if outScope.TableType == nil {
		outScope.TableType = make(map[tree.TableType]int)
	}
	outScope.TableType.Insert(tab.GetTableType())

	for i := 0; i < colCount; i++ {
		col := tab.Column(i)
		name := col.ColName()
		colID := tabID.ColumnID(i)
		tabColIDs.Add(colID)
		outScope.cols = append(outScope.cols, scopeColumn{
			id:     colID,
			name:   name,
			table:  tabMeta.Alias,
			typ:    col.DatumType(),
			hidden: col.IsHidden(),
		})
	}

	private := memo.TSScanPrivate{Table: tabID, Cols: tabColIDs, AccessMode: -1}

	if indexFlags != nil {
		if indexFlags.FromHintTree {
			private.HintType = indexFlags.HintType
			if indexFlags.HintType == keys.TagOnlyHint {
				private.AccessMode = int(execinfrapb.TSTableReadMode_onlyTag)
			}
		}
	}

	// construct memo.ScanExpr.
	outScope.expr = b.factory.ConstructTSScan(&private)

	return outScope
}

// checkoutInterpolate check whether there is an interpolate agg, and then
// limit the use of interpolate(). It must be used with time_bucket_gapfill.
func checkoutInterpolate(groupBy tree.GroupBy, s *scope) bool {
	var hasInterpolate bool
	if s.groupby != nil {
		for _, info := range s.groupby.aggs {
			if getAggName(info) == Interpolate {
				hasInterpolate = true
			}
		}
		if hasInterpolate && !s.hasGapfill {
			panic(pgerror.New(pgcode.FeatureNotSupported, "interpolate should be used with time_bucket_gapfill"))
		}
	}
	return hasInterpolate
}

// implicitOrderInInterpolate implicitly construct order by for gapfill and interpolate.
func implicitOrderInInterpolate(groupBy tree.GroupBy, s *scope) tree.OrderBy {
	if s.groupby != nil && groupBy != nil {
		// We need order by for gapfill and interpolate.
		return tree.OrderBy{
			{Expr: groupBy[0]},
		}
	}
	return nil
}

// checkoutGroupByGapfill check has time_bucket_gapfill in group by,
// limit time_bucket_gapfill() should be used with group by time_bucket_gapfill(),
// interpolate() should be used with group by time_bucket_gapfill().
func checkoutGroupByGapfill(s *scope, sel *tree.SelectClause, hasInterpolate bool) {
	if s.groupby != nil {
		var hasGapfill bool
		for key := range s.groupby.groupStrs {
			if strings.Contains(key, Gapfill) {
				hasGapfill = true
			}
		}

		if hasInterpolate && (!hasGapfill || sel.GroupBy == nil) {
			panic(pgerror.New(pgcode.FeatureNotSupported, "interpolate should be used with group by time_bucket_gapfill"))
		}

		if !hasInterpolate && (!hasGapfill || sel.GroupBy == nil) {
			panic(pgerror.New(pgcode.FeatureNotSupported, "time_bucket_gapfill should be use with group by time_bucket_gapfill"))
		}
		getFuncName := func(f *tree.FuncExpr) string {
			if define, ok := f.Func.FunctionReference.(*tree.FunctionDefinition); ok {
				return define.Name
			}
			return ""
		}
		if funcInGroup, ok := sel.GroupBy[0].(*tree.FuncExpr); ok {
			for _, expr := range sel.Exprs {
				if funcInSelect, ok := expr.Expr.(*tree.FuncExpr); ok {
					if strings.Contains(getFuncName(funcInGroup), Gapfill) && strings.Contains(getFuncName(funcInSelect), Gapfill) {
						if funcInGroup.Exprs.String() != funcInSelect.Exprs.String() {
							panic(pgerror.New(pgcode.Grouping, "The parameters of the time_bucket_gapfill() in select list and group by must be consistent"))
						}
					}
				}
			}
		}
	}
}

// getAggName get agg func name from aggregateInfo.
func getAggName(agg aggregateInfo) string {
	if funcDefine, ok := agg.FuncExpr.Func.FunctionReference.(*tree.FunctionDefinition); ok {
		return funcDefine.Name
	}
	return ""
}

// resetFuncExpr copy the FuncExpr of gapfill_internal , and then
// transformed into the gapfill built-in function expression.
func resetFuncExpr(info *aggregateInfo) *tree.FuncExpr {
	name := getAggName(*info)
	if name == Gapfillinternal {
		temp := *info.FuncExpr
		res := &temp
		if _, ok := res.Func.FunctionReference.(*tree.FunctionDefinition); ok {
			res.Func.FunctionReference = &tree.UnresolvedName{
				NumParts: 1,
				Parts:    [4]string{Gapfill},
			}
		}
		(*res).Reset()
		return res
	}
	return nil
}
