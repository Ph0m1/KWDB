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

package ordering

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

func scalarGroupByBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	// Scalar group by requires the ordering in its private.
	return parent.(*memo.ScalarGroupByExpr).Ordering
}

func groupByCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// GroupBy may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	groupBy := expr.(*memo.GroupByExpr)
	return required.CanProjectCols(groupBy.GroupingCols) && required.Intersects(&groupBy.Ordering)
}

func groupByBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	groupBy := parent.(*memo.GroupByExpr)
	result := *required
	if !result.SubsetOfCols(groupBy.GroupingCols) {
		result = result.Copy()
		result.ProjectCols(groupBy.GroupingCols)
	}

	result = result.Intersection(&groupBy.Ordering)

	// The FD set of the input doesn't "pass through" to the GroupBy FD set;
	// check the ordering to see if it can be simplified with respect to the
	// input FD set.
	result.Simplify(&groupBy.Input.Relational().FuncDeps)

	return result
}

func groupByBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	groupBy := expr.(*memo.GroupByExpr)
	provided := groupBy.Input.ProvidedPhysical().Ordering
	inputFDs := &groupBy.Input.Relational().FuncDeps

	// Since the input's provided ordering has to satisfy both <required> and the
	// GroupBy internal ordering, it may need to be trimmed.
	provided = trimProvided(provided, required, inputFDs)
	return remapProvided(provided, inputFDs, groupBy.GroupingCols)
}

func distinctOnCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// DistinctOn may require a certain ordering of its input, but can also pass
	// through a stronger ordering on the grouping columns.
	return required.Intersects(&expr.Private().(*memo.GroupingPrivate).Ordering)
}

func distinctOnBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	// The FD set of the input doesn't "pass through" to the DistinctOn FD set;
	// check the ordering to see if it can be simplified with respect to the input
	// FD set.
	result := required.Intersection(&parent.Private().(*memo.GroupingPrivate).Ordering)
	result.Simplify(&parent.Child(0).(memo.RelExpr).Relational().FuncDeps)
	return result
}

func distinctOnBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	input := expr.Child(0).(memo.RelExpr)
	provided := input.ProvidedPhysical().Ordering
	inputFDs := &input.Relational().FuncDeps
	// Since the input's provided ordering has to satisfy both <required> and the
	// DistinctOn internal ordering, it may need to be trimmed.
	provided = trimProvided(provided, required, inputFDs)
	return remapProvided(provided, inputFDs, expr.Relational().OutputCols)
}

// StreamingGroupingColOrdering returns an ordering on grouping columns that is
// guaranteed on the input of an aggregation operator. This ordering can be used
// perform a streaming aggregation.
func StreamingGroupingColOrdering(
	g *memo.GroupingPrivate, required *physical.OrderingChoice,
) opt.Ordering {
	inputOrdering := required.Intersection(&g.Ordering)
	ordering := make(opt.Ordering, len(inputOrdering.Columns))
	for i := range inputOrdering.Columns {
		// Get any grouping column from the set. Normally there would be at most one
		// because we have rules that remove redundant grouping columns.
		cols := inputOrdering.Columns[i].Group.Intersection(g.GroupingCols)
		colID, ok := cols.Next(0)
		if !ok {
			// This group refers to a column that is not a grouping column.
			// The rest of the ordering is not useful.
			return ordering[:i]
		}
		ordering[i] = opt.MakeOrderingColumn(colID, inputOrdering.Columns[i].Descending)
	}
	return ordering
}
