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

package norm

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RemoveGroupingCols returns a new grouping private struct with the given
// columns removed from the grouping column set.
func (c *CustomFuncs) RemoveGroupingCols(
	private *memo.GroupingPrivate, cols opt.ColSet,
) *memo.GroupingPrivate {
	p := *private
	p.GroupingCols = private.GroupingCols.Difference(cols)
	return &p
}

// AppendAggCols constructs a new Aggregations operator containing the aggregate
// functions from an existing Aggregations operator plus an additional set of
// aggregate functions, one for each column in the given set. The new functions
// are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols(
	aggs memo.AggregationsExpr, aggOp opt.Operator, cols opt.ColSet,
) memo.AggregationsExpr {
	outAggs := make(memo.AggregationsExpr, len(aggs)+cols.Len())
	copy(outAggs, aggs)
	c.makeAggCols(aggOp, cols, outAggs[len(aggs):])
	return outAggs
}

// AppendAggCols2 constructs a new Aggregations operator containing the
// aggregate functions from an existing Aggregations operator plus an
// additional set of aggregate functions, one for each column in the given set.
// The new functions are of the given aggregate operator type.
func (c *CustomFuncs) AppendAggCols2(
	aggs memo.AggregationsExpr,
	aggOp opt.Operator,
	cols opt.ColSet,
	aggOp2 opt.Operator,
	cols2 opt.ColSet,
) memo.AggregationsExpr {
	colsLen := cols.Len()
	outAggs := make(memo.AggregationsExpr, len(aggs)+colsLen+cols2.Len())
	copy(outAggs, aggs)

	offset := len(aggs)
	c.makeAggCols(aggOp, cols, outAggs[offset:])
	offset += colsLen
	c.makeAggCols(aggOp2, cols2, outAggs[offset:])

	return outAggs
}

// makeAggCols is a helper method that constructs a new aggregate function of
// the given operator type for each column in the given set. The resulting
// aggregates are written into outElems and outColList. As an example, for
// columns (1,2) and operator ConstAggOp, makeAggCols will set the following:
//
//	outElems[0] = (ConstAggOp (Variable 1))
//	outElems[1] = (ConstAggOp (Variable 2))
//
//	outColList[0] = 1
//	outColList[1] = 2
func (c *CustomFuncs) makeAggCols(
	aggOp opt.Operator, cols opt.ColSet, outAggs memo.AggregationsExpr,
) {
	// Append aggregate functions wrapping a Variable reference to each column.
	i := 0
	for id, ok := cols.Next(0); ok; id, ok = cols.Next(id + 1) {
		varExpr := c.f.ConstructVariable(id)

		var outAgg opt.ScalarExpr
		switch aggOp {
		case opt.ConstAggOp:
			outAgg = c.f.ConstructConstAgg(varExpr)

		case opt.AnyNotNullAggOp:
			outAgg = c.f.ConstructAnyNotNullAgg(varExpr)

		case opt.FirstAggOp:
			outAgg = c.f.ConstructFirstAgg(varExpr)

		default:
			panic(errors.AssertionFailedf("unrecognized aggregate operator type: %v", log.Safe(aggOp)))
		}

		outAggs[i] = c.f.ConstructAggregationsItem(outAgg, id)
		i++
	}
}

// CanRemoveAggDistinctForKeys returns true if the given aggregate function
// where its input column, together with the grouping columns, form a key. In
// this case, the wrapper AggDistinct can be removed.
func (c *CustomFuncs) CanRemoveAggDistinctForKeys(
	input memo.RelExpr, private *memo.GroupingPrivate, agg opt.ScalarExpr,
) bool {
	if agg.ChildCount() == 0 {
		return false
	}
	inputFDs := &input.Relational().FuncDeps
	variable := agg.Child(0).(*memo.VariableExpr)
	cols := c.AddColToSet(private.GroupingCols, variable.Col)
	return inputFDs.ColsAreStrictKey(cols)
}

// ReplaceAggregationsItem returns a new list that is a copy of the given list,
// except that the given search item has been replaced by the given replace
// item. If the list contains the search item multiple times, then only the
// first instance is replaced. If the list does not contain the item, then the
// method panics.
func (c *CustomFuncs) ReplaceAggregationsItem(
	aggs memo.AggregationsExpr, search *memo.AggregationsItem, replace opt.ScalarExpr,
) memo.AggregationsExpr {
	newAggs := make([]memo.AggregationsItem, len(aggs))
	for i := range aggs {
		if search == &aggs[i] {
			copy(newAggs, aggs[:i])
			newAggs[i] = c.f.ConstructAggregationsItem(replace, search.Col)
			copy(newAggs[i+1:], aggs[i+1:])
			return newAggs
		}
	}
	panic(errors.AssertionFailedf("item to replace is not in the list: %v", search))
}

// HasNoGroupingCols returns true if the GroupingCols in the private are empty.
func (c *CustomFuncs) HasNoGroupingCols(private *memo.GroupingPrivate) bool {
	return private.GroupingCols.Empty()
}

// GroupingInputOrdering returns the Ordering in the private.
func (c *CustomFuncs) GroupingInputOrdering(private *memo.GroupingPrivate) physical.OrderingChoice {
	return private.Ordering
}

// ConstructProjectionFromDistinctOn converts a DistinctOn to a projection; this
// is correct when input groupings have at most one row (i.e. the input is
// already distinct). Note that DistinctOn can only have aggregations of type
// FirstAgg or ConstAgg.
func (c *CustomFuncs) ConstructProjectionFromDistinctOn(
	input memo.RelExpr, groupingCols opt.ColSet, aggs memo.AggregationsExpr,
) memo.RelExpr {
	// Always pass through grouping columns.
	passthrough := groupingCols.Copy()

	var projections memo.ProjectionsExpr
	for i := range aggs {
		varExpr := memo.ExtractAggFirstVar(aggs[i].Agg)
		inputCol := varExpr.Col
		outputCol := aggs[i].Col
		if inputCol == outputCol {
			passthrough.Add(inputCol)
		} else {
			projections = append(projections, c.f.ConstructProjectionsItem(varExpr, aggs[i].Col))
		}
	}
	return c.f.ConstructProject(input, projections, passthrough)
}

// DuplicateUpsertErrText returns the error text used when duplicate input rows
// to the Upsert operator are detected.
func (c *CustomFuncs) DuplicateUpsertErrText() string {
	return sqlbase.DuplicateUpsertErrText
}

// AreValuesDistinct returns true if a constant Values operator input contains
// only rows that are already distinct with respect to the given grouping
// columns. The Values operator can be wrapped by Select, Project, and/or
// LeftJoin operators.
//
// If nullsAreDistinct is true, then NULL values are treated as not equal to one
// another, and therefore rows containing a NULL value in any grouping column
// are always distinct.
func (c *CustomFuncs) AreValuesDistinct(
	input memo.RelExpr, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	switch t := input.(type) {
	case *memo.ValuesExpr:
		return c.areRowsDistinct(t.Rows, t.Cols, groupingCols, nullsAreDistinct)

	case *memo.SelectExpr:
		return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)

	case *memo.ProjectExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}

	case *memo.LeftJoinExpr:
		// Pass through call to left input if grouping on its columns. Also,
		// ensure that the left join does not cause duplication of left rows.
		leftCols := t.Left.Relational().OutputCols
		rightCols := t.Right.Relational().OutputCols
		if !groupingCols.SubsetOf(leftCols) {
			break
		}

		// If any set of key columns (lax or strict) from the right input are
		// equality joined to columns in the left input, then the left join will
		// never cause duplication of left rows.
		var eqCols opt.ColSet
		for i := range t.On {
			condition := t.On[i].Condition
			ok, _, rightColID := memo.ExtractJoinEquality(leftCols, rightCols, condition)
			if ok {
				eqCols.Add(rightColID)
			}
		}
		if !t.Right.Relational().FuncDeps.ColsAreLaxKey(eqCols) {
			// Not joining on a right input key.
			break
		}

		return c.AreValuesDistinct(t.Left, groupingCols, nullsAreDistinct)

	case *memo.UpsertDistinctOnExpr:
		// Pass through call to input if grouping on passthrough columns.
		if groupingCols.SubsetOf(t.Input.Relational().OutputCols) {
			return c.AreValuesDistinct(t.Input, groupingCols, nullsAreDistinct)
		}
	}
	return false
}

// areRowsDistinct returns true if the given rows are unique on the given
// grouping columns. If nullsAreDistinct is true, then NULL values are treated
// as unique, and therefore a row containing a NULL value in any grouping column
// is always distinct from every other row.
func (c *CustomFuncs) areRowsDistinct(
	rows memo.ScalarListExpr, cols opt.ColList, groupingCols opt.ColSet, nullsAreDistinct bool,
) bool {
	var seen map[string]bool
	var encoded []byte
	for _, scalar := range rows {
		row := scalar.(*memo.TupleExpr)

		// Reset scratch bytes.
		encoded = encoded[:0]

		forceDistinct := false
		for i, colID := range cols {
			if !groupingCols.Contains(colID) {
				// This is not a grouping column, so ignore.
				continue
			}

			// Try to extract constant value from column. Call IsConstValueOp first,
			// since this code doesn't handle the tuples and arrays that
			// ExtractConstDatum can return.
			col := row.Elems[i]
			if !opt.IsConstValueOp(col) {
				// At least one grouping column in at least one row is not constant,
				// so can't determine whether the rows are distinct.
				return false
			}
			datum := memo.ExtractConstDatum(col)

			// If this is an UpsertDistinctOn operator, then treat NULL values as
			// always distinct.
			if nullsAreDistinct && datum == tree.DNull {
				forceDistinct = true
				break
			}

			// Encode the datum using the key encoding format. The encodings for
			// multiple column datums are simply appended to one another.
			var err error
			encoded, err = sqlbase.EncodeTableKey(encoded, datum, encoding.Ascending)
			if err != nil {
				// Assume rows are not distinct if an encoding error occurs.
				return false
			}
		}

		if seen == nil {
			seen = make(map[string]bool, len(rows))
		}

		// Determine whether key has already been seen.
		key := string(encoded)
		if _, ok := seen[key]; ok && !forceDistinct {
			// Found duplicate.
			return false
		}

		// Add the key to the seen map.
		seen[key] = true
	}

	return true
}

// TryPushAgg determines whether the memo expression meets the optimization criteria.
// If it does, push down the group by expr and construct a new group by expr to return.
// input params:
//
//	input: child of GroupByExpr or ScalarGroupByExpr(only InnerJoinExpr can be optimized)
//	aggs : Aggregations of GroupByExpr or ScalarGroupByExpr, record info of agg functions
//	private : GroupingPrivate of GroupByExpr or ScalarGroupByExpr
//
// output params: the new GroupByExpr that had been optimized.
func (c *CustomFuncs) TryPushAgg(
	input memo.RelExpr, aggs memo.AggregationsExpr, private *memo.GroupingPrivate,
) memo.RelExpr {
	var optHelper = &memo.InsideOutOptHelper{}
	canOpt, join := c.precheckInsideOutOptApplicable(input, aggs, private, optHelper)
	if !canOpt {
		return nil
	}

	c.f.CheckWhiteListAndSetEngine(&input)

	finishOpt, newInnerJoin, newAggItems, projectItems, passCols := PushAggIntoJoinTSEngineNode(c.f, join, aggs, private, private.GroupingCols, optHelper)
	if finishOpt {
		c.mem.SetFlag(opt.FinishOptInsideOut)
		var newGroup memo.RelExpr
		for _, agg := range newAggItems {
			if private.GroupingCols.Empty() && agg.Agg.Op() == opt.SumIntOp {
				private.OptFlags |= opt.ScalarGroupByWithSumInt
			}
		}
		if private.GroupingCols.Empty() {
			newGroup = c.f.ConstructScalarGroupBy(newInnerJoin, newAggItems, private)
		} else {
			newGroup = c.f.ConstructGroupBy(newInnerJoin, newAggItems, private)
		}

		if len(projectItems) > 0 {
			newGroup = c.f.ConstructProject(newGroup, projectItems, passCols)
		}
		return newGroup
	}

	return nil
}

// IsAvailable returns true when input is not nil.
func (c *CustomFuncs) IsAvailable(input memo.RelExpr) bool {
	return input != nil
}

// ConstructNewGroupBy returns input.
func (c *CustomFuncs) ConstructNewGroupBy(input memo.RelExpr) memo.RelExpr {
	return input
}

// precheckInsideOutOptApplicable pre-checks whether group by can be optimized.
// Optimization needs to meet the following four points
// 1. The cluster parameter "sql.inside_out.enabled" needs to be set to true
// 2. GroupByExpr has not been optimized
// 3. The child of GroupByExp must be InnerJoinExpr or ProjectExpr. If it is a ProjectExpr,
// it must satisfy that the child of the ProjectExpr is InnerJoinExpr and the ProjectItems cannot cross modules
// 4. the col of grouping must be single col and the ts col must be tag col
// input params:
// input: child of GroupByExpr or ScalarGroupByExpr
// aggs: Aggregations of GroupByExpr or ScalarGroupByExpr, record infos of agg functions
// private: GroupingPrivate of GroupByExpr or ScalarGroupByExpr
// optHelper: record aggItems, grouping, projectionItems to help opt inside-out
//
// output params: is true when GroupByExpr can be optimized
func (c *CustomFuncs) precheckInsideOutOptApplicable(
	input memo.RelExpr,
	aggs memo.AggregationsExpr,
	private *memo.GroupingPrivate,
	optHelper *memo.InsideOutOptHelper,
) (bool, *memo.InnerJoinExpr) {
	// could not opt inside-out when sql.inside_out.enabled is false
	if !opt.CheckOptMode(opt.TSQueryOptMode.Get(&c.f.evalCtx.Settings.SV), opt.JoinPushAgg) {
		return false, nil
	}

	optTimeBucket := opt.CheckOptMode(opt.TSQueryOptMode.Get(&c.f.evalCtx.Settings.SV), opt.JoinPushTimeBucket)

	// do not need to opt when there has no ts table, or it has already opted.
	if !c.mem.CheckFlag(opt.IncludeTSTable) || c.mem.CheckFlag(opt.FinishOptInsideOut) {
		return false, nil
	}

	// can only opt when the agg is (Sum, Count, CountRows, Avg, Min, Max)
	if !c.checkAggOptApplicable(aggs, optHelper) {
		return false, nil
	}

	// could opt inside-out when the child of GroupByExpr isn't InnerJoinExpr
	join, isJoin := input.(*memo.InnerJoinExpr)
	if !isJoin {
		if project, isPro := input.(*memo.ProjectExpr); isPro {
			join1, isJoin1 := project.Input.(*memo.InnerJoinExpr)
			if !isJoin1 {
				return false, nil
			}
			// check projectionItems(could not cross mode) and the project must can exec in ts engine
			if !c.checkProjectionApplicable(project, optHelper) {
				return false, nil
			}
			optHelper.Passthrough = project.Passthrough
			join = join1
		} else {
			return false, nil
		}
	}

	// check grouping is single col and tag col
	if !private.GroupingCols.Empty() {
		groupCols := private.GroupingCols
		isApplicable := true
		groupCols.ForEach(func(colID opt.ColumnID) {
			colMeta := c.mem.Metadata().ColumnMeta(colID)
			isRelSingleCol := colMeta.Table != 0 && colMeta.TSType == opt.ColNormal
			isTag := colMeta.IsTag()
			isApplicable = isTag || isRelSingleCol
			if optTimeBucket {
				isTimeBucket := false
				if tb, ok := c.mem.CheckHelper.PushHelper.Find(colID); ok {
					isTimeBucket = tb.IsTimeBucket
				}
				isApplicable = isApplicable || isTimeBucket
			}
		})
		if !isApplicable {
			return false, nil
		}
	}

	return true, join
}

// checkProjectionApplicable checks if the projection is applicable for optimization.
// It must satisfy that the ProjectItems cannot cross modules.
func (c *CustomFuncs) checkProjectionApplicable(
	project *memo.ProjectExpr, optHelper *memo.InsideOutOptHelper,
) bool {
	for _, proj := range project.Projections {
		// check if elements of ProjectionExpr can be pushed down
		push, _ := memo.CheckExprCanExecInTSEngine(proj.Element.(opt.Expr), memo.ExprPosProjList,
			c.f.TSWhiteListMap.CheckWhiteListParam, false)
		if !push {
			return false
		}
		// check if elements of ProjectionExpr do not cross modules
		m := modeHelper{modes: 0}
		c.getEngineMode(proj.Element, &m)
		if m.isHybridMode() {
			return false
		}
		optHelper.Projections = append(optHelper.Projections, proj)
		if m.checkMode(tsMode) {
			optHelper.ProEngine = append(optHelper.ProEngine, tree.EngineTypeTimeseries)
		} else {
			optHelper.ProEngine = append(optHelper.ProEngine, tree.EngineTypeRelational)
		}
		// check whether count/sum/avg parameter is in relational mode
		for _, aggArg := range optHelper.AggArgs {
			if aggArg.ArgColID == -1 {
				return false
			}
			if proj.Col == aggArg.ArgColID {
				if aggArg.AggOp == opt.SumOp || aggArg.AggOp == opt.CountOp || aggArg.AggOp == opt.AvgOp {
					if m.checkMode(relMode) {
						return false
					}
				}
			}
		}
		// check if elements of ProjectionExpr is time_bucket
		if proj.Element.Op() == opt.FunctionOp {
			f := proj.Element.(*memo.FunctionExpr)
			if f.Name == tree.FuncTimeBucket && m.checkMode(tsMode) {
				c.mem.AddColumn(proj.Col, "", memo.GetExprType(proj.Element),
					memo.ExprPosProjList, 0, true)
			}
		}
	}
	return true
}

const (
	relMode = 1 << 0
	tsMode  = 1 << 1
	anyMode = 1 << 2
)

// modeHelper is used to get the mode in which the projection was executed.
type modeHelper struct {
	modes int
}

// setMode sets mode is true
func (m *modeHelper) setMode(mode int) {
	m.modes |= mode
}

// checkMode checks if the mode is set.
func (m *modeHelper) checkMode(mode int) bool {
	return m.modes&mode > 0
}

// isHybridMode checks to see if there is both timeseries mode and relational mode
func (m *modeHelper) isHybridMode() bool {
	return m.checkMode(relMode) && m.checkMode(tsMode)
}

// getEngineMode gets the execution mode of the input expression and places it in the modeHelper.
func (c *CustomFuncs) getEngineMode(src opt.ScalarExpr, m *modeHelper) {
	switch t := src.(type) {
	case *memo.VariableExpr:
		colType := c.mem.Metadata().ColumnMeta(t.Col).TSType
		if colType == opt.ColNormal {
			m.setMode(relMode)
		} else {
			m.setMode(tsMode)
		}

	case *memo.FunctionExpr:
		for _, param := range t.Args {
			c.getEngineMode(param, m)
		}

	case *memo.ScalarListExpr:
		for i := range *t {
			c.getEngineMode((*t)[i], m)
		}

	case *memo.TrueExpr, *memo.FalseExpr, *memo.ConstExpr, *memo.IsExpr, *memo.IsNotExpr, *memo.NullExpr:
		m.setMode(anyMode)

	case *memo.TupleExpr:
		for i := range t.Elems {
			c.getEngineMode(t.Elems[i], m)
		}

	case *memo.ArrayExpr:
		for i := range t.Elems {
			c.getEngineMode(t.Elems[i], m)
		}

	case *memo.CaseExpr:
		for i := range t.Whens {
			c.getEngineMode(t.Whens[i], m)
		}
		c.getEngineMode(t.Input, m)
		c.getEngineMode(t.OrElse, m)

	case *memo.CastExpr, *memo.NotExpr, *memo.RangeExpr:
		c.getEngineMode(t.Child(0).(opt.ScalarExpr), m)

	case *memo.AndExpr, *memo.OrExpr, *memo.GeExpr, *memo.GtExpr, *memo.NeExpr, *memo.EqExpr, *memo.LeExpr, *memo.LtExpr, *memo.LikeExpr,
		*memo.NotLikeExpr, *memo.ILikeExpr, *memo.NotILikeExpr, *memo.SimilarToExpr, *memo.NotSimilarToExpr, *memo.RegMatchExpr,
		*memo.NotRegMatchExpr, *memo.RegIMatchExpr, *memo.NotRegIMatchExpr, *memo.ContainsExpr, *memo.JsonExistsExpr,
		*memo.JsonAllExistsExpr, *memo.JsonSomeExistsExpr, *memo.AnyScalarExpr, *memo.BitandExpr, *memo.BitorExpr, *memo.BitxorExpr,
		*memo.PlusExpr, *memo.MinusExpr, *memo.MultExpr, *memo.DivExpr, *memo.FloorDivExpr, *memo.ModExpr, *memo.PowExpr, *memo.ConcatExpr,
		*memo.LShiftExpr, *memo.RShiftExpr, *memo.WhenExpr, *memo.InExpr, *memo.NotInExpr:
		c.getEngineMode(t.Child(0).(opt.ScalarExpr), m)
		c.getEngineMode(t.Child(1).(opt.ScalarExpr), m)

	default:
		m.setMode(relMode)
		m.setMode(tsMode)
	}
}
