// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
)

// joinPredicate implements the predicate logic for joins.
type joinPredicate struct {
	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	_ util.NoCopy

	joinType sqlbase.JoinType

	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// left/rightEqualityIndices give the position of equality columns
	// on the left and right input row arrays, respectively.
	// Only columns with the same left and right value types can be equality
	// columns.
	leftEqualityIndices  []int
	rightEqualityIndices []int

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames tree.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames tree.NameList

	// For ON predicates or joins with an added filter expression,
	// we need an IndexedVarHelper, the DataSourceInfo, a row buffer
	// and the expression itself.
	iVarHelper tree.IndexedVarHelper
	curRow     tree.Datums
	// The ON condition that needs to be evaluated (in addition to the
	// equality columns).
	onCond tree.TypedExpr

	leftCols  sqlbase.ResultColumns
	rightCols sqlbase.ResultColumns
	cols      sqlbase.ResultColumns

	// If set, the left equality columns form a key in the left input. Used as a
	// hint for optimizing execution.
	leftEqKey bool
	// If set, the right equality columns form a key in the right input. Used as a
	// hint for optimizing execution.
	rightEqKey bool
}

// makePredicate constructs a joinPredicate object for joins. The join condition
// includes equality between usingColumns.
func makePredicate(
	joinType sqlbase.JoinType, left, right sqlbase.ResultColumns,
) (*joinPredicate, error) {
	// For anti and semi joins, the right columns are omitted from the output (but
	// they must be available internally for the ON condition evaluation).
	omitRightColumns := joinType == sqlbase.JoinType_LEFT_SEMI || joinType == sqlbase.JoinType_LEFT_ANTI

	// Prepare the metadata for the result columns.
	// The structure of the join data source results is like this:
	// - all the left columns,
	// - then all the right columns (except for anti/semi join).
	columns := make(sqlbase.ResultColumns, 0, len(left)+len(right))
	columns = append(columns, left...)
	if !omitRightColumns {
		columns = append(columns, right...)
	}

	pred := &joinPredicate{
		joinType:     joinType,
		numLeftCols:  len(left),
		numRightCols: len(right),
		leftCols:     left,
		rightCols:    right,
		cols:         columns,
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no on condition, so that getNeededColumns() does not get
	// confused.
	pred.curRow = make(tree.Datums, len(left)+len(right))
	pred.iVarHelper = tree.MakeIndexedVarHelper(pred, len(pred.curRow))

	return pred, nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) *types.T {
	if idx < p.numLeftCols {
		return p.leftCols[idx].Typ
	}
	return p.rightCols[idx-p.numLeftCols].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	if idx < p.numLeftCols {
		return p.leftCols.NodeFormatter(idx)
	}
	return p.rightCols.NodeFormatter(idx - p.numLeftCols)
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(ctx *tree.EvalContext, leftRow, rightRow tree.Datums) (bool, error) {
	if p.onCond != nil {
		copy(p.curRow[:len(leftRow)], leftRow)
		copy(p.curRow[len(leftRow):], rightRow)
		ctx.PushIVarContainer(p.iVarHelper.Container())
		pred, err := sqlbase.RunFilter(p.onCond, ctx)
		ctx.PopIVarContainer()
		return pred, err
	}
	return true, nil
}

// prepareRow prepares the output row by combining values from the
// input data sources.
func (p *joinPredicate) prepareRow(result, leftRow, rightRow tree.Datums) {
	copy(result[:len(leftRow)], leftRow)
	copy(result[len(leftRow):], rightRow)
}
