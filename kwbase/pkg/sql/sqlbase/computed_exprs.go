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

package sqlbase

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/transform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// RowIndexedVarContainer is used to evaluate expressions over various rows.
type RowIndexedVarContainer struct {
	CurSourceRow tree.Datums

	// Because the rows we have might not be permuted in the same way as the
	// original table, we need to store a mapping between them.

	Cols    []ColumnDescriptor
	Mapping map[ColumnID]int
}

var _ tree.IndexedVarContainer = &RowIndexedVarContainer{}

// IndexedVarEval implements tree.IndexedVarContainer.
func (r *RowIndexedVarContainer) IndexedVarEval(
	idx int, ctx *tree.EvalContext,
) (tree.Datum, error) {
	rowIdx, ok := r.Mapping[r.Cols[idx].ID]
	if !ok {
		return tree.DNull, nil
	}
	return r.CurSourceRow[rowIdx], nil
}

// IndexedVarResolvedType implements tree.IndexedVarContainer.
func (*RowIndexedVarContainer) IndexedVarResolvedType(idx int) *types.T {
	panic("unsupported")
}

// IndexedVarNodeFormatter implements tree.IndexedVarContainer.
func (*RowIndexedVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// descContainer is a helper type that implements tree.IndexedVarContainer; it
// is used to type check computed columns and does not support evaluation.
type descContainer struct {
	cols []ColumnDescriptor
}

func (j *descContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unsupported")
}

func (j *descContainer) IndexedVarResolvedType(idx int) *types.T {
	return &j.cols[idx].Type
}

func (*descContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// CannotWriteToComputedColError constructs a write error for a computed column.
func CannotWriteToComputedColError(colName string) error {
	return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
		"cannot write directly to computed column %q", tree.ErrNameString(colName))
}

// MakeComputedExprs returns a slice of the computed expressions for the
// slice of input column descriptors, or nil if none of the input column
// descriptors have computed expressions.
// The length of the result slice matches the length of the input column
// descriptors. For every column that has no computed expression, a NULL
// expression is reported.
// addingCols indicates if the input column descriptors are being added
// and allows type checking of the compute expressions to reference
// input columns earlier in the slice.
func MakeComputedExprs(
	cols []ColumnDescriptor,
	tableDesc *ImmutableTableDescriptor,
	tn *tree.TableName,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
	addingCols bool,
) ([]tree.TypedExpr, error) {
	// Check to see if any of the columns have computed expressions. If there
	// are none, we don't bother with constructing the map as the expressions
	// are all NULL.
	haveComputed := false
	for i := range cols {
		if cols[i].IsComputed() {
			haveComputed = true
			break
		}
	}
	if !haveComputed {
		return nil, nil
	}

	// Build the computed expressions map from the parsed statement.
	computedExprs := make([]tree.TypedExpr, 0, len(cols))
	exprStrings := make([]string, 0, len(cols))
	for i := range cols {
		col := &cols[i]
		if col.IsComputed() {
			exprStrings = append(exprStrings, *col.ComputeExpr)
		}
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	// We need an ivarHelper and sourceInfo, unlike DEFAULT, since computed
	// columns can reference other columns and thus need to be able to resolve
	// column names (at this stage they only need to resolve the types so that
	// the expressions can be typechecked - we have no need to evaluate them).
	iv := &descContainer{tableDesc.Columns}
	ivarHelper := tree.MakeIndexedVarHelper(iv, len(tableDesc.Columns))

	source := NewSourceInfoForSingleTable(*tn, ResultColumnsFromColDescs(tableDesc.GetID(), tableDesc.Columns))
	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = iv

	addColumnInfo := func(col *ColumnDescriptor) {
		ivarHelper.AppendSlot()
		iv.cols = append(iv.cols, *col)
		newCols := ResultColumnsFromColDescs(tableDesc.GetID(), []ColumnDescriptor{*col})
		source.SourceColumns = append(source.SourceColumns, newCols...)
	}

	compExprIdx := 0
	for i := range cols {
		col := &cols[i]
		if !col.IsComputed() {
			computedExprs = append(computedExprs, tree.DNull)
			if addingCols {
				addColumnInfo(col)
			}
			continue
		}
		expr, _, err := ResolveNames(
			exprs[compExprIdx], source, ivarHelper, evalCtx.SessionData.SearchPath)
		if err != nil {
			return nil, err
		}

		typedExpr, err := tree.TypeCheck(expr, &semaCtx, &col.Type)
		if err != nil {
			return nil, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, err
		}
		computedExprs = append(computedExprs, typedExpr)
		compExprIdx++
		if addingCols {
			addColumnInfo(col)
		}
	}
	return computedExprs, nil
}
