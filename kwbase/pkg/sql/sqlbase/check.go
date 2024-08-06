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

package sqlbase

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CheckHelper validates check constraints on rows; it is used only by the
// legacy foreign-key path.
//
// CheckHelper analyzes and evaluates each check constraint as a standalone
// expression. This is used by the heuristic planner, and is the
// backwards-compatible code path.
//
// Callers should call NewEvalCheckHelper to initialize a new instance of
// CheckHelper. For each row, they call LoadEvalRow one or more times to set row
// values for evaluation, and then call CheckEval to trigger evaluation.
type CheckHelper struct {
	Exprs        []tree.TypedExpr
	cols         []ColumnDescriptor
	sourceInfo   *DataSourceInfo
	ivarHelper   *tree.IndexedVarHelper
	curSourceRow tree.Datums
}

// AnalyzeExprFunction is the function type used by the CheckHelper during
// initialization to analyze an expression. See sql/analyze_expr.go for details
// about the function.
type AnalyzeExprFunction func(
	ctx context.Context,
	raw tree.Expr,
	source *DataSourceInfo,
	iVarHelper tree.IndexedVarHelper,
	expectedType *types.T,
	requireType bool,
	typingContext string,
) (tree.TypedExpr, error)

// NewEvalCheckHelper constructs a new instance of the CheckHelper.
func NewEvalCheckHelper(
	ctx context.Context, analyzeExpr AnalyzeExprFunction, tableDesc *ImmutableTableDescriptor,
) (*CheckHelper, error) {
	if len(tableDesc.ActiveChecks()) == 0 {
		return nil, nil
	}

	c := &CheckHelper{}
	c.cols = tableDesc.AllNonDropColumns()
	c.sourceInfo = NewSourceInfoForSingleTable(
		tree.MakeUnqualifiedTableName(tree.Name(tableDesc.Name)),
		ResultColumnsFromColDescs(tableDesc.GetID(), c.cols),
	)

	c.Exprs = make([]tree.TypedExpr, len(tableDesc.ActiveChecks()))
	exprStrings := make([]string, len(tableDesc.ActiveChecks()))
	for i, check := range tableDesc.ActiveChecks() {
		exprStrings[i] = check.Expr
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	ivarHelper := tree.MakeIndexedVarHelper(c, len(c.cols))
	for i, raw := range exprs {
		typedExpr, err := analyzeExpr(
			ctx,
			raw,
			c.sourceInfo,
			ivarHelper,
			types.Bool,
			false, /* requireType */
			"",    /* typingContext */
		)
		if err != nil {
			return nil, err
		}
		c.Exprs[i] = typedExpr
	}
	c.ivarHelper = &ivarHelper
	c.curSourceRow = make(tree.Datums, len(c.cols))
	return c, nil
}

// LoadEvalRow sets values in the IndexedVars used by the CHECK exprs.
// Any value not passed is set to NULL, unless `merge` is true, in which
// case it is left unchanged (allowing updating a subset of a row's values).
func (c *CheckHelper) LoadEvalRow(colIdx map[ColumnID]int, row tree.Datums, merge bool) error {
	if len(c.Exprs) == 0 {
		return nil
	}
	// Populate IndexedVars.
	for _, ivar := range c.ivarHelper.GetIndexedVars() {
		if !ivar.Used {
			continue
		}
		ri, has := colIdx[c.cols[ivar.Idx].ID]
		if has {
			if row[ri] != tree.DNull {
				expected, provided := ivar.ResolvedType(), row[ri].ResolvedType()
				if !expected.Equivalent(provided) {
					return errors.Errorf("%s value does not match CHECK expr type %s", provided, expected)
				}
			}
			c.curSourceRow[ivar.Idx] = row[ri]
		} else if !merge {
			c.curSourceRow[ivar.Idx] = tree.DNull
		}
	}
	return nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return c.curSourceRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarResolvedType(idx int) *types.T {
	return c.sourceInfo.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the parser.IndexedVarContainer interface.
func (c *CheckHelper) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return c.sourceInfo.NodeFormatter(idx)
}

// CheckEval evaluates each check constraint expression using values from the
// current row that was previously set via a call to LoadEvalRow.
func (c *CheckHelper) CheckEval(ctx *tree.EvalContext) error {
	ctx.PushIVarContainer(c)
	defer func() { ctx.PopIVarContainer() }()
	for _, expr := range c.Exprs {
		if d, err := expr.Eval(ctx); err != nil {
			return err
		} else if res, err := tree.GetBool(d); err != nil {
			return err
		} else if !res && d != tree.DNull {
			// Failed to satisfy CHECK constraint.
			return pgerror.Newf(pgcode.CheckViolation,
				"failed to satisfy CHECK constraint (%s)", expr)
		}
	}
	return nil
}
