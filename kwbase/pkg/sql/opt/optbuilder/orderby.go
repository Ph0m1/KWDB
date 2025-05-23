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

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// analyzeOrderBy analyzes an Ordering physical property from the ORDER BY
// clause and adds the resulting typed expressions to orderByScope.
func (b *Builder) analyzeOrderBy(
	orderBy tree.OrderBy, inScope, projectionsScope *scope, rejectFlags tree.SemaRejectFlags,
) (orderByScope *scope) {
	if orderBy == nil {
		return nil
	}

	orderByScope = inScope.push()
	orderByScope.cols = make([]scopeColumn, 0, len(orderBy))

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprTypeOrderBy.String(), rejectFlags)
	inScope.context = exprTypeOrderBy

	for i := range orderBy {
		b.analyzeOrderByArg(orderBy[i], inScope, projectionsScope, orderByScope)
	}
	return orderByScope
}

// buildOrderBy builds an Ordering physical property from the ORDER BY clause.
// ORDER BY is not a relational expression, but instead a required physical
// property on the output.
//
// Since the ordering property can only refer to output columns, we may need
// to add a projection for the ordering columns. For example, consider the
// following query:
//
//	SELECT a FROM t ORDER BY c
//
// The `c` column must be retained in the projection (and the presentation
// property then omits it).
//
// buildOrderBy builds a set of memo groups for any ORDER BY columns that are
// not already present in the SELECT list (as represented by the initial set
// of columns in projectionsScope). buildOrderBy adds these new ORDER BY
// columns to the projectionsScope and sets the ordering property on the
// projectionsScope. This property later becomes part of the required physical
// properties returned by Build.
func (b *Builder) buildOrderBy(inScope, projectionsScope, orderByScope *scope) {
	if orderByScope == nil {
		return
	}

	orderByScope.ordering = make([]opt.OrderingColumn, 0, len(orderByScope.cols))

	for i := range orderByScope.cols {
		b.buildOrderByArg(inScope, projectionsScope, orderByScope, &orderByScope.cols[i])
	}

	projectionsScope.setOrdering(orderByScope.cols, orderByScope.ordering)
}

// findIndexByName returns an index in the table with the given name. If the
// name is empty the primary index is returned.
func (b *Builder) findIndexByName(table cat.Table, name tree.UnrestrictedName) (cat.Index, error) {
	if name == "" {
		return table.Index(0), nil
	}

	for i, n := 0, table.IndexCount(); i < n; i++ {
		idx := table.Index(i)
		if tree.Name(name) == idx.Name() {
			return idx, nil
		}
	}

	return nil, pgerror.Newf(pgcode.UndefinedObject,
		`index %q not found`, name)
}

// addOrderByOrDistinctOnColumn builds extraCol.expr as a column in extraColsScope; if it is
// already projected in projectionsScope then that projection is re-used.
func (b *Builder) addOrderByOrDistinctOnColumn(
	inScope, projectionsScope, extraColsScope *scope, extraCol *scopeColumn,
) {
	// Use an existing projection if possible (even if it has side-effects; see
	// the SQL99 rules described in analyzeExtraArgument). Otherwise, build a new
	// projection.
	if col := projectionsScope.findExistingCol(
		extraCol.getExpr(),
		true, /* allowSideEffects */
	); col != nil {
		extraCol.id = col.id
	} else {
		// group window function can be only used in groupby
		if scalar := b.buildScalar(extraCol.getExpr(), inScope, extraColsScope, extraCol, nil); scalar != nil {
			if name, ok := memo.CheckGroupWindowExist(scalar); ok {
				panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
			}
		} else if inScope.groupby != nil && len(inScope.groupby.groupStrs) > 0 {
			if groupCol, ok := inScope.groupby.groupStrs[symbolicExprStr(extraCol.getExpr())]; ok {
				if name, ok := memo.CheckGroupWindowExist(groupCol.scalar); ok {
					panic(pgerror.Newf(pgcode.Syntax, "%s() can be only used in groupby", name))
				}
			}
		}
	}
}

// analyzeOrderByIndex appends to the orderByScope a column for each indexed
// column in the specified index, including the implicit primary key columns.
func (b *Builder) analyzeOrderByIndex(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	tab, tn := b.resolveTable(&order.Table, privilege.SELECT)
	index, err := b.findIndexByName(tab, order.Index)
	if err != nil {
		panic(err)
	}

	// We fully qualify the table name in case another table expression was
	// aliased to the same name as an existing table.
	tn.ExplicitCatalog = true
	tn.ExplicitSchema = true

	// Append each key column from the index (including the implicit primary key
	// columns) to the ordering scope.
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		// Columns which are indexable are always orderable.
		col := index.Column(i)
		if err != nil {
			panic(err)
		}

		desc := col.Descending

		// DESC inverts the order of the index.
		if order.Direction == tree.Descending {
			desc = !desc
		}

		colItem := tree.NewColumnItem(&tn, col.ColName())
		expr := inScope.resolveType(colItem, types.Any)
		outCol := b.addColumn(orderByScope, "" /* alias */, expr, false)
		outCol.descending = desc
	}
}

// analyzeOrderByArg analyzes a single ORDER BY argument. Typically this is a
// single column, with the exception of qualified star "table.*". The resulting
// typed expression(s) are added to orderByScope.
func (b *Builder) analyzeOrderByArg(
	order *tree.Order, inScope, projectionsScope, orderByScope *scope,
) {
	if order.OrderType == tree.OrderByIndex {
		b.analyzeOrderByIndex(order, inScope, projectionsScope, orderByScope)
		return
	}

	// Analyze the ORDER BY column(s).
	start := len(orderByScope.cols)
	b.analyzeExtraArgument(order.Expr, inScope, projectionsScope, orderByScope)
	for i := start; i < len(orderByScope.cols); i++ {
		col := &orderByScope.cols[i]
		col.descending = order.Direction == tree.Descending
	}
}

// buildOrderByArg sets up the projection of a single ORDER BY argument.
// The projection column is built in the orderByScope and used to build
// an ordering on the same scope.
func (b *Builder) buildOrderByArg(
	inScope, projectionsScope, orderByScope *scope, orderByCol *scopeColumn,
) {
	// Build the ORDER BY column.
	b.addOrderByOrDistinctOnColumn(inScope, projectionsScope, orderByScope, orderByCol)

	// Add the new column to the ordering.
	orderByScope.ordering = append(orderByScope.ordering,
		opt.MakeOrderingColumn(orderByCol.id, orderByCol.descending),
	)
}

// analyzeExtraArgument analyzes a single ORDER BY or DISTINCT ON argument.
// Typically this is a single column, with the exception of qualified star
// (table.*). The resulting typed expression(s) are added to extraColsScope.
func (b *Builder) analyzeExtraArgument(
	expr tree.Expr, inScope, projectionsScope, extraColsScope *scope,
) {
	// Unwrap parenthesized expressions like "((a))" to "a".
	expr = tree.StripParens(expr)

	// The logical data source for ORDER BY or DISTINCT ON is the list of column
	// expressions for a SELECT, as specified in the input SQL text (or an entire
	// UNION or VALUES clause).  Alas, SQL has some historical baggage from SQL92
	// and there are some special cases:
	//
	// SQL92 rules:
	//
	// 1) if the expression is the aliased (AS) name of an
	//    expression in a SELECT clause, then use that
	//    expression as sort key.
	//    e.g. SELECT a AS b, b AS c ORDER BY b
	//    this sorts on the first column.
	//
	// 2) column ordinals. If a simple integer literal is used,
	//    optionally enclosed within parentheses but *not subject to
	//    any arithmetic*, then this refers to one of the columns of
	//    the data source. Then use the SELECT expression at that
	//    ordinal position as sort key.
	//
	// SQL99 rules:
	//
	// 3) otherwise, if the expression is already in the SELECT list,
	//    then use that expression as sort key.
	//    e.g. SELECT b AS c ORDER BY b
	//    this sorts on the first column.
	//    (this is an optimization)
	//
	// 4) if the sort key is not dependent on the data source (no
	//    IndexedVar) then simply do not sort. (this is an optimization)
	//
	// 5) otherwise, add a new projection with the ORDER BY expression
	//    and use that as sort key.
	//    e.g. SELECT a FROM t ORDER by b
	//    e.g. SELECT a, b FROM t ORDER by a+b

	// First, deal with projection aliases.
	idx := colIdxByProjectionAlias(expr, inScope.context.String(), projectionsScope)

	// If the expression does not refer to an alias, deal with
	// column ordinals.
	if idx == -1 {
		if t, ok := expr.(*tree.UserDefinedVar); ok {
			d, err := t.Eval(b.evalCtx)
			if err != nil {
				panic(err)
			}
			expr = d
		}

		idx = colIndex(len(projectionsScope.cols), expr, inScope.context.String())
	}

	var exprs tree.TypedExprs
	if idx != -1 {
		exprs = []tree.TypedExpr{projectionsScope.cols[idx].getExpr()}
		if inScope.hasGapfill {
			if info, ok := exprs[0].(*aggregateInfo); ok {
				res := resetFuncExpr(info)
				if res != nil {
					exprs = b.expandStarAndResolveType(res, inScope)
				}
			}
		}
	} else {
		exprs = b.expandStarAndResolveType(expr, inScope)

		// ORDER BY (a, b) -> ORDER BY a, b
		exprs = flattenTuples(exprs)
	}

	for _, e := range exprs {
		// Ensure we can order on the given column(s).
		ensureColumnOrderable(e)
		b.addColumn(extraColsScope, "" /* alias */, e, false)
	}
}

func ensureColumnOrderable(e tree.TypedExpr) {
	typ := e.ResolvedType()
	if typ.Family() == types.ArrayFamily {
		panic(unimplementedWithIssueDetailf(35707, "", "can't order by column type %s", typ))
	}
	if typ.Family() == types.JsonFamily {
		panic(unimplementedWithIssueDetailf(35706, "", "can't order by column type jsonb"))
	}
}
