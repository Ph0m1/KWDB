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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// buildAlterTableSplit builds an ALTER TABLE/INDEX .. SPLIT AT .. statement.
func (b *Builder) buildAlterTableSplit(split *tree.Split, inScope *scope) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, tn, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &split.TableOrIndex)
	if err != nil {
		panic(err)
	}
	table := index.Table()
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(err)
	}
	if table.GetTableType() != tree.RelationalTable {
		panic(sqlbase.TSUnsupportedError("split"))
	}

	b.DisableMemoReuse = true

	// Calculate the desired types for the input expression. It is OK if it
	// returns fewer columns (the relevant prefix is used).
	colNames, colTypes := getIndexColumnNamesAndTypes(index)

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(split.Rows, colTypes, emptyScope)
	checkInputColumns("SPLIT AT", inputScope, colNames, colTypes, 1)

	// Build the expiration scalar.
	var expiration opt.ScalarExpr
	if split.ExpireExpr != nil {
		emptyScope.context = exprTypeAlterTableSplitAt
		// We need to save and restore the previous value of the field in
		// semaCtx in case we are recursively called within a subquery
		// context.
		defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
		b.semaCtx.Properties.Require(emptyScope.context.String(), tree.RejectSpecial)

		texpr := emptyScope.resolveType(split.ExpireExpr, types.String)
		expiration = b.buildScalar(texpr, emptyScope, nil /* outScope */, nil /* outCol */, nil /* colRefs */)
	} else {
		expiration = b.factory.ConstructNull(types.String)
	}

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableSplitColumns)
	outScope.expr = b.factory.ConstructAlterTableSplit(
		inputScope.expr.(memo.RelExpr),
		expiration,
		&memo.AlterTableSplitPrivate{
			Table:   b.factory.Metadata().AddTable(table, &tn),
			Index:   index.Ordinal(),
			Columns: colsToColList(outScope.cols),
			Props:   inputScope.makePhysicalProps(),
		},
	)
	return outScope
}

// buildAlterTableUnsplit builds an ALTER TABLE/INDEX .. UNSPLIT AT/ALL .. statement.
func (b *Builder) buildAlterTableUnsplit(unsplit *tree.Unsplit, inScope *scope) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, tn, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &unsplit.TableOrIndex)
	if err != nil {
		panic(err)
	}
	table := index.Table()
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(err)
	}

	if table.GetTableType() != tree.RelationalTable {
		panic(sqlbase.TSUnsupportedError("unsplit"))
	}
	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableUnsplitColumns)
	private := &memo.AlterTableSplitPrivate{
		Table:   b.factory.Metadata().AddTable(table, &tn),
		Index:   index.Ordinal(),
		Columns: colsToColList(outScope.cols),
	}

	if unsplit.All {
		private.Props = physical.MinRequired
		outScope.expr = b.factory.ConstructAlterTableUnsplitAll(private)
		return outScope
	}

	// Calculate the desired types for the input expression. It is OK if it
	// returns fewer columns (the relevant prefix is used).
	colNames, colTypes := getIndexColumnNamesAndTypes(index)

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	inputScope := b.buildStmt(unsplit.Rows, colTypes, b.allocScope())
	checkInputColumns("UNSPLIT AT", inputScope, colNames, colTypes, 1)
	private.Props = inputScope.makePhysicalProps()

	outScope.expr = b.factory.ConstructAlterTableUnsplit(
		inputScope.expr.(memo.RelExpr),
		private,
	)
	return outScope
}

// buildAlterTableRelocate builds an ALTER TABLE/INDEX .. UNSPLIT AT/ALL .. statement.
func (b *Builder) buildAlterTableRelocate(
	relocate *tree.Relocate, inScope *scope,
) (outScope *scope) {
	flags := cat.Flags{
		AvoidDescriptorCaches: true,
		NoTableStats:          true,
	}
	index, tn, err := cat.ResolveTableIndex(b.ctx, b.catalog, flags, &relocate.TableOrIndex)
	if err != nil {
		panic(err)
	}
	table := index.Table()
	if table.GetTableType() != tree.RelationalTable {
		panic(sqlbase.TSUnsupportedError("relocate"))
	}
	if err := b.catalog.CheckPrivilege(b.ctx, table, privilege.INSERT); err != nil {
		panic(err)
	}

	b.DisableMemoReuse = true

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, sqlbase.AlterTableRelocateColumns)

	// Calculate the desired types for the input expression. It is OK if it
	// returns fewer columns (the relevant prefix is used).
	colNames, colTypes := getIndexColumnNamesAndTypes(index)

	// The first column is the target leaseholder or the relocation array,
	// depending on variant.
	cmdName := "EXPERIMENTAL_RELOCATE"
	if relocate.RelocateLease {
		cmdName += " LEASE"
		colNames = append([]string{"target leaseholder"}, colNames...)
		colTypes = append([]*types.T{types.Int}, colTypes...)
	} else {
		colNames = append([]string{"relocation array"}, colNames...)
		colTypes = append([]*types.T{types.IntArray}, colTypes...)
	}

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	inputScope := b.buildStmt(relocate.Rows, colTypes, b.allocScope())
	checkInputColumns(cmdName, inputScope, colNames, colTypes, 2)

	outScope.expr = b.factory.ConstructAlterTableRelocate(
		inputScope.expr.(memo.RelExpr),
		&memo.AlterTableRelocatePrivate{
			RelocateLease: relocate.RelocateLease,
			AlterTableSplitPrivate: memo.AlterTableSplitPrivate{
				Table:   b.factory.Metadata().AddTable(table, &tn),
				Index:   index.Ordinal(),
				Columns: colsToColList(outScope.cols),
				Props:   inputScope.makePhysicalProps(),
			},
		},
	)
	return outScope
}

// getIndexColumnNamesAndTypes returns the names and types of the index columns.
func getIndexColumnNamesAndTypes(index cat.Index) (colNames []string, colTypes []*types.T) {
	colNames = make([]string, index.LaxKeyColumnCount())
	colTypes = make([]*types.T, index.LaxKeyColumnCount())
	for i := range colNames {
		c := index.Column(i)
		colNames[i] = string(c.ColName())
		colTypes[i] = c.DatumType()
	}
	return colNames, colTypes
}

// checkInputColumns verifies the types of the columns in the given scope. The
// input must have at least minPrefix columns, and their types must match that
// prefix of colTypes.
func checkInputColumns(
	context string, inputScope *scope, colNames []string, colTypes []*types.T, minPrefix int,
) {
	if len(inputScope.cols) < minPrefix {
		if len(inputScope.cols) == 0 {
			panic(pgerror.Newf(pgcode.Syntax, "no columns in %s data", context))
		}
		panic(pgerror.Newf(pgcode.Syntax, "less than %d columns in %s data", minPrefix, context))
	}
	if len(inputScope.cols) > len(colTypes) {
		panic(pgerror.Newf(pgcode.Syntax, "too many columns in %s data", context))
	}
	for i := range inputScope.cols {
		if !inputScope.cols[i].typ.Equivalent(colTypes[i]) {
			panic(pgerror.Newf(
				pgcode.Syntax, "%s data column %d (%s) must be of type %s, not type %s",
				context, i+1, colNames[i], colTypes[i], inputScope.cols[i].typ,
			))
		}
	}
}
