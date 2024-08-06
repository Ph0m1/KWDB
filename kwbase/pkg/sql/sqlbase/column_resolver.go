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
	"bytes"
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// ProcessTargetColumns returns the column descriptors identified by the
// given name list. It also checks that a given column name is only
// listed once. If no column names are given (special case for INSERT)
// and ensureColumns is set, the descriptors for all visible columns
// are returned. If allowMutations is set, even columns undergoing
// mutations are added.
func ProcessTargetColumns(
	tableDesc *ImmutableTableDescriptor, nameList tree.NameList, ensureColumns, allowMutations bool,
) ([]ColumnDescriptor, error) {
	if len(nameList) == 0 {
		if ensureColumns {
			// VisibleColumns is used here to prevent INSERT INTO <table> VALUES (...)
			// (as opposed to INSERT INTO <table> (...) VALUES (...)) from writing
			// hidden columns. At present, the only hidden column is the implicit rowid
			// primary key column.
			return tableDesc.VisibleColumns(), nil
		}
		return nil, nil
	}

	cols := make([]ColumnDescriptor, len(nameList))
	colIDSet := make(map[ColumnID]struct{}, len(nameList))
	for i, colName := range nameList {
		var col *ColumnDescriptor
		var err error
		if allowMutations {
			col, _, err = tableDesc.FindColumnByName(colName)
		} else {
			col, err = tableDesc.FindActiveColumnByName(string(colName))
		}
		if err != nil {
			return nil, err
		}

		if _, ok := colIDSet[col.ID]; ok {
			return nil, pgerror.Newf(pgcode.Syntax,
				"multiple assignments to the same column %q", &nameList[i])
		}
		colIDSet[col.ID] = struct{}{}
		cols[i] = *col
	}

	return cols, nil
}

// sourceNameMatches checks whether a request for table name toFind
// can be satisfied by the FROM source name srcName.
//
// For example:
// - a request for "kv" is matched by a source named "db1.public.kv"
// - a request for "public.kv" is not matched by a source named just "kv"
func sourceNameMatches(srcName *tree.TableName, toFind tree.TableName) bool {
	if srcName.TableName != toFind.TableName {
		return false
	}
	if toFind.ExplicitSchema {
		if !srcName.ExplicitSchema || srcName.SchemaName != toFind.SchemaName {
			return false
		}
		if toFind.ExplicitCatalog {
			if !srcName.ExplicitCatalog || srcName.CatalogName != toFind.CatalogName {
				return false
			}
		}
	}
	return true
}

// ColumnResolver is a utility struct to be used when resolving column
// names to point to one of the data sources and one of the column IDs
// in that data source.
type ColumnResolver struct {
	Source *DataSourceInfo

	// ResolverState is modified in-place by the implementation of the
	// tree.ColumnItemResolver interface in resolver.go.
	ResolverState struct {
		ColIdx int
	}
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) FindSourceMatchingName(
	ctx context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	err error,
) {
	if !sourceNameMatches(&r.Source.SourceAlias, tn) {
		return tree.NoResults, nil, nil, nil
	}
	prefix = &r.Source.SourceAlias
	return tree.ExactlyOne, prefix, nil, nil
}

const invalidColIdx = -1

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) FindSourceProvidingColumn(
	ctx context.Context, col tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	colIdx := invalidColIdx
	colName := string(col)

	for idx := range r.Source.SourceColumns {
		colIdx, err = r.findColHelper(colName, colIdx, idx)
		if err != nil {
			return nil, nil, -1, err
		}
		if colIdx != invalidColIdx {
			prefix = &r.Source.SourceAlias
			break
		}
	}
	if colIdx == invalidColIdx {
		colAlloc := col
		return nil, nil, -1, NewUndefinedColumnError(tree.ErrString(&colAlloc))
	}
	r.ResolverState.ColIdx = colIdx
	return prefix, nil, colIdx, nil
}

// Resolve is part of the tree.ColumnItemResolver interface.
func (r *ColumnResolver) Resolve(
	ctx context.Context,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	col tree.Name,
) (tree.ColumnResolutionResult, error) {
	if colHint != -1 {
		// (*ColumnItem).Resolve() is telling us that we found the source
		// via FindSourceProvidingColumn(). So we can count on
		// r.ResolverState.ColIdx being set already. There's nothing remaining
		// to do!
		return nil, nil
	}

	// If we're here, we just know that some source alias was found that
	// matches the column prefix, but we haven't found the column
	// yet. Do this now.
	// FindSourceMatchingName() was careful to set r.ResolverState.SrcIdx
	// and r.ResolverState.ColSetIdx for us.
	colIdx := invalidColIdx
	colName := string(col)
	for idx := range r.Source.SourceColumns {
		var err error
		colIdx, err = r.findColHelper(colName, colIdx, idx)
		if err != nil {
			return nil, err
		}
	}

	if colIdx == invalidColIdx {
		r.ResolverState.ColIdx = invalidColIdx
		return nil, NewUndefinedColumnError(
			tree.ErrString(tree.NewColumnItem(&r.Source.SourceAlias, tree.Name(colName))))
	}
	r.ResolverState.ColIdx = colIdx
	return nil, nil
}

// findColHelper is used by FindSourceProvidingColumn and Resolve above.
// It checks whether a column name is available in a given data source.
func (r *ColumnResolver) findColHelper(colName string, colIdx, idx int) (int, error) {
	col := r.Source.SourceColumns[idx]
	if col.Name == colName {
		if colIdx != invalidColIdx {
			colString := tree.ErrString(r.Source.NodeFormatter(idx))
			var msgBuf bytes.Buffer
			name := tree.ErrString(&r.Source.SourceAlias)
			if len(name) == 0 {
				name = "<anonymous>"
			}
			fmt.Fprintf(&msgBuf, "%s.%s", name, colString)
			return invalidColIdx, pgerror.Newf(pgcode.AmbiguousColumn,
				"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String())
		}
		colIdx = idx
	}
	return colIdx, nil
}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (*TableDescriptor) NameResolutionResult() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (*DatabaseDescriptor) SchemaMeta() {}

// SchemaMeta implements the tree.SchemaMeta interface.
func (Descriptor) SchemaMeta() {}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (Descriptor) NameResolutionResult() {}
