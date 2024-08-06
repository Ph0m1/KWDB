// Copyright 2019 The Cockroach Authors.
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
	"context"
	"reflect"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// BuildOpaqueFn is a handler for building the metadata for an opaque statement.
type BuildOpaqueFn func(
	context.Context, *tree.SemaContext, *tree.EvalContext, tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error)

// OpaqueType indicates whether an opaque statement can mutate data or change
// schema.
type OpaqueType int

const (
	// OpaqueReadOnly is used for statements that do not mutate state as part of
	// the transaction, and can be run in read-only transactions.
	OpaqueReadOnly OpaqueType = iota

	// OpaqueMutation is used for statements that mutate data and cannot be run as
	// part of read-only transactions.
	OpaqueMutation

	// OpaqueDDL is used for statements that change a schema and cannot be
	// executed following a mutation in the same transaction.
	OpaqueDDL
)

// RegisterOpaque registers an opaque handler for a specific statement type.
func RegisterOpaque(stmtType reflect.Type, opaqueType OpaqueType, fn BuildOpaqueFn) {
	if _, ok := opaqueStatements[stmtType]; ok {
		panic(errors.AssertionFailedf("opaque statement %s already registered", stmtType))
	}
	opaqueStatements[stmtType] = opaqueStmtInfo{
		typ:     opaqueType,
		buildFn: fn,
	}
}

type opaqueStmtInfo struct {
	typ     OpaqueType
	buildFn BuildOpaqueFn
}

var opaqueStatements = make(map[reflect.Type]opaqueStmtInfo)

func (b *Builder) tryBuildOpaque(stmt tree.Statement, inScope *scope) (outScope *scope) {
	info, ok := opaqueStatements[reflect.TypeOf(stmt)]
	if !ok {
		return nil
	}
	obj, cols, err := info.buildFn(b.ctx, b.semaCtx, b.evalCtx, stmt)
	if err != nil {
		panic(err)
	}
	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, cols)
	private := &memo.OpaqueRelPrivate{
		Columns:  colsToColList(outScope.cols),
		Metadata: obj,
	}
	switch info.typ {
	case OpaqueReadOnly:
		outScope.expr = b.factory.ConstructOpaqueRel(private)
	case OpaqueMutation:
		outScope.expr = b.factory.ConstructOpaqueMutation(private)
	case OpaqueDDL:
		outScope.expr = b.factory.ConstructOpaqueDDL(private)
	default:
		panic(errors.AssertionFailedf("invalid opaque statement type %d", info.typ))
	}
	return outScope
}
