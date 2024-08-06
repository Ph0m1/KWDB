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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// buildExport builds an EXPORT statement.
func (b *Builder) buildExport(export *tree.Export, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(export.Query, nil /* desiredTypes */, emptyScope)

	texpr := emptyScope.resolveType(export.File, types.String)
	fileName := b.buildScalar(
		texpr, emptyScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
	)
	options := b.buildKVOptions(export.Options, emptyScope)

	outScope = inScope.push()
	if export.IsTS {
		b.synthesizeResultColumns(outScope, sqlbase.ExportTsColumns)
	} else {
		b.synthesizeResultColumns(outScope, sqlbase.ExportColumns)
	}
	outScope.expr = b.factory.ConstructExport(
		inputScope.expr,
		fileName,
		options,
		&memo.ExportPrivate{
			FileFormat: export.FileFormat,
			Columns:    colsToColList(outScope.cols),
			Props:      inputScope.makePhysicalProps(),
		},
	)
	return outScope
}

func (b *Builder) buildKVOptions(opts tree.KVOptions, inScope *scope) memo.KVOptionsExpr {
	res := make(memo.KVOptionsExpr, len(opts))
	for i := range opts {
		res[i].Key = string(opts[i].Key)
		if opts[i].Value != nil {
			texpr := inScope.resolveType(opts[i].Value, types.String)
			res[i].Value = b.buildScalar(
				texpr, inScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
			)
		} else {
			res[i].Value = b.factory.ConstructNull(types.String)
		}
		res[i].Typ = types.String
	}
	return res
}
