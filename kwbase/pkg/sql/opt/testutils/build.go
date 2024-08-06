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

package testutils

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optbuilder"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/xform"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// BuildQuery initializes an optimizer and builds the given sql statement.
func BuildQuery(
	t *testing.T, o *xform.Optimizer, catalog cat.Catalog, evalCtx *tree.EvalContext, sql string,
) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
		t.Fatal(err)
	}
	semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	o.Init(evalCtx, catalog)
	err = optbuilder.New(ctx, &semaCtx, evalCtx, catalog, o.Factory(), stmt.AST).Build()
	if err != nil {
		t.Fatal(err)
	}
}
