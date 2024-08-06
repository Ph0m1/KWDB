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

package tree_test

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestConstantEvalArrayComparison(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer tree.MockNameTypes(map[string]*types.T{"a": types.MakeArray(types.Int)})()

	expr, err := parser.ParseExpr("a = ARRAY[1:::INT,2:::INT]")
	if err != nil {
		t.Fatal(err)
	}

	semaCtx := tree.MakeSemaContext()
	typedExpr, err := expr.TypeCheck(&semaCtx, types.Any)
	if err != nil {
		t.Fatal(err)
	}

	ctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer ctx.Mon.Stop(context.Background())
	c := tree.MakeConstantEvalVisitor(ctx)
	expr, _ = tree.WalkExpr(&c, typedExpr)
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}

	left := tree.ColumnItem{
		ColumnName: "a",
	}
	right := tree.DArray{
		ParamTyp:    types.Int,
		Array:       tree.Datums{tree.NewDInt(1), tree.NewDInt(2)},
		HasNonNulls: true,
	}
	expected := tree.NewTypedComparisonExpr(tree.EQ, &left, &right)
	if !reflect.DeepEqual(expr, expected) {
		t.Errorf("invalid expr '%v', expected '%v'", expr, expected)
	}
}
