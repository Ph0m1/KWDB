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

package execbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func init() {
	// Install the interceptor that implements the ExprFmtHideScalars functionality.
	memo.ScalarFmtInterceptor = fmtInterceptor
}

// fmtInterceptor is a function suitable for memo.ScalarFmtInterceptor. It detects
// if an expression tree contains only scalar expressions; if so, it tries to
// execbuild them and print the SQL expressions.
func fmtInterceptor(f *memo.ExprFmtCtx, scalar opt.ScalarExpr) string {
	if !onlyScalars(scalar) {
		return ""
	}

	// Let the filters node show up; we will apply the code on each filter.
	if scalar.Op() == opt.FiltersOp {
		return ""
	}

	// Build the scalar expression and format it as a single string.
	bld := New(nil /* factory */, f.Memo, nil /* catalog */, scalar, nil /* evalCtx */)
	md := f.Memo.Metadata()
	ivh := tree.MakeIndexedVarHelper(nil /* container */, md.NumColumns())
	expr, err := bld.BuildScalar(&ivh)
	if err != nil {
		// Not all scalar operators are supported (e.g. Projections).
		return ""
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
		ctx.WriteString(f.ColumnString(opt.ColumnID(idx + 1)))
	})
	expr.Format(fmtCtx)
	return fmtCtx.String()
}

func onlyScalars(expr opt.Expr) bool {
	if !opt.IsScalarOp(expr) {
		return false
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if !onlyScalars(expr.Child(i)) {
			return false
		}
	}
	return true
}
