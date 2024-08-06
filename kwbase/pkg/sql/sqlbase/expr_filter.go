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

import "gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"

// RunFilter runs a filter expression and returns whether the filter passes.
func RunFilter(filter tree.TypedExpr, evalCtx *tree.EvalContext) (bool, error) {
	if filter == nil {
		return true, nil
	}

	d, err := filter.Eval(evalCtx)
	if err != nil {
		return false, err
	}

	return d == tree.DBoolTrue, nil
}

type ivarRemapper struct {
	indexVarMap []int
}

var _ tree.Visitor = &ivarRemapper{}

func (v *ivarRemapper) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if ivar, ok := expr.(*tree.IndexedVar); ok {
		newIvar := *ivar
		newIvar.Idx = v.indexVarMap[ivar.Idx]
		return false, &newIvar
	}
	return true, expr
}

func (*ivarRemapper) VisitPost(expr tree.Expr) tree.Expr { return expr }

// RemapIVarsInTypedExpr remaps tree.IndexedVars in expr using indexVarMap.
// Note that a new expression is returned.
func RemapIVarsInTypedExpr(expr tree.TypedExpr, indexVarMap []int) tree.TypedExpr {
	v := &ivarRemapper{indexVarMap: indexVarMap}
	newExpr, _ := tree.WalkExpr(v, expr)
	return newExpr.(tree.TypedExpr)
}
