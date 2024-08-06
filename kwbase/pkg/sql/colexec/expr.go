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

package colexec

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// Remove unused warning.
var _ = findIVarsInRange

// findIVarsInRange searches Expr for presence of tree.IndexedVars with indices
// in range [start, end). It returns a slice containing all such indices.
func findIVarsInRange(expr execinfrapb.Expression, start int, end int) ([]uint32, error) {
	res := make([]uint32, 0)
	if start >= end {
		return res, nil
	}
	var exprToWalk tree.Expr
	if expr.LocalExpr != nil {
		exprToWalk = expr.LocalExpr
	} else {
		e, err := parser.ParseExpr(expr.Expr)
		if err != nil {
			return nil, err
		}
		exprToWalk = e
	}
	visitor := ivarExpressionVisitor{ivarSeen: make([]bool, end)}
	_, _ = tree.WalkExpr(visitor, exprToWalk)
	for i := start; i < end; i++ {
		if visitor.ivarSeen[i] {
			res = append(res, uint32(i))
		}
	}
	return res, nil
}

type ivarExpressionVisitor struct {
	ivarSeen []bool
}

var _ tree.Visitor = &ivarExpressionVisitor{}

// VisitPre is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	switch e := expr.(type) {
	case *tree.IndexedVar:
		if e.Idx < len(i.ivarSeen) {
			i.ivarSeen[e.Idx] = true
		}
		return false, expr
	default:
		return true, expr
	}
}

// VisitPost is a part of tree.Visitor interface.
func (i ivarExpressionVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
