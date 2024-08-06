// Copyright 2017 The Cockroach Authors.
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

package transform

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

// IsAggregateVisitor checks if walked expressions contain aggregate functions.
type IsAggregateVisitor struct {
	Aggregated bool
	// searchPath is used to search for unqualified function names.
	searchPath sessiondata.SearchPath
}

var _ tree.Visitor = &IsAggregateVisitor{}

// VisitPre satisfies the Visitor interface.
func (v *IsAggregateVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsWindowFunctionApplication() {
			// A window function application of an aggregate builtin is not an
			// aggregate function, but it can contain aggregate functions.
			return true, expr
		}
		fd, err := t.Func.Resolve(v.searchPath)
		if err != nil {
			return false, expr
		}
		if fd.Class == tree.AggregateClass {
			v.Aggregated = true
			return false, expr
		}
	case *tree.Subquery:
		return false, expr
	}

	return true, expr
}

// VisitPost satisfies the Visitor interface.
func (*IsAggregateVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }
