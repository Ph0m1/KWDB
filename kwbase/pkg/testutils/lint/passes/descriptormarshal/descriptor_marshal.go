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

// Package descriptormarshal defines an suite of Analyzers that
// detects correct setting of timestamps when unmarshaling table
// descriptors.
package descriptormarshal

import (
	"go/ast"
	"go/types"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/lint/passes/passesutil"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Doc documents this pass.
const Doc = `check for correct unmarshaling of sqlbase descriptors`

// TODO(ajwerner): write an Analyzer which determines whether a function passes
// a pointer to a struct which contains a sqlbase.Descriptor to a function
// which will pass that pointer to protoutil.Unmarshal and verify that said
// function also calls Descriptor.Table().

const sqlbasePkg = "gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"

const name = "descriptormarshal"

// Analyzer is a linter that ensures there are no calls to
// sqlbase.Descriptor.GetTable() except where appropriate.
var Analyzer = &analysis.Analyzer{
	Name:     name,
	Doc:      Doc,
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run: func(pass *analysis.Pass) (interface{}, error) {
		inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
		inspect.Preorder([]ast.Node{
			(*ast.CallExpr)(nil),
		}, func(n ast.Node) {
			call := n.(*ast.CallExpr)
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return
			}
			obj, ok := pass.TypesInfo.Uses[sel.Sel]
			if !ok {
				return
			}
			f, ok := obj.(*types.Func)
			if !ok {
				return
			}
			if f.Pkg() == nil || f.Pkg().Path() != sqlbasePkg || f.Name() != "GetTable" {
				return
			}
			if !isMethodForNamedType(f, "Descriptor") {
				return
			}

			if passesutil.HasNolintComment(pass, sel, name) {
				return
			}
			pass.Report(analysis.Diagnostic{
				Pos:     n.Pos(),
				Message: "Illegal call to Descriptor.GetTable(), see sqlbase.TableFromDescriptor()",
			})
		})
		return nil, nil
	},
}

func isMethodForNamedType(f *types.Func, name string) bool {
	sig := f.Type().(*types.Signature)
	recv := sig.Recv()
	if recv == nil { // not a method
		return false
	}
	switch recv := recv.Type().(type) {
	case *types.Named:
		return recv.Obj().Name() == name
	case *types.Pointer:
		named, ok := recv.Elem().(*types.Named)
		return ok && named.Obj().Name() == name
	}
	return false
}
