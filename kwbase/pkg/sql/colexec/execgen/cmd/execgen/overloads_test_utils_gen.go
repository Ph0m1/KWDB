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

package main

import (
	"io"
	"text/template"
)

const overloadsTestUtilsTemplate = `
package colexec

import (
	"bytes"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
)

{{define "opName"}}perform{{.Name}}{{.LTyp}}{{.RTyp}}{{end}}

{{/* The range is over all overloads */}}
{{range .}}

func {{template "opName" .}}(a {{.LTyp.GoTypeName}}, b {{.RTyp.GoTypeName}}) {{.RetTyp.GoTypeName}} {
	var r {{.RetTyp.GoTypeName}}
	// In order to inline the templated code of overloads, we need to have a
	// "decimalScratch" local variable of type "decimalOverloadScratch".
	var decimalScratch decimalOverloadScratch
	// However, the scratch is not used in all of the functions, so we add this
	// to go around "unused" error.
	_ = decimalScratch
	{{(.Assign "r" "a" "b")}}
	return r
}

{{end}}
`

// genOverloadsTestUtils creates a file that has a function for each overload
// defined in overloads.go. This is so that we can more easily test each
// overload.
func genOverloadsTestUtils(wr io.Writer) error {
	tmpl, err := template.New("overloads_test_utils").Parse(overloadsTestUtilsTemplate)
	if err != nil {
		return err
	}

	allOverloads := make([]*overload, 0)
	allOverloads = append(allOverloads, binaryOpOverloads...)
	allOverloads = append(allOverloads, comparisonOpOverloads...)
	return tmpl.Execute(wr, allOverloads)
}

func init() {
	registerGenerator(genOverloadsTestUtils, "overloads_test_utils.eg.go", "")
}
