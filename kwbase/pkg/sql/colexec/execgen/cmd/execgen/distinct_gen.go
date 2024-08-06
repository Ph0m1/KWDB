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

package main

import (
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

const distinctOpsTmpl = "pkg/sql/colexec/distinct_tmpl.go"

func genDistinctOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(distinctOpsTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	innerLoopRe := makeFunctionRegex("_CHECK_DISTINCT", 5)
	s = innerLoopRe.ReplaceAllString(s, `{{template "checkDistinct" buildDict "Global" . "LTyp" .LTyp}}`)

	innerLoopNullsRe := makeFunctionRegex("_CHECK_DISTINCT_WITH_NULLS", 7)
	s = innerLoopNullsRe.ReplaceAllString(s, `{{template "checkDistinctWithNulls" buildDict "Global" . "LTyp" .LTyp}}`)
	s = replaceManipulationFuncs(".LTyp", s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("distinct_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.NE])
}
func init() {
	registerGenerator(genDistinctOps, "distinct.eg.go", distinctOpsTmpl)
}
