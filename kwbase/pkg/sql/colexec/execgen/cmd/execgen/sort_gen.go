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

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type sortOverload struct {
	*overload
	Dir       string
	DirString string
	Nulls     bool
}

type sortOverloads struct {
	LTyp      coltypes.T
	Overloads []sortOverload
}

// typesToSortOverloads maps types to whether nulls are handled to
// the overload representing the sort direction.
var typesToSortOverloads map[coltypes.T]map[bool]sortOverloads

const sortOpsTmpl = "pkg/sql/colexec/sort_tmpl.go"

func genSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(sortOpsTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{$typ}}", -1)
	s = strings.Replace(s, "_TYPE", "{{$typ}}", -1)
	s = strings.Replace(s, "_DIR_ENUM", "{{.Dir}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_ISNULL", "{{$isNull}}", -1)
	s = strings.Replace(s, "_HANDLES_NULLS", "{{if .Nulls}}WithNulls{{else}}{{end}}", -1)

	assignLtRe := makeFunctionRegex("_ASSIGN_LT", 3)
	s = assignLtRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 3))

	s = replaceManipulationFuncs(".LTyp", s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("sort_op").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

const quickSortTmpl = "pkg/sql/colexec/quicksort_tmpl.go"

func genQuickSortOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(quickSortTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_DIR", "{{.DirString}}", -1)
	s = strings.Replace(s, "_HANDLES_NULLS", "{{if .Nulls}}WithNulls{{else}}{{end}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("quicksort").Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, typesToSortOverloads)
}

func init() {
	registerGenerator(genSortOps, "sort.eg.go", sortOpsTmpl)
	registerGenerator(genQuickSortOps, "quicksort.eg.go", quickSortTmpl)
	typesToSortOverloads = make(map[coltypes.T]map[bool]sortOverloads)
	for _, o := range sameTypeComparisonOpToOverloads[tree.LT] {
		typesToSortOverloads[o.LTyp] = make(map[bool]sortOverloads)
		for _, b := range []bool{true, false} {
			typesToSortOverloads[o.LTyp][b] = sortOverloads{
				LTyp: o.LTyp,
				Overloads: []sortOverload{
					{overload: o, Dir: "execinfrapb.Ordering_Column_ASC", DirString: "Asc", Nulls: b},
					{}},
			}
		}
	}
	for _, o := range sameTypeComparisonOpToOverloads[tree.GT] {
		for _, b := range []bool{true, false} {
			typesToSortOverloads[o.LTyp][b].Overloads[1] = sortOverload{
				overload: o, Dir: "execinfrapb.Ordering_Column_DESC", DirString: "Desc", Nulls: b}
		}
	}
}
