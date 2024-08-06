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

type aggOverloads struct {
	Agg       string
	Overloads []*overload
}

// AggNameLower returns the aggregation name in lower case, e.g. "min".
func (a aggOverloads) AggNameLower() string {
	return strings.ToLower(a.Agg)
}

// AggNameTitle returns the aggregation name in title case, e.g. "Min".
func (a aggOverloads) AggNameTitle() string {
	return strings.Title(a.AggNameLower())
}

// Avoid unused warning for functions which are only used in templates.
var _ = aggOverloads{}.AggNameLower()
var _ = aggOverloads{}.AggNameTitle()

const minMaxAggTmpl = "pkg/sql/colexec/min_max_agg_tmpl.go"

func genMinMaxAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(minMaxAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_AGG_TITLE", "{{.AggNameTitle}}", -1)
	s = strings.Replace(s, "_AGG", "{{$agg}}", -1)
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 3)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	accumulateMinMax := makeFunctionRegex("_ACCUMULATE_MINMAX", 4)
	s = accumulateMinMax.ReplaceAllString(s, `{{template "accumulateMinMax" buildDict "Global" . "LTyp" .LTyp "HasNulls" $4}}`)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("min_max_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)

	if err != nil {
		return err
	}
	data := []aggOverloads{
		{
			Agg:       "MIN",
			Overloads: sameTypeComparisonOpToOverloads[tree.LT],
		},
		{
			Agg:       "MAX",
			Overloads: sameTypeComparisonOpToOverloads[tree.GT],
		},
	}
	return tmpl.Execute(wr, data)
}

func init() {
	registerGenerator(genMinMaxAgg, "min_max_agg.eg.go", minMaxAggTmpl)
}
