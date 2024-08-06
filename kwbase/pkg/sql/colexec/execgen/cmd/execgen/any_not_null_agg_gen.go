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

const anyNotNullAggTmpl = "pkg/sql/colexec/any_not_null_agg_tmpl.go"

func genAnyNotNullAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(anyNotNullAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)

	findAnyNotNull := makeFunctionRegex("_FIND_ANY_NOT_NULL", 4)
	s = findAnyNotNull.ReplaceAllString(s, `{{template "findAnyNotNull" buildDict "Global" . "LTyp" .LTyp "HasNulls" $4}}`)

	s = replaceManipulationFuncs(".LTyp", s)

	tmpl, err := template.New("any_not_null_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.EQ])
}

func init() {
	registerGenerator(genAnyNotNullAgg, "any_not_null_agg.eg.go", anyNotNullAggTmpl)
}
