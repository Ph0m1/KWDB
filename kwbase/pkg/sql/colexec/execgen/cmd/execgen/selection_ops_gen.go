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
)

const selectionOpsTmpl = "pkg/sql/colexec/selection_ops_tmpl.go"

func getSelectionOpsTmpl() (*template.Template, error) {
	t, err := ioutil.ReadFile(selectionOpsTmpl)
	if err != nil {
		return nil, err
	}

	s := string(t)
	s = strings.Replace(s, "_OP_CONST_NAME", "sel{{.Name}}{{.LTyp}}{{.RTyp}}ConstOp", -1)
	s = strings.Replace(s, "_OP_NAME", "sel{{.Name}}{{.LTyp}}{{.RTyp}}Op", -1)
	s = strings.Replace(s, "_R_GO_TYPE", "{{.RGoType}}", -1)
	s = strings.Replace(s, "_L_TYP_VAR", "{{$lTyp}}", -1)
	s = strings.Replace(s, "_R_TYP_VAR", "{{$rTyp}}", -1)
	s = strings.Replace(s, "_L_TYP", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_R_TYP", "{{.RTyp}}", -1)
	s = strings.Replace(s, "_NAME", "{{.Name}}", -1)

	assignCmpRe := makeFunctionRegex("_ASSIGN_CMP", 3)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 3))

	s = replaceManipulationFuncs(".LTyp", s)
	s = strings.Replace(s, "_R_UNSAFEGET", "execgen.UNSAFEGET", -1)
	s = strings.Replace(s, "_R_SLICE", "execgen.SLICE", -1)
	s = replaceManipulationFuncs(".RTyp", s)

	s = strings.Replace(s, "_HAS_NULLS", "$hasNulls", -1)
	selConstLoop := makeFunctionRegex("_SEL_CONST_LOOP", 1)
	s = selConstLoop.ReplaceAllString(s, `{{template "selConstLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)
	selLoop := makeFunctionRegex("_SEL_LOOP", 1)
	s = selLoop.ReplaceAllString(s, `{{template "selLoop" buildDict "Global" $ "HasNulls" $1 "Overload" .}}`)

	return template.New("selection_ops").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
}

func genSelectionOps(wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl()
	if err != nil {
		return err
	}
	lTypToRTypToOverloads := make(map[coltypes.T]map[coltypes.T][]*overload)
	for _, ov := range comparisonOpOverloads {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverloads := lTypToRTypToOverloads[lTyp]
		if rTypToOverloads == nil {
			rTypToOverloads = make(map[coltypes.T][]*overload)
			lTypToRTypToOverloads[lTyp] = rTypToOverloads
		}
		rTypToOverloads[rTyp] = append(rTypToOverloads[rTyp], ov)
	}
	return tmpl.Execute(wr, lTypToRTypToOverloads)
}

func init() {
	registerGenerator(genSelectionOps, "selection_ops.eg.go", selectionOpsTmpl)
}
