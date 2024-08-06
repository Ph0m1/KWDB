// Copyright 2020 The Cockroach Authors.
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

const hashAggTmpl = "pkg/sql/colexec/hash_aggregator_tmpl.go"

func genHashAggregator(wr io.Writer) error {
	t, err := ioutil.ReadFile(hashAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = replaceManipulationFuncs(".Global.LTyp", s)

	assignCmpRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignCmpRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	matchLoop := makeFunctionRegex("_MATCH_LOOP", 8)
	s = matchLoop.ReplaceAllString(
		s, `{{template "matchLoop" buildDict "Global" . "LhsMaybeHasNulls" $7 "RhsMaybeHasNulls" $8}}`)

	tmpl, err := template.New("hash_aggregator").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, sameTypeComparisonOpToOverloads[tree.NE])
}

func init() {
	registerGenerator(genHashAggregator, "hash_aggregator.eg.go", hashAggTmpl)
}
