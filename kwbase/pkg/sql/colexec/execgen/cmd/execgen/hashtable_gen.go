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

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

const hashTableTmpl = "pkg/sql/colexec/hashtable_tmpl.go"

func genHashTable(wr io.Writer) error {
	t, err := ioutil.ReadFile(hashTableTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_PROBE_TYPE", "coltypes.{{$lTyp}}", -1)
	s = strings.Replace(s, "_BUILD_TYPE", "coltypes.{{$rTyp}}", -1)
	s = strings.Replace(s, "_ProbeType", "{{$lTyp}}", -1)
	s = strings.Replace(s, "_BuildType", "{{$rTyp}}", -1)

	assignNeRe := makeFunctionRegex("_ASSIGN_NE", 3)
	s = assignNeRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.Assign", 3))

	checkColBody := makeFunctionRegex("_CHECK_COL_BODY", 12)
	s = checkColBody.ReplaceAllString(
		s,
		`{{template "checkColBody" buildDict "Global" .Global "UseProbeSel" .UseProbeSel "UseBuildSel" .UseBuildSel "ProbeHasNulls" $7 "BuildHasNulls" $8 "AllowNullEquality" $9 "SelectDistinct" $10}}`)

	checkColWithNulls := makeFunctionRegex("_CHECK_COL_WITH_NULLS", 8)
	s = checkColWithNulls.ReplaceAllString(s,
		`{{template "checkColWithNulls" buildDict "Global" . "UseProbeSel" $7 "UseBuildSel" $8}}`)

	checkColForDistinctWithNulls := makeFunctionRegex("_CHECK_COL_FOR_DISTINCT_WITH_NULLS", 8)
	s = checkColForDistinctWithNulls.ReplaceAllString(s,
		`{{template "checkColForDistinctWithNulls" buildDict "Global" . "UseProbeSel" $7 "UseBuildSel" $8}}`)

	checkBody := makeFunctionRegex("_CHECK_BODY", 3)
	s = checkBody.ReplaceAllString(s,
		`{{template "checkBody" buildDict "Global" . "SelectSameTuples" $3}}`)

	updateSelBody := makeFunctionRegex("_UPDATE_SEL_BODY", 4)
	s = updateSelBody.ReplaceAllString(s,
		`{{template "updateSelBody" buildDict "Global" . "UseSel" $4}}`)

	s = replaceManipulationFuncs(".Global.LTyp", s)

	tmpl, err := template.New("hashtable").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	lTypToRTypToOverload := make(map[coltypes.T]map[coltypes.T]*overload)
	for _, ov := range anyTypeComparisonOpToOverloads[tree.NE] {
		lTyp := ov.LTyp
		rTyp := ov.RTyp
		rTypToOverload := lTypToRTypToOverload[lTyp]
		if rTypToOverload == nil {
			rTypToOverload = make(map[coltypes.T]*overload)
			lTypToRTypToOverload[lTyp] = rTypToOverload
		}
		rTypToOverload[rTyp] = ov
	}
	return tmpl.Execute(wr, lTypToRTypToOverload)
}

func init() {
	registerGenerator(genHashTable, "hashtable.eg.go", hashTableTmpl)
}
