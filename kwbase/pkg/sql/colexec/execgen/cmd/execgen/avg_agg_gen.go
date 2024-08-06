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
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type avgAggTmplInfo struct {
	Type      coltypes.T
	assignAdd func(string, string, string) string
}

func (a avgAggTmplInfo) AssignAdd(target, l, r string) string {
	return a.assignAdd(target, l, r)
}

func (a avgAggTmplInfo) AssignDivInt64(target, l, r string) string {
	switch a.Type {
	case coltypes.Decimal:
		return fmt.Sprintf(
			`%s.SetInt64(%s)
if _, err := tree.DecimalCtx.Quo(&%s, &%s, &%s); err != nil {
			execerror.VectorizedInternalPanic(err)
		}`,
			target, r, target, l, target,
		)
	case coltypes.Float64:
		return fmt.Sprintf("%s = %s / float64(%s)", target, l, r)
	default:
		execerror.VectorizedInternalPanic("unsupported avg agg type")
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = avgAggTmplInfo{}.AssignAdd
	_ = avgAggTmplInfo{}.AssignDivInt64
)

const avgAggTmpl = "pkg/sql/colexec/avg_agg_tmpl.go"

func genAvgAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(avgAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_GOTYPE", "{{.Type.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.Type}}", -1)
	s = strings.Replace(s, "_TYPE", "{{.Type}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.Type}}", -1)

	assignDivRe := makeFunctionRegex("_ASSIGN_DIV_INT64", 3)
	s = assignDivRe.ReplaceAllString(s, makeTemplateFunctionCall("AssignDivInt64", 3))
	assignAddRe := makeFunctionRegex("_ASSIGN_ADD", 3)
	s = assignAddRe.ReplaceAllString(s, makeTemplateFunctionCall("Global.AssignAdd", 3))

	accumulateAvg := makeFunctionRegex("_ACCUMULATE_AVG", 4)
	s = accumulateAvg.ReplaceAllString(s, `{{template "accumulateAvg" buildDict "Global" . "HasNulls" $4}}`)

	tmpl, err := template.New("avg_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	// TODO(asubiotto): Support more coltypes.
	supportedTypes := []coltypes.T{coltypes.Decimal, coltypes.Float64}
	spm := make(map[coltypes.T]int)
	for i, typ := range supportedTypes {
		spm[typ] = i
	}
	tmplInfos := make([]avgAggTmplInfo, len(supportedTypes))
	for _, o := range sameTypeBinaryOpToOverloads[tree.Plus] {
		i, ok := spm[o.LTyp]
		if !ok {
			continue
		}
		tmplInfos[i].Type = o.LTyp
		tmplInfos[i].assignAdd = o.Assign
	}

	return tmpl.Execute(wr, tmplInfos)
}

func init() {
	registerGenerator(genAvgAgg, "avg_agg.eg.go", avgAggTmpl)
}
