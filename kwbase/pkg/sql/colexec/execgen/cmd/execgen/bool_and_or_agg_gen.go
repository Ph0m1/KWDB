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
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
)

type booleanAggTmplInfo struct {
	IsAnd bool
}

func (b booleanAggTmplInfo) AssignBoolOp(target, l, r string) string {
	switch b.IsAnd {
	case true:
		return fmt.Sprintf("%s = %s && %s", target, l, r)
	case false:
		return fmt.Sprintf("%s = %s || %s", target, l, r)
	default:
		execerror.VectorizedInternalPanic("unsupported boolean agg type")
		// This code is unreachable, but the compiler cannot infer that.
		return ""
	}
}

func (b booleanAggTmplInfo) OpType() string {
	if b.IsAnd {
		return "And"
	}
	return "Or"
}

func (b booleanAggTmplInfo) DefaultVal() string {
	if b.IsAnd {
		return "true"
	}
	return "false"
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = booleanAggTmplInfo{}.AssignBoolOp
	_ = booleanAggTmplInfo{}.OpType
	_ = booleanAggTmplInfo{}.DefaultVal
)

const boolAggTmpl = "pkg/sql/colexec/bool_and_or_agg_tmpl.go"

func genBooleanAgg(wr io.Writer) error {
	t, err := ioutil.ReadFile(boolAggTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	s = strings.Replace(s, "_OP_TYPE", "{{.OpType}}", -1)
	s = strings.Replace(s, "_DEFAULT_VAL", "{{.DefaultVal}}", -1)

	accumulateBoolean := makeFunctionRegex("_ACCUMULATE_BOOLEAN", 3)
	s = accumulateBoolean.ReplaceAllString(s, `{{template "accumulateBoolean" buildDict "Global" .}}`)

	assignBoolRe := makeFunctionRegex("_ASSIGN_BOOL_OP", 3)
	s = assignBoolRe.ReplaceAllString(s, makeTemplateFunctionCall(`AssignBoolOp`, 3))

	tmpl, err := template.New("bool_and_or_agg").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, []booleanAggTmplInfo{{IsAnd: true}, {IsAnd: false}})
}

func init() {
	registerGenerator(genBooleanAgg, "bool_and_or_agg.eg.go", boolAggTmpl)
}
