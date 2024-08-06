// Copyright 2019 The Cockroach Authors.
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
)

type logicalOperation struct {
	Lower string
	Title string

	IsOr bool
}

const andOrProjTmpl = "pkg/sql/colexec/and_or_projection_tmpl.go"

func genAndOrProjectionOps(wr io.Writer) error {
	t, err := ioutil.ReadFile(andOrProjTmpl)
	if err != nil {
		return err
	}

	s := string(t)
	s = strings.Replace(s, "_OP_LOWER", "{{.Lower}}", -1)
	s = strings.Replace(s, "_OP_TITLE", "{{.Title}}", -1)
	s = strings.Replace(s, "_IS_OR_OP", ".IsOr", -1)
	s = strings.Replace(s, "_L_HAS_NULLS", "$.lHasNulls", -1)
	s = strings.Replace(s, "_R_HAS_NULLS", "$.rHasNulls", -1)

	addTupleForRight := makeFunctionRegex("_ADD_TUPLE_FOR_RIGHT", 1)
	s = addTupleForRight.ReplaceAllString(s, `{{template "addTupleForRight" buildDict "Global" $ "lHasNulls" $1}}`)
	setValues := makeFunctionRegex("_SET_VALUES", 3)
	s = setValues.ReplaceAllString(s, `{{template "setValues" buildDict "Global" $ "IsOr" $1 "lHasNulls" $2 "rHasNulls" $3}}`)
	setSingleValue := makeFunctionRegex("_SET_SINGLE_VALUE", 3)
	s = setSingleValue.ReplaceAllString(s, `{{template "setSingleValue" buildDict "Global" $ "IsOr" $1 "lHasNulls" $2 "rHasNulls" $3}}`)

	tmpl, err := template.New("and").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	operations := []logicalOperation{
		{
			Lower: "and",
			Title: "And",
			IsOr:  false,
		},
		{
			Lower: "or",
			Title: "Or",
			IsOr:  true,
		},
	}

	return tmpl.Execute(wr, operations)
}

func init() {
	registerGenerator(genAndOrProjectionOps, "and_or_projection.eg.go", andOrProjTmpl)
}
