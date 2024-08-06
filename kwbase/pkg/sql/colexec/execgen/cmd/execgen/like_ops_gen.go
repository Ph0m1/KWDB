// Copyright 2019 The Cockroach Authors.
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
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

// likeTemplate depends on the selConstOp template from selection_ops_gen. We
// handle LIKE operators separately from the other selection operators because
// there are several different implementations which may be chosen depending on
// the complexity of the LIKE pattern.
const likeTemplate = `
package colexec

import (
	"bytes"
	"context"
	"regexp"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
)

{{range .}}
{{template "selConstOp" .}}
{{template "projConstOp" .}}
{{end}}
`

func genLikeOps(wr io.Writer) error {
	tmpl, err := getSelectionOpsTmpl()
	if err != nil {
		return err
	}
	projTemplate, err := getProjConstOpTmplString(false /* isConstLeft */)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Funcs(template.FuncMap{"buildDict": buildDict}).Parse(projTemplate)
	if err != nil {
		return err
	}
	tmpl, err = tmpl.Parse(likeTemplate)
	if err != nil {
		return err
	}
	overloads := []overload{
		{
			Name:    "Prefix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Suffix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "Regexp",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "*regexp.Regexp",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = %s.Match(%s)", target, r, l)
			},
		},
		{
			Name:    "NotPrefix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasPrefix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotSuffix",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "[]byte",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !bytes.HasSuffix(%s, %s)", target, l, r)
			},
		},
		{
			Name:    "NotRegexp",
			LTyp:    coltypes.Bytes,
			RTyp:    coltypes.Bytes,
			RGoType: "*regexp.Regexp",
			AssignFunc: func(_ overload, target, l, r string) string {
				return fmt.Sprintf("%s = !%s.Match(%s)", target, r, l)
			},
		},
	}
	return tmpl.Execute(wr, overloads)
}

func init() {
	registerGenerator(genLikeOps, "like_ops.eg.go", selectionOpsTmpl)
}
