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

type rowNumberTmplInfo struct {
	HasPartition bool
	String       string
}

const rowNumberTmpl = "pkg/sql/colexec/row_number_tmpl.go"

func genRowNumberOp(wr io.Writer) error {
	d, err := ioutil.ReadFile(rowNumberTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_ROW_NUMBER_STRING", "{{.String}}", -1)

	// Now, generate the op, from the template.
	tmpl, err := template.New("row_number_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	rankTmplInfos := []rowNumberTmplInfo{
		{HasPartition: false, String: "rowNumberNoPartition"},
		{HasPartition: true, String: "rowNumberWithPartition"},
	}
	return tmpl.Execute(wr, rankTmplInfos)
}

func init() {
	registerGenerator(genRowNumberOp, "row_number.eg.go", rowNumberTmpl)
}
