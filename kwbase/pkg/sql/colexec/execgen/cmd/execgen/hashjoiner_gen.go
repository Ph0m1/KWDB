// Copyright 2018 The Cockroach Authors.
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
	"text/template"
)

const hashJoinerTmpl = "pkg/sql/colexec/hashjoiner_tmpl.go"

func genHashJoiner(wr io.Writer) error {
	t, err := ioutil.ReadFile(hashJoinerTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	distinctCollectRightOuter := makeFunctionRegex("_DISTINCT_COLLECT_PROBE_OUTER", 3)
	s = distinctCollectRightOuter.ReplaceAllString(s, `{{template "distinctCollectProbeOuter" buildDict "Global" . "UseSel" $3}}`)

	distinctCollectNoOuter := makeFunctionRegex("_DISTINCT_COLLECT_PROBE_NO_OUTER", 4)
	s = distinctCollectNoOuter.ReplaceAllString(s, `{{template "distinctCollectProbeNoOuter" buildDict "Global" . "UseSel" $4}}`)

	collectRightOuter := makeFunctionRegex("_COLLECT_PROBE_OUTER", 5)
	s = collectRightOuter.ReplaceAllString(s, `{{template "collectProbeOuter" buildDict "Global" . "UseSel" $5}}`)

	collectNoOuter := makeFunctionRegex("_COLLECT_PROBE_NO_OUTER", 5)
	s = collectNoOuter.ReplaceAllString(s, `{{template "collectProbeNoOuter" buildDict "Global" . "UseSel" $5}}`)

	collectLeftAnti := makeFunctionRegex("_COLLECT_LEFT_ANTI", 5)
	s = collectLeftAnti.ReplaceAllString(s, `{{template "collectLeftAnti" buildDict "Global" . "UseSel" $5}}`)

	tmpl, err := template.New("hashjoiner_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, struct{}{})
}

func init() {
	registerGenerator(genHashJoiner, "hashjoiner.eg.go", hashJoinerTmpl)
}
