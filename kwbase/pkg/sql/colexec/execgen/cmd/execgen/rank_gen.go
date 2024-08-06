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
	"io/ioutil"
	"strings"
	"text/template"
)

type rankTmplInfo struct {
	Dense        bool
	HasPartition bool
	String       string
}

// UpdateRank is used to encompass the different logic between DENSE_RANK and
// RANK window functions when updating the internal state of rank operators. It
// is used by the template.
func (r rankTmplInfo) UpdateRank() string {
	if r.Dense {
		return `r.rank++`
	}
	return fmt.Sprintf(
		`r.rank += r.rankIncrement
r.rankIncrement = 1`,
	)
}

// UpdateRankIncrement is used to encompass the different logic between
// DENSE_RANK and RANK window functions when updating the internal state of
// rank operators. It is used by the template.
func (r rankTmplInfo) UpdateRankIncrement() string {
	if r.Dense {
		return ``
	}
	return `r.rankIncrement++`
}

// Avoid unused warnings. These methods are used in the template.
var (
	_ = rankTmplInfo{}.UpdateRank()
	_ = rankTmplInfo{}.UpdateRankIncrement()
)

const rankTmpl = "pkg/sql/colexec/rank_tmpl.go"

func genRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(rankTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_RANK_STRING", "{{.String}}", -1)

	updateRankRe := makeFunctionRegex("_UPDATE_RANK", 0)
	s = updateRankRe.ReplaceAllString(s, makeTemplateFunctionCall("UpdateRank", 0))
	updateRankIncrementRe := makeFunctionRegex("_UPDATE_RANK_INCREMENT", 0)
	s = updateRankIncrementRe.ReplaceAllString(s, makeTemplateFunctionCall("UpdateRankIncrement", 0))

	// Now, generate the op, from the template.
	tmpl, err := template.New("rank_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	rankTmplInfos := []rankTmplInfo{
		{Dense: false, HasPartition: false, String: "rankNoPartition"},
		{Dense: false, HasPartition: true, String: "rankWithPartition"},
		{Dense: true, HasPartition: false, String: "denseRankNoPartition"},
		{Dense: true, HasPartition: true, String: "denseRankWithPartition"},
	}
	return tmpl.Execute(wr, rankTmplInfos)
}

func init() {
	registerGenerator(genRankOps, "rank.eg.go", rankTmpl)
}
