// Copyright 2020 The Cockroach Authors.
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

type relativeRankTmplInfo struct {
	IsPercentRank bool
	IsCumeDist    bool
	HasPartition  bool
	String        string
}

const relativeRankTmpl = "pkg/sql/colexec/relative_rank_tmpl.go"

func genRelativeRankOps(wr io.Writer) error {
	d, err := ioutil.ReadFile(relativeRankTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	s = strings.Replace(s, "_RELATIVE_RANK_STRING", "{{.String}}", -1)

	computePartitionsSizesRe := makeFunctionRegex("_COMPUTE_PARTITIONS_SIZES", 0)
	s = computePartitionsSizesRe.ReplaceAllString(s, `{{template "computePartitionsSizes"}}`)
	computePeerGroupsSizesRe := makeFunctionRegex("_COMPUTE_PEER_GROUPS_SIZES", 0)
	s = computePeerGroupsSizesRe.ReplaceAllString(s, `{{template "computePeerGroupsSizes"}}`)

	// Now, generate the op, from the template.
	tmpl, err := template.New("relative_rank_op").Parse(s)
	if err != nil {
		return err
	}

	relativeRankTmplInfos := []relativeRankTmplInfo{
		{IsPercentRank: true, HasPartition: false, String: "percentRankNoPartition"},
		{IsPercentRank: true, HasPartition: true, String: "percentRankWithPartition"},
		{IsCumeDist: true, HasPartition: false, String: "cumeDistNoPartition"},
		{IsCumeDist: true, HasPartition: true, String: "cumeDistWithPartition"},
	}
	return tmpl.Execute(wr, relativeRankTmplInfos)
}

func init() {
	registerGenerator(genRelativeRankOps, "relative_rank.eg.go", relativeRankTmpl)
}
