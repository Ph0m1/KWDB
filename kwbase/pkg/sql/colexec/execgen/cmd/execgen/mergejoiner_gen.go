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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// mjOverload contains the overloads for all needed comparisons.
type mjOverload struct {
	// The embedded overload has the shared type information for both of the
	// overloads, so that you can reference that information inside of . without
	// needing to pick Eq or Lt.
	overload
	Eq *overload
	Lt *overload
}

// selPermutation contains information about which permutation of selection
// vector state the template is materializing.
type selPermutation struct {
	IsLSel bool
	IsRSel bool

	LSelString string
	RSelString string
}

type joinTypeInfo struct {
	IsInner      bool
	IsLeftOuter  bool
	IsRightOuter bool
	IsLeftSemi   bool
	IsLeftAnti   bool

	String string
}

const mergeJoinerTmpl = "pkg/sql/colexec/mergejoiner_tmpl.go"

func genMergeJoinOps(wr io.Writer, jti joinTypeInfo) error {
	d, err := ioutil.ReadFile(mergeJoinerTmpl)
	if err != nil {
		return err
	}

	s := string(d)

	// Replace the template variables.
	s = strings.Replace(s, "_GOTYPESLICE", "{{.LTyp.GoTypeSliceName}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.LTyp.GoTypeName}}", -1)
	s = strings.Replace(s, "_TYPES_T", "coltypes.{{.LTyp}}", -1)
	s = strings.Replace(s, "_TemplateType", "{{.LTyp}}", -1)
	s = strings.Replace(s, "_L_SEL_IND", "{{$sel.LSelString}}", -1)
	s = strings.Replace(s, "_R_SEL_IND", "{{$sel.RSelString}}", -1)
	s = strings.Replace(s, "_IS_L_SEL", "{{$sel.IsLSel}}", -1)
	s = strings.Replace(s, "_IS_R_SEL", "{{$sel.IsRSel}}", -1)
	s = strings.Replace(s, "_SEL_ARG", "$sel", -1)
	s = strings.Replace(s, "_JOIN_TYPE_STRING", "{{$.JoinType.String}}", -1)
	s = strings.Replace(s, "_JOIN_TYPE", "$.JoinType", -1)
	s = strings.Replace(s, "_MJ_OVERLOAD", "$mjOverload", -1)
	s = strings.Replace(s, "_L_HAS_NULLS", "$.lHasNulls", -1)
	s = strings.Replace(s, "_R_HAS_NULLS", "$.rHasNulls", -1)
	s = strings.Replace(s, "_HAS_NULLS", "$.HasNulls", -1)
	s = strings.Replace(s, "_HAS_SELECTION", "$.HasSelection", -1)
	s = strings.Replace(s, "_SEL_PERMUTATION", "$.SelPermutation", -1)

	leftUnmatchedGroupSwitch := makeFunctionRegex("_LEFT_UNMATCHED_GROUP_SWITCH", 1)
	s = leftUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "leftUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	rightUnmatchedGroupSwitch := makeFunctionRegex("_RIGHT_UNMATCHED_GROUP_SWITCH", 1)
	s = rightUnmatchedGroupSwitch.ReplaceAllString(s, `{{template "rightUnmatchedGroupSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromLeftSwitch := makeFunctionRegex("_NULL_FROM_LEFT_SWITCH", 1)
	s = nullFromLeftSwitch.ReplaceAllString(s, `{{template "nullFromLeftSwitch" buildDict "Global" $ "JoinType" $1}}`)

	nullFromRightSwitch := makeFunctionRegex("_NULL_FROM_RIGHT_SWITCH", 1)
	s = nullFromRightSwitch.ReplaceAllString(s, `{{template "nullFromRightSwitch" buildDict "Global" $ "JoinType" $1}}`)

	incrementLeftSwitch := makeFunctionRegex("_INCREMENT_LEFT_SWITCH", 4)
	s = incrementLeftSwitch.ReplaceAllString(s, `{{template "incrementLeftSwitch" buildDict "Global" $ "LTyp" .LTyp "JoinType" $1 "SelPermutation" $2 "MJOverload" $3 "lHasNulls" $4}}`)

	incrementRightSwitch := makeFunctionRegex("_INCREMENT_RIGHT_SWITCH", 4)
	s = incrementRightSwitch.ReplaceAllString(s, `{{template "incrementRightSwitch" buildDict "Global" $ "LTyp" .LTyp "JoinType" $1 "SelPermutation" $2 "MJOverload" $3 "rHasNulls" $4}}`)

	processNotLastGroupInColumnSwitch := makeFunctionRegex("_PROCESS_NOT_LAST_GROUP_IN_COLUMN_SWITCH", 1)
	s = processNotLastGroupInColumnSwitch.ReplaceAllString(s, `{{template "processNotLastGroupInColumnSwitch" buildDict "Global" $ "JoinType" $1}}`)

	probeSwitch := makeFunctionRegex("_PROBE_SWITCH", 4)
	s = probeSwitch.ReplaceAllString(s, `{{template "probeSwitch" buildDict "Global" $ "JoinType" $1 "SelPermutation" $2 "lHasNulls" $3 "rHasNulls" $4}}`)

	sourceFinishedSwitch := makeFunctionRegex("_SOURCE_FINISHED_SWITCH", 1)
	s = sourceFinishedSwitch.ReplaceAllString(s, `{{template "sourceFinishedSwitch" buildDict "Global" $ "JoinType" $1}}`)

	leftSwitch := makeFunctionRegex("_LEFT_SWITCH", 3)
	s = leftSwitch.ReplaceAllString(s, `{{template "leftSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2 "HasNulls" $3 }}`)

	rightSwitch := makeFunctionRegex("_RIGHT_SWITCH", 3)
	s = rightSwitch.ReplaceAllString(s, `{{template "rightSwitch" buildDict "Global" $ "JoinType" $1 "HasSelection" $2  "HasNulls" $3 }}`)

	assignEqRe := makeFunctionRegex("_ASSIGN_EQ", 3)
	s = assignEqRe.ReplaceAllString(s, makeTemplateFunctionCall("Eq.Assign", 3))

	assignLtRe := makeFunctionRegex("_ASSIGN_LT", 3)
	s = assignLtRe.ReplaceAllString(s, makeTemplateFunctionCall("Lt.Assign", 3))

	s = replaceManipulationFuncs(".LTyp", s)

	// Now, generate the op, from the template.
	tmpl, err := template.New("mergejoin_op").Funcs(template.FuncMap{"buildDict": buildDict}).Parse(s)
	if err != nil {
		return err
	}

	allOverloads := intersectOverloads(sameTypeComparisonOpToOverloads[tree.EQ], sameTypeComparisonOpToOverloads[tree.LT])

	// Create an mjOverload for each overload combining three overloads so that
	// the template code can access all of EQ, LT, and GT in the same range loop.
	mjOverloads := make([]mjOverload, len(allOverloads[0]))
	for i := range allOverloads[0] {
		mjOverloads[i] = mjOverload{
			overload: *allOverloads[0][i],
			Eq:       allOverloads[0][i],
			Lt:       allOverloads[1][i],
		}
	}

	// Create each permutation of selection vector state.
	selPermutations := []selPermutation{
		{
			IsLSel:     true,
			IsRSel:     true,
			LSelString: "lSel[curLIdx]",
			RSelString: "rSel[curRIdx]",
		},
		{
			IsLSel:     true,
			IsRSel:     false,
			LSelString: "lSel[curLIdx]",
			RSelString: "curRIdx",
		},
		{
			IsLSel:     false,
			IsRSel:     true,
			LSelString: "curLIdx",
			RSelString: "rSel[curRIdx]",
		},
		{
			IsLSel:     false,
			IsRSel:     false,
			LSelString: "curLIdx",
			RSelString: "curRIdx",
		},
	}

	return tmpl.Execute(wr, struct {
		MJOverloads     interface{}
		SelPermutations interface{}
		JoinType        interface{}
	}{
		MJOverloads:     mjOverloads,
		SelPermutations: selPermutations,
		JoinType:        jti,
	})
}

func init() {
	joinTypeInfos := []joinTypeInfo{
		{
			IsInner: true,
			String:  "Inner",
		},
		{
			IsLeftOuter: true,
			String:      "LeftOuter",
		},
		{
			IsRightOuter: true,
			String:       "RightOuter",
		},
		{
			IsLeftOuter:  true,
			IsRightOuter: true,
			String:       "FullOuter",
		},
		{
			IsLeftSemi: true,
			String:     "LeftSemi",
		},
		{
			IsLeftAnti: true,
			String:     "LeftAnti",
		},
	}

	mergeJoinGenerator := func(jti joinTypeInfo) generator {
		return func(wr io.Writer) error {
			return genMergeJoinOps(wr, jti)
		}
	}

	for _, join := range joinTypeInfos {
		registerGenerator(mergeJoinGenerator(join), fmt.Sprintf("mergejoiner_%s.eg.go", strings.ToLower(join.String)), mergeJoinerTmpl)
	}
}
