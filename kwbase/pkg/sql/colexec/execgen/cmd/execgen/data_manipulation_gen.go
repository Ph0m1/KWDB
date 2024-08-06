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
	"fmt"
	"regexp"
	"strings"
)

type dataManipulationReplacementInfo struct {
	re                  *regexp.Regexp
	templatePlaceholder string
	numArgs             int
	replaceWith         string
}

var dataManipulationReplacementInfos = []dataManipulationReplacementInfo{
	{
		templatePlaceholder: "execgen.UNSAFEGET",
		numArgs:             2,
		replaceWith:         "Get",
	},
	{
		templatePlaceholder: "execgen.COPYVAL",
		numArgs:             2,
		replaceWith:         "CopyVal",
	},
	{
		templatePlaceholder: "execgen.SET",
		numArgs:             3,
		replaceWith:         "Set",
	},
	{
		templatePlaceholder: "execgen.SLICE",
		numArgs:             3,
		replaceWith:         "Slice",
	},
	{
		templatePlaceholder: "execgen.COPYSLICE",
		numArgs:             5,
		replaceWith:         "CopySlice",
	},
	{
		templatePlaceholder: "execgen.APPENDSLICE",
		numArgs:             5,
		replaceWith:         "AppendSlice",
	},
	{
		templatePlaceholder: "execgen.APPENDVAL",
		numArgs:             2,
		replaceWith:         "AppendVal",
	},
	{
		templatePlaceholder: "execgen.LEN",
		numArgs:             1,
		replaceWith:         "Len",
	},
	{
		templatePlaceholder: "execgen.RANGE",
		numArgs:             4,
		replaceWith:         "Range",
	},
	{
		templatePlaceholder: "execgen.WINDOW",
		numArgs:             3,
		replaceWith:         "Window",
	},
}

func init() {
	for i, dmri := range dataManipulationReplacementInfos {
		placeHolderArgs := make([]string, dmri.numArgs)
		templResultArgs := make([]string, dmri.numArgs)
		for j := 0; j < dmri.numArgs; j++ {
			placeHolderArgs[j] = `(.*)`
			templResultArgs[j] = fmt.Sprintf("\"$%d\"", j+1)
		}
		dataManipulationReplacementInfos[i].templatePlaceholder += `\(` + strings.Join(placeHolderArgs, ",") + `\)`
		dataManipulationReplacementInfos[i].replaceWith += " " + strings.Join(templResultArgs, " ")
		dataManipulationReplacementInfos[i].re = regexp.MustCompile(dataManipulationReplacementInfos[i].templatePlaceholder)
	}
}

// replaceManipulationFuncs replaces commonly used template placeholders for
// data manipulation. typeIdent is the types.T struct used in the template
// (e.g. "" if using a types.T struct in templates directly or ".Type" if
// stored in the "Type" field of the template struct. body is the template body,
// which is returned with all the replacements. Refer to the init function in
// this file for a list of replacements done.
func replaceManipulationFuncs(typeIdent string, body string) string {
	for _, dmri := range dataManipulationReplacementInfos {
		body = dmri.re.ReplaceAllString(body, fmt.Sprintf("{{ %s.%s }}", typeIdent, dmri.replaceWith))
	}
	return body
}
