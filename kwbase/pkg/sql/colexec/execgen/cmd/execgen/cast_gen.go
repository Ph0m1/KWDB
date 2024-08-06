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

const castTmpl = "pkg/sql/colexec/cast_tmpl.go"

func genCastOperators(wr io.Writer) error {
	t, err := ioutil.ReadFile(castTmpl)
	if err != nil {
		return err
	}

	s := string(t)

	assignCast := makeFunctionRegex("_ASSIGN_CAST", 2)
	s = assignCast.ReplaceAllString(s, makeTemplateFunctionCall("Assign", 2))
	s = strings.Replace(s, "_ALLTYPES", "{{$typ}}", -1)
	s = strings.Replace(s, "_FROMTYPE", "{{.FromTyp}}", -1)
	s = strings.Replace(s, "_TOTYPE", "{{.ToTyp}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.ToGoTyp}}", -1)

	// replace _FROM_TYPE_SLICE's with execgen.SLICE's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_SLICE", "execgen.SLICE", -1)
	// replace the _FROM_TYPE_UNSAFEGET's with execgen.UNSAFEGET's of the correct type.
	s = strings.Replace(s, "_FROM_TYPE_UNSAFEGET", "execgen.UNSAFEGET", -1)
	s = replaceManipulationFuncs(".FromTyp", s)

	// replace the _TO_TYPE_SET's with execgen.SET's of the correct type
	s = strings.Replace(s, "_TO_TYPE_SET", "execgen.SET", -1)
	s = replaceManipulationFuncs(".ToTyp", s)

	isCastFuncSet := func(ov castOverload) bool {
		return ov.AssignFunc != nil
	}

	tmpl, err := template.New("cast").Funcs(template.FuncMap{"isCastFuncSet": isCastFuncSet}).Parse(s)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, castOverloads)
}

func init() {
	registerGenerator(genCastOperators, "cast.eg.go", castTmpl)
}
