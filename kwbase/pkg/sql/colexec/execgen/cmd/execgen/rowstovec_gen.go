// Copyright 2018 The Cockroach Authors.
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
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"text/template"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// Width is used when a type family has a width that has an associated distinct
// ExecType. One or more of these structs is used as a special case when
// multiple widths need to be associated with one type family in a
// columnConversion struct.
type Width struct {
	Width    int32
	ExecType coltypes.T
}

// columnConversion defines a conversion from a coltypes.ColumnType to an
// exec.ColVec.
type columnConversion struct {
	// Family is the type family of the ColumnType.
	Family string

	// Widths is set if this type family has several widths to special-case. If
	// set, only the ExecType and GoType in the Widths is used.
	Widths []Width

	// ExecType is the exec.T to which we're converting. It should correspond to
	// a method name on exec.ColVec.
	ExecType coltypes.T
}

const rowsToVecTmpl = "pkg/sql/colexec/rowstovec_tmpl.go"

func genRowsToVec(wr io.Writer) error {
	f, err := ioutil.ReadFile(rowsToVecTmpl)
	if err != nil {
		return err
	}

	s := string(f)

	// Replace the template variables.
	s = strings.Replace(s, "_TemplateType", "{{.ExecType.String}}", -1)
	s = strings.Replace(s, "_GOTYPE", "{{.ExecType.GoTypeName}}", -1)
	s = strings.Replace(s, "_FAMILY", "types.{{.Family}}", -1)
	s = strings.Replace(s, "_WIDTH", "{{.Width}}", -1)

	rowsToVecRe := makeFunctionRegex("_ROWS_TO_COL_VEC", 4)
	s = rowsToVecRe.ReplaceAllString(s, `{{ template "rowsToColVec" . }}`)

	s = replaceManipulationFuncs(".ExecType", s)

	// Build the list of supported column conversions.
	conversionsMap := make(map[types.Family]*columnConversion)
	for _, ct := range types.OidToType {
		t := typeconv.FromColumnType(ct)
		if t == coltypes.Unhandled {
			continue
		}

		var conversion *columnConversion
		var ok bool
		if conversion, ok = conversionsMap[ct.Family()]; !ok {
			conversion = &columnConversion{
				Family: ct.Family().String(),
			}
			conversionsMap[ct.Family()] = conversion
		}

		if ct.Width() != 0 {
			conversion.Widths = append(
				conversion.Widths, Width{Width: ct.Width(), ExecType: t},
			)
		} else {
			conversion.ExecType = t
		}
	}

	tmpl, err := template.New("rowsToVec").Parse(s)
	if err != nil {
		return err
	}

	columnConversions := make([]columnConversion, 0, len(conversionsMap))
	for _, conversion := range conversionsMap {
		sort.Slice(conversion.Widths, func(i, j int) bool {
			return conversion.Widths[i].Width < conversion.Widths[j].Width
		})
		columnConversions = append(columnConversions, *conversion)
	}
	// Sort the list so that we output in a consistent order.
	sort.Slice(columnConversions, func(i, j int) bool {
		return strings.Compare(columnConversions[i].Family, columnConversions[j].Family) < 0
	})
	return tmpl.Execute(wr, columnConversions)
}

func init() {
	registerGenerator(genRowsToVec, "rowstovec.eg.go", rowsToVecTmpl)
}
