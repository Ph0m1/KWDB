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

package ordering

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func TestOrdinalityProvided(t *testing.T) {
	emptyFD, equivFD, constFD := testFDs()
	// The ordinality column is 10.
	testCases := []struct {
		required string
		input    string
		fds      props.FuncDepSet
		provided string
	}{
		{ // case 1
			required: "+10",
			input:    "+1,+2",
			fds:      emptyFD,
			provided: "+10",
		},
		{ // case 2
			required: "+1,+10",
			input:    "+1,+2,+3",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 3
			required: "+1,+10,+5",
			input:    "+1,+2",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 4
			required: "+(1|2),+(3|10)",
			input:    "+1,+4,+5",
			fds:      emptyFD,
			provided: "+1,+10",
		},
		{ // case 5
			required: "+1",
			input:    "",
			fds:      constFD,
			provided: "",
		},
		{ // case 6
			required: "-1,+10",
			input:    "-2",
			fds:      equivFD,
			provided: "-2,+10",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			evalCtx := tree.NewTestingEvalContext(nil /* st */)
			var f norm.Factory
			f.Init(evalCtx, nil /* catalog */)
			input := &testexpr.Instance{
				Rel: &props.Relational{OutputCols: opt.MakeColSet(1, 2, 3, 4, 5)},
				Provided: &physical.Provided{
					Ordering: physical.ParseOrdering(tc.input),
				},
			}
			r := f.Memo().MemoizeOrdinality(input, &memo.OrdinalityPrivate{ColID: 10})
			r.Relational().FuncDeps = tc.fds
			req := physical.ParseOrderingChoice(tc.required)
			res := ordinalityBuildProvided(r, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}
