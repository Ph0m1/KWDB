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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestProject(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())
	md := f.Metadata()
	for i := 1; i <= 4; i++ {
		md.AddColumn(fmt.Sprintf("col%d", i), types.Int)
	}

	var fds props.FuncDepSet
	fds.AddEquivalency(2, 3)
	fds.AddConstants(opt.MakeColSet(4))

	input := &testexpr.Instance{
		Rel: &props.Relational{
			OutputCols: opt.MakeColSet(1, 2, 3, 4),
			FuncDeps:   fds,
		},
	}

	type testCase struct {
		req string
		exp string
	}
	testCases := []testCase{
		{
			req: "",
			exp: "",
		},
		{
			req: "+1,+2,+3,+4",
			exp: "+1,+(2|3) opt(4)",
		},
		{
			req: "+3 opt(5)",
			exp: "+(2|3) opt(4)",
		},
		{
			req: "+1 opt(2,5)",
			exp: "+1 opt(2-4)",
		},
		{
			req: "+1,+2,+3,+4,+5",
			exp: "no",
		},
		{
			req: "+5",
			exp: "no",
		},
	}
	for _, tc := range testCases {
		req := physical.ParseOrderingChoice(tc.req)
		project := f.Memo().MemoizeProject(input, nil /* projections */, opt.MakeColSet(1, 2, 3, 4))

		res := "no"
		if projectCanProvideOrdering(project, &req) {
			res = projectBuildChildReqOrdering(project, &req, 0).String()
		}
		if res != tc.exp {
			t.Errorf("req: %s  expected: %s  got: %s\n", tc.req, tc.exp, res)
		}
	}
}
