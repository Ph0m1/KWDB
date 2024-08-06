// Copyright 2020 The Cockroach Authors.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func TestDistinctOnProvided(t *testing.T) {
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, testcat.New())
	md := f.Metadata()
	for i := 1; i <= 5; i++ {
		md.AddColumn(fmt.Sprintf("c%d", i), types.Int)
	}
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	fd1eq5 := props.FuncDepSet{}
	fd1eq5.AddEquivalency(1, 5)

	// DistinctOn might not project all input columns, so we have three sets of
	// columns, corresponding to this SQL:
	//   SELECT <outCols> FROM
	//     SELECT DISTINCT ON(<groupingCols>) <inputCols>
	testCases := []struct {
		inCols       opt.ColSet
		inFDs        props.FuncDepSet
		outCols      opt.ColSet
		groupingCols opt.ColSet
		required     string
		internal     string
		input        string
		expected     string
	}{
		{ // case 1: Internal ordering is stronger; the provided ordering needs
			//         trimming.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        props.FuncDepSet{},
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+1",
			internal:     "+1,+5",
			input:        "+1,+5",
			expected:     "+1",
		},
		{ // case 2: Projecting all input columns; ok to pass through provided.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3, 4, 5),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+5",
		},
		{ // case 3: Not projecting all input columns; the provided ordering
			//         needs remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "",
			input:        "+5",
			expected:     "+1",
		},
		{ // case 4: The provided ordering needs both trimming and remapping.
			inCols:       c(1, 2, 3, 4, 5),
			inFDs:        fd1eq5,
			outCols:      c(1, 2, 3),
			groupingCols: c(1, 2),
			required:     "+(1|5)",
			internal:     "+5,+4",
			input:        "+5,+4",
			expected:     "+1",
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{
					OutputCols: tc.outCols,
					FuncDeps:   fd1eq5,
				},
				Provided: &physical.Provided{
					Ordering: physical.ParseOrdering(tc.input),
				},
			}
			p := memo.GroupingPrivate{
				GroupingCols: tc.groupingCols,
				Ordering:     physical.ParseOrderingChoice(tc.internal),
			}
			var aggs memo.AggregationsExpr
			tc.outCols.Difference(tc.groupingCols).ForEach(func(col opt.ColumnID) {
				aggs = append(aggs, f.ConstructAggregationsItem(
					f.ConstructFirstAgg(f.ConstructVariable(col)),
					col,
				))
			})
			distinctOn := f.Memo().MemoizeDistinctOn(input, aggs, &p)
			req := physical.ParseOrderingChoice(tc.required)
			res := distinctOnBuildProvided(distinctOn, &req).String()
			if res != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, res)
			}
		})
	}
}
