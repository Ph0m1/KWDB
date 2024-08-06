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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testexpr"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func TestLookupJoinProvided(t *testing.T) {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2))",
	); err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}

	testCases := []struct {
		keyCols  opt.ColList
		outCols  opt.ColSet
		required string
		input    string
		provided string
	}{
		// In these tests, the input (left side of the join) has columns 5,6 and the
		// table (right side) has columns 1,2,3,4 and the join has condition
		// (c5, c6) = (c1, c2).
		//
		{ // case 1: the lookup join adds columns 3,4 from the table and retains the
			// input columns.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(3, 4, 5, 6),
			required: "+5,+6",
			input:    "+5,+6",
			provided: "+5,+6",
		},
		{ // case 2: the lookup join produces all columns. The provided ordering
			// on 5,6 is equivalent to an ordering on 1,2.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4, 5, 6),
			required: "-1,+2",
			input:    "-5,+6",
			provided: "-5,+6",
		},
		{ // case 3: the lookup join does not produce input columns 5,6; we must
			// remap the input ordering to refer to output columns 1,2 instead.
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4),
			required: "+1,-2",
			input:    "+5,-6",
			provided: "+1,-2",
		},
		{ // case 4: a hybrid of the two cases above (we need to remap column 6).
			keyCols:  opt.ColList{5, 6},
			outCols:  c(1, 2, 3, 4, 5),
			required: "-1,-2",
			input:    "-5,-6",
			provided: "-5,-2",
		},
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			input := &testexpr.Instance{
				Rel: &props.Relational{},
				Provided: &physical.Provided{
					Ordering: physical.ParseOrdering(tc.input),
				},
			}
			lookupJoin := f.Memo().MemoizeLookupJoin(
				input,
				nil, /* FiltersExpr */
				&memo.LookupJoinPrivate{
					JoinType: opt.InnerJoinOp,
					Table:    tab,
					Index:    cat.PrimaryIndex,
					KeyCols:  tc.keyCols,
					Cols:     tc.outCols,
				},
			)
			req := physical.ParseOrderingChoice(tc.required)
			res := lookupJoinBuildProvided(lookupJoin, &req).String()
			if res != tc.provided {
				t.Errorf("expected '%s', got '%s'", tc.provided, res)
			}
		})
	}
}
