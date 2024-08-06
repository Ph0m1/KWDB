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

// This file doesn't live next to execinfra/indexjoiner.go in order to use
// unexported method runProcessorsTest.
// TODO(yuzefovich): move this test file next to indexjoiner.go.

package rowexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestIndexJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create a table where each row is:
	//
	//  |     a    |     b    |         sum         |         s           |
	//  |-----------------------------------------------------------------|
	//  | rowId/10 | rowId%10 | rowId/10 + rowId%10 | IntToEnglish(rowId) |

	aFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row / 10))
	}
	bFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row % 10))
	}
	sumFn := func(row int) tree.Datum {
		return tree.NewDInt(tree.DInt(row/10 + row%10))
	}

	sqlutils.CreateTable(t, sqlDB, "t",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	sqlutils.CreateTable(t, sqlDB, "t2",
		"a INT, b INT, sum INT, s STRING, PRIMARY KEY (a,b), FAMILY f1 (a, b), FAMILY f2 (s), FAMILY f3 (sum), INDEX bs (b,s)",
		99,
		sqlutils.ToRowFn(aFn, bFn, sumFn, sqlutils.RowEnglishFn))

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	tdf := sqlbase.GetTableDescriptor(kvDB, "test", "t2")

	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.IntEncDatum(i)
	}

	testCases := []struct {
		description string
		desc        *sqlbase.TableDescriptor
		post        execinfrapb.PostProcessSpec
		input       sqlbase.EncDatumRows
		outputTypes []types.T
		expected    sqlbase.EncDatumRows
	}{
		{
			description: "Test selecting rows using the primary index",
			desc:        td,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
		{
			description: "Test a filter in the post process spec and using a secondary index",
			desc:        td,
			post: execinfrapb.PostProcessSpec{
				Filter:        execinfrapb.Expression{Expr: "@3 <= 5"}, // sum <= 5
				Projection:    true,
				OutputColumns: []uint32{3},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[1]},
				{v[2], v[5]},
				{v[0], v[5]},
				{v[2], v[1]},
				{v[3], v[4]},
				{v[1], v[3]},
				{v[5], v[1]},
				{v[5], v[0]},
			},
			outputTypes: []types.T{*types.String},
			expected: sqlbase.EncDatumRows{
				{sqlbase.StrEncDatum("one")},
				{sqlbase.StrEncDatum("five")},
				{sqlbase.StrEncDatum("two-one")},
				{sqlbase.StrEncDatum("one-three")},
				{sqlbase.StrEncDatum("five-zero")},
			},
		},
		{
			description: "Test selecting rows using the primary index with multiple family spans",
			desc:        tdf,
			post: execinfrapb.PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1, 2},
			},
			input: sqlbase.EncDatumRows{
				{v[0], v[2]},
				{v[0], v[5]},
				{v[1], v[0]},
				{v[1], v[5]},
			},
			outputTypes: sqlbase.ThreeIntCols,
			expected: sqlbase.EncDatumRows{
				{v[0], v[2], v[2]},
				{v[0], v[5], v[5]},
				{v[1], v[0], v[1]},
				{v[1], v[5], v[6]},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.description, func(t *testing.T) {
			spec := execinfrapb.JoinReaderSpec{
				Table:    *c.desc,
				IndexIdx: 0,
			}
			txn := kv.NewTxn(context.Background(), s.DB(), s.NodeID())
			runProcessorTest(
				t,
				execinfrapb.ProcessorCoreUnion{JoinReader: &spec},
				c.post,
				sqlbase.TwoIntCols,
				c.input,
				c.outputTypes,
				c.expected,
				txn,
			)
		})
	}
}
