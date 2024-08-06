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

package rowexec

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestOrdinality(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		spec     execinfrapb.OrdinalitySpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			input: sqlbase.EncDatumRows{
				{v[2]},
				{v[5]},
				{v[2]},
				{v[5]},
				{v[2]},
				{v[3]},
				{v[2]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
		},
		{
			input: sqlbase.EncDatumRows{
				{},
				{},
				{},
				{},
				{},
				{},
				{},
			},
			expected: sqlbase.EncDatumRows{
				{v[1]},
				{v[2]},
				{v[3]},
				{v[4]},
				{v[5]},
				{v[6]},
				{v[7]},
			},
		},
		{
			input: sqlbase.EncDatumRows{
				{v[2], v[1]},
				{v[5], v[2]},
				{v[2], v[3]},
				{v[5], v[4]},
				{v[2], v[5]},
				{v[3], v[6]},
				{v[2], v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[1], v[1]},
				{v[5], v[2], v[2]},
				{v[2], v[3], v[3]},
				{v[5], v[4], v[4]},
				{v[2], v[5], v[5]},
				{v[3], v[6], v[6]},
				{v[2], v[7], v[7]},
			},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			os := c.spec

			in := distsqlutils.NewRowBuffer(sqlbase.TwoIntCols, c.input, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			d, err := newOrdinalityProcessor(&flowCtx, 0 /* processorID */, &os, in, &execinfrapb.PostProcessSpec{}, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			var typs []types.T
			switch len(res[0]) {
			case 1:
				typs = sqlbase.OneIntCol
			case 2:
				typs = sqlbase.TwoIntCols
			case 3:
				typs = sqlbase.ThreeIntCols
			}
			if result := res.String(typs); result != c.expected.String(typs) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(sqlbase.TwoIntCols))
			}
		})
	}
}

func BenchmarkOrdinality(b *testing.B) {
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	spec := &execinfrapb.OrdinalitySpec{}

	post := &execinfrapb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		input := execinfra.NewRepeatableRowSource(sqlbase.TwoIntCols, sqlbase.MakeIntRows(numRows, numCols))
		b.SetBytes(int64(8 * numRows * numCols))
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				o, err := newOrdinalityProcessor(flowCtx, 0 /* processorID */, spec, input, post, &execinfra.RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				o.Run(ctx)
				input.Reset()
			}
		})
	}
}
