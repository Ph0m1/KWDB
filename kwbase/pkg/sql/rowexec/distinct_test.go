// Copyright 2016 The Cockroach Authors.
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

func TestDistinct(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}
	vNull := sqlbase.DatumToEncDatum(types.Unknown, tree.DNull)

	testCases := []struct {
		spec     execinfrapb.DistinctSpec
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
		error    string
	}{
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns: []uint32{0, 1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[5], v[6], v[2]},
				{v[2], v[3], v[3]},
				{v[5], v[6], v[4]},
				{v[2], v[6], v[5]},
				{v[3], v[5], v[6]},
				{v[2], v[9], v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[5], v[6], v[2]},
				{v[2], v[6], v[5]},
				{v[3], v[5], v[6]},
				{v[2], v[9], v[7]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{0, 1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[5], v[6], v[7]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[6], v[6], v[7]},
				{v[7], v[6], v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{1},
				DistinctColumns: []uint32{1},
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
				{v[6], v[6], v[7]},
				{v[7], v[6], v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[2], v[6], v[3]},
				{v[2], v[9], v[4]},
				{v[3], v[5], v[5]},
				{v[5], v[6], v[6]},
			},
		},

		// Test NullsAreDistinct flag (not ordered).
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: false,
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: true,
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{vNull, v[2], v[6]},
				{vNull, v[2], v[7]},
				{v[1], vNull, v[8]},
			},
		},

		// Test NullsAreDistinct flag (ordered).
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:   []uint32{0},
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: false,
			},
			input: sqlbase.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
				{v[1], v[2], v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
			},
		},
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:   []uint32{0},
				DistinctColumns:  []uint32{0, 1},
				NullsAreDistinct: true,
			},
			input: sqlbase.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
				{v[1], v[2], v[8]},
			},
			expected: sqlbase.EncDatumRows{
				{vNull, v[2], v[1]},
				{vNull, vNull, v[2]},
				{vNull, v[2], v[3]},
				{vNull, vNull, v[4]},
				{v[1], vNull, v[5]},
				{v[1], v[2], v[6]},
				{v[1], vNull, v[7]},
			},
		},

		// Test ErrorOnDup flag (ordered).
		{
			spec: execinfrapb.DistinctSpec{
				OrderedColumns:  []uint32{0},
				DistinctColumns: []uint32{0, 1},
				ErrorOnDup:      "duplicate rows",
			},
			input: sqlbase.EncDatumRows{
				{v[1], v[2], v[1]},
				{v[2], v[3], v[2]},
				{v[2], v[3], v[3]},
				{v[3], v[4], v[4]},
			},
			error: "duplicate rows",
		},

		// Test ErrorOnDup flag (unordered).
		{
			spec: execinfrapb.DistinctSpec{
				DistinctColumns: []uint32{0, 1},
				ErrorOnDup:      "duplicate rows",
			},
			input: sqlbase.EncDatumRows{
				{v[2], v[3], v[1]},
				{v[1], v[2], v[2]},
				{v[3], v[4], v[3]},
				{v[2], v[3], v[4]},
			},
			error: "duplicate rows",
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ds := c.spec

			in := distsqlutils.NewRowBuffer(sqlbase.ThreeIntCols, c.input, distsqlutils.RowBufferArgs{})
			out := &distsqlutils.RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := execinfra.FlowCtx{
				Cfg:     &execinfra.ServerConfig{Settings: st},
				EvalCtx: &evalCtx,
			}

			d, err := newDistinct(&flowCtx, 0 /* processorID */, &ds, in, &execinfrapb.PostProcessSpec{}, out)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed() {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row, meta := out.Next()
				if meta != nil {
					err = meta.Err
					break
				}
				if row == nil {
					break
				}
				res = append(res, row.Copy())
			}

			if c.error != "" {
				if err == nil || err.Error() != c.error {
					t.Errorf("expected error: %v, got %v", c.error, err)
				}
			} else {
				if result := res.String(sqlbase.ThreeIntCols); result != c.expected.String(sqlbase.ThreeIntCols) {
					t.Errorf("invalid results: %v, expected %v'", result, c.expected.String(sqlbase.ThreeIntCols))
				}
			}
		})
	}
}

func benchmarkDistinct(b *testing.B, orderedColumns []uint32) {
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	spec := &execinfrapb.DistinctSpec{
		DistinctColumns: []uint32{0, 1},
	}
	spec.OrderedColumns = orderedColumns

	post := &execinfrapb.PostProcessSpec{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			input := execinfra.NewRepeatableRowSource(sqlbase.TwoIntCols, sqlbase.MakeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newDistinct(flowCtx, 0 /* processorID */, spec, input, post, &execinfra.RowDisposer{})
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}

func BenchmarkOrderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{0, 1})
}

func BenchmarkPartiallyOrderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{0})
}

func BenchmarkUnorderedDistinct(b *testing.B) {
	benchmarkDistinct(b, []uint32{})
}
