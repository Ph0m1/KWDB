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

package colexec

import (
	"context"
	"testing"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestColumnarizeMaterialize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(jordan,asubiotto): add randomness to this test as more types are supported.
	typs := []types.T{*types.Int, *types.Int}
	nRows := 10000
	nCols := 2
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := execinfra.NewRepeatableRowSource(typs, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewMaterializer(
		flowCtx,
		1, /* processorID */
		c,
		typs,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(ctx)

	for i := 0; i < nRows; i++ {
		row, meta := m.Next()
		if meta != nil {
			t.Fatalf("unexpected meta %+v", meta)
		}
		if row == nil {
			t.Fatal("unexpected nil row")
		}
		for j := 0; j < nCols; j++ {
			if row[j].Datum.Compare(&evalCtx, rows[i][j].Datum) != 0 {
				t.Fatal("unequal rows", row, rows[i])
			}
		}
	}
	row, meta := m.Next()
	if meta != nil {
		t.Fatalf("unexpected meta %+v", meta)
	}
	if row != nil {
		t.Fatal("unexpected not nil row", row)
	}
}

func TestMaterializeTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(andyk): Make sure to add more types here. Consider iterating over
	// types.OidToTypes list and also using randomly generated EncDatums.
	types := []types.T{
		*types.Bool,
		*types.Int,
		*types.Float,
		*types.Decimal,
		*types.Date,
		*types.String,
		*types.Bytes,
		*types.Name,
		*types.Oid,
	}
	inputRow := sqlbase.EncDatumRow{
		sqlbase.EncDatum{Datum: tree.DBoolTrue},
		sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(31))},
		sqlbase.EncDatum{Datum: tree.NewDFloat(37.41)},
		sqlbase.EncDatum{Datum: &tree.DDecimal{Decimal: *apd.New(43, 47)}},
		sqlbase.EncDatum{Datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(53))},
		sqlbase.EncDatum{Datum: tree.NewDString("hello")},
		sqlbase.EncDatum{Datum: tree.NewDBytes("ciao")},
		sqlbase.EncDatum{Datum: tree.NewDName("aloha")},
		sqlbase.EncDatum{Datum: tree.NewDOid(59)},
	}
	input := execinfra.NewRepeatableRowSource(types, sqlbase.EncDatumRows{inputRow})

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	outputToInputColIdx := make([]int, len(types))
	for i := range outputToInputColIdx {
		outputToInputColIdx[i] = i
	}
	m, err := NewMaterializer(
		flowCtx,
		1, /* processorID */
		c,
		types,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(ctx)

	row, meta := m.Next()
	if meta != nil {
		t.Fatalf("unexpected meta %+v", meta)
	}
	if row == nil {
		t.Fatal("unexpected nil row")
	}
	for i := range inputRow {
		inDatum := inputRow[i].Datum
		outDatum := row[i].Datum
		if inDatum.Compare(&evalCtx, outDatum) != 0 {
			t.Fatal("unequal datums", inDatum, outDatum)
		}
	}
}

func TestMaterializerNextErrorAfterConsumerDone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testError := errors.New("test-induced error")
	metadataSource := &execinfrapb.CallbackMetadataSource{DrainMetaCb: func(_ context.Context) []execinfrapb.ProducerMetadata {
		execerror.VectorizedInternalPanic(testError)
		// Unreachable
		return nil
	}}
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
	}

	m, err := NewMaterializer(
		flowCtx,
		0, /* processorID */
		&CallbackOperator{},
		nil,                            /* typ */
		&execinfrapb.PostProcessSpec{}, /* post */
		nil,                            /* output */
		[]execinfrapb.MetadataSource{metadataSource},
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	require.NoError(t, err)

	m.Start(ctx)
	// We expect ConsumerDone to panic since it immediately moves the materializer
	// into the trailing meta state which will produce the panic. The key is that
	// the materializer is moved into the draining state first.
	testutils.IsError(
		execerror.CatchVectorizedRuntimeError(func() {
			// Call ConsumerDone.
			m.ConsumerDone()
		}),
		testError.Error(),
	)
	// We expect Next to panic since DrainMeta panics are currently not caught by
	// the materializer and it's not clear whether they should be since
	// implementers of DrainMeta do not return errors as panics.
	testutils.IsError(
		execerror.CatchVectorizedRuntimeError(func() {
			m.Next()
		}),
		testError.Error(),
	)
}

func BenchmarkColumnarizeMaterialize(b *testing.B) {
	types := []types.T{*types.Int, *types.Int}
	nRows := 10000
	nCols := 2
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := execinfra.NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))
	for i := 0; i < b.N; i++ {
		m, err := NewMaterializer(
			flowCtx,
			1, /* processorID */
			c,
			types,
			&execinfrapb.PostProcessSpec{},
			nil, /* output */
			nil, /* metadataSourcesQueue */
			nil, /* toClose */
			nil, /* outputStatsToTrace */
			nil, /* cancelFlow */
		)
		if err != nil {
			b.Fatal(err)
		}
		m.Start(ctx)

		foundRows := 0
		for {
			row, meta := m.Next()
			if meta != nil {
				b.Fatalf("unexpected metadata %v", meta)
			}
			if row == nil {
				break
			}
			foundRows++
		}
		if foundRows != nRows {
			b.Fatalf("expected %d rows, found %d", nRows, foundRows)
		}
		input.Reset()
	}
}
