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

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestColumnarizerResetsInternalBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs := []types.T{*types.Int}
	// There will be at least two batches of rows so that we can see whether the
	// internal batch is reset.
	nRows := coldata.BatchSize() * 2
	nCols := len(typs)
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
	c.Init()
	foundRows := 0
	for {
		batch := c.Next(ctx)
		require.Nil(t, batch.Selection(), "Columnarizer didn't reset the internal batch")
		if batch.Length() == 0 {
			break
		}
		foundRows += batch.Length()
		// The "meat" of the test - we're updating the batch that the Columnarizer
		// owns.
		batch.SetSelection(true)
	}
	require.Equal(t, nRows, foundRows)
}

func TestColumnarizerDrainsAndClosesInput(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	rb := distsqlutils.NewRowBuffer([]types.T{*types.Int}, nil /* rows */, distsqlutils.RowBufferArgs{})
	flowCtx := &execinfra.FlowCtx{EvalCtx: &evalCtx}

	const errMsg = "artificial error"
	rb.Push(nil, &execinfrapb.ProducerMetadata{Err: errors.New(errMsg)})
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0 /* processorID */, rb)
	require.NoError(t, err)

	c.Init()

	// Calling DrainMeta from the vectorized execution engine should propagate to
	// non-vectorized components as calling ConsumerDone and then draining their
	// metadata.
	metaCh := make(chan []execinfrapb.ProducerMetadata)
	go func() {
		metaCh <- c.DrainMeta(ctx)
	}()

	// Make Next race with DrainMeta, this should be supported by the
	// Columnarizer. If the metadata is obtained through this Next call, the
	// Columnarizer still returns it in DrainMeta.
	_ = c.Next(ctx)

	meta := <-metaCh
	require.True(t, len(meta) == 1)
	require.True(t, testutils.IsError(meta[0].Err, errMsg))
	require.True(t, rb.Done)
}

func BenchmarkColumnarize(b *testing.B) {
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

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))

	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}
	c.Init()
	for i := 0; i < b.N; i++ {
		foundRows := 0
		for {
			batch := c.Next(ctx)
			if batch.Length() == 0 {
				break
			}
			foundRows += batch.Length()
		}
		if foundRows != nRows {
			b.Fatalf("found %d rows, expected %d", foundRows, nRows)
		}
		input.Reset()
	}
}
