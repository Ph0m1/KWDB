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

package colflow_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestVectorizedInternalPanic verifies that materializers successfully
// handle panics coming from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic doesn't occur yet the error is propagated.
func TestVectorizedInternalPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	types := sqlbase.OneIntCol
	input := execinfra.NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := colexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	vee := newTestVectorizedInternalPanicEmitter(col)
	mat, err := colexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		vee,
		types,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	mat.Start(ctx)

	var meta *execinfrapb.ProducerMetadata
	require.NotPanics(t, func() { _, meta = mat.Next() }, "VectorizedInternalPanic was not caught")
	require.NotNil(t, meta.Err, "VectorizedInternalPanic was not propagated as metadata")
}

// TestNonVectorizedPanicPropagation verifies that materializers do not handle
// panics coming not from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic is emitted all the way through the chain.
func TestNonVectorizedPanicPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	types := sqlbase.OneIntCol
	input := execinfra.NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := colexec.NewColumnarizer(ctx, testAllocator, &flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	nvee := newTestNonVectorizedPanicEmitter(col)
	mat, err := colexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		nvee,
		types,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	mat.Start(ctx)

	require.Panics(t, func() { mat.Next() }, "NonVectorizedPanic was caught by the operators")
}

// testVectorizedInternalPanicEmitter is an colexec.Operator that panics with
// execerror.VectorizedInternalPanic on every odd-numbered invocation of Next()
// and returns the next batch from the input on every even-numbered (i.e. it
// becomes a noop for those iterations). Used for tests only.
type testVectorizedInternalPanicEmitter struct {
	colexec.OneInputNode
	emitBatch bool
}

var _ colexec.Operator = &testVectorizedInternalPanicEmitter{}

func newTestVectorizedInternalPanicEmitter(input colexec.Operator) colexec.Operator {
	return &testVectorizedInternalPanicEmitter{
		OneInputNode: colexec.NewOneInputNode(input),
	}
}

// Init is part of exec.Operator interface.
func (e *testVectorizedInternalPanicEmitter) Init() {
	e.Input().Init()
}

// Next is part of exec.Operator interface.
func (e *testVectorizedInternalPanicEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		execerror.VectorizedInternalPanic("")
	}

	e.emitBatch = false
	return e.Input().Next(ctx)
}

// testNonVectorizedPanicEmitter is the same as
// testVectorizedInternalPanicEmitter but it panics with the builtin panic
// function. Used for tests only. It is the only colexec.Operator panics from
// which are not caught.
type testNonVectorizedPanicEmitter struct {
	colexec.OneInputNode
	emitBatch bool
}

var _ colexec.Operator = &testVectorizedInternalPanicEmitter{}

func newTestNonVectorizedPanicEmitter(input colexec.Operator) colexec.Operator {
	return &testNonVectorizedPanicEmitter{
		OneInputNode: colexec.NewOneInputNode(input),
	}
}

// Init is part of exec.Operator interface.
func (e *testNonVectorizedPanicEmitter) Init() {
	e.Input().Init()
}

// Next is part of exec.Operator interface.
func (e *testNonVectorizedPanicEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic("")
	}

	e.emitBatch = false
	return e.Input().Next(ctx)
}
