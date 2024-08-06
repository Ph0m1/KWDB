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

package colexec

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestParallelUnorderedSynchronizer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		maxInputs  = 16
		maxBatches = 16
	)

	var (
		rng, _     = randutil.NewPseudoRand()
		typs       = []coltypes.T{coltypes.Int64}
		numInputs  = rng.Intn(maxInputs) + 1
		numBatches = rng.Intn(maxBatches) + 1
	)

	inputs := make([]Operator, numInputs)
	for i := range inputs {
		source := NewRepeatableBatchSource(
			testAllocator,
			RandomBatch(testAllocator, rng, typs, coldata.BatchSize(), 0 /* length */, rng.Float64()),
		)
		source.ResetBatchesToReturn(numBatches)
		inputs[i] = source
	}

	var wg sync.WaitGroup
	s := NewParallelUnorderedSynchronizer(inputs, typs, &wg)

	ctx, cancelFn := context.WithCancel(context.Background())
	var cancel bool
	if rng.Float64() < 0.5 {
		cancel = true
	}
	if cancel {
		wg.Add(1)
		sleepTime := time.Duration(rng.Intn(500)) * time.Microsecond
		go func() {
			time.Sleep(sleepTime)
			cancelFn()
			wg.Done()
		}()
	} else {
		// Appease the linter complaining about context leaks.
		defer cancelFn()
	}

	batchesReturned := 0
	for {
		var b coldata.Batch
		if err := execerror.CatchVectorizedRuntimeError(func() { b = s.Next(ctx) }); err != nil {
			if cancel {
				require.True(t, testutils.IsError(err, "context canceled"), err)
				break
			} else {
				t.Fatal(err)
			}
		}
		if b.Length() == 0 {
			break
		}
		batchesReturned++
	}
	if !cancel {
		require.Equal(t, numInputs*numBatches, batchesReturned)
	}
	wg.Wait()
}

func TestUnorderedSynchronizerNoLeaksOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const expectedErr = "first input error"

	inputs := make([]Operator, 6)
	inputs[0] = &CallbackOperator{NextCb: func(context.Context) coldata.Batch {
		execerror.VectorizedInternalPanic(expectedErr)
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}}
	for i := 1; i < len(inputs); i++ {
		inputs[i] = &CallbackOperator{
			NextCb: func(ctx context.Context) coldata.Batch {
				<-ctx.Done()
				execerror.VectorizedInternalPanic(ctx.Err())
				// This code is unreachable, but the compiler cannot infer that.
				return nil
			},
		}
	}

	var (
		ctx = context.Background()
		wg  sync.WaitGroup
	)
	s := NewParallelUnorderedSynchronizer(inputs, []coltypes.T{coltypes.Int64}, &wg)
	err := execerror.CatchVectorizedRuntimeError(func() { _ = s.Next(ctx) })
	// This is the crux of the test: assert that all inputs have finished.
	require.Equal(t, len(inputs), int(atomic.LoadUint32(&s.numFinishedInputs)))
	require.True(t, testutils.IsError(err, expectedErr), err)
}

func BenchmarkParallelUnorderedSynchronizer(b *testing.B) {
	const numInputs = 6

	typs := []coltypes.T{coltypes.Int64}
	inputs := make([]Operator, numInputs)
	for i := range inputs {
		batch := testAllocator.NewMemBatchWithSize(typs, coldata.BatchSize())
		batch.SetLength(coldata.BatchSize())
		inputs[i] = NewRepeatableBatchSource(testAllocator, batch)
	}
	var wg sync.WaitGroup
	ctx, cancelFn := context.WithCancel(context.Background())
	s := NewParallelUnorderedSynchronizer(inputs, typs, &wg)
	b.SetBytes(8 * int64(coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Next(ctx)
	}
	b.StopTimer()
	cancelFn()
	wg.Wait()
}
