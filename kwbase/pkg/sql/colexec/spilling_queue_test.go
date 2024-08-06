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

package colexec

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/colcontainerutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/humanizeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestSpillingQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rng, _ := randutil.NewPseudoRand()
	for _, rewindable := range []bool{false, true} {
		for _, memoryLimit := range []int64{10 << 10 /* 10 KiB */, 1<<20 + int64(rng.Intn(64<<20)) /* 1 MiB up to 64 MiB */} {
			alwaysCompress := rng.Float64() < 0.5
			diskQueueCacheMode := colcontainer.DiskQueueCacheModeDefault
			// testReuseCache will test the reuse cache modes.
			testReuseCache := rng.Float64() < 0.5
			dequeuedProbabilityBeforeAllEnqueuesAreDone := 0.5
			if testReuseCache {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				if rng.Float64() < 0.5 {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeReuseCache
				} else {
					diskQueueCacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
				}
			}
			prefix := ""
			if rewindable {
				dequeuedProbabilityBeforeAllEnqueuesAreDone = 0
				prefix = "Rewindable/"
			}
			numBatches := 1 + rng.Intn(1024)
			t.Run(fmt.Sprintf("%sMemoryLimit=%s/DiskQueueCacheMode=%d/AlwaysCompress=%t/NumBatches=%d",
				prefix, humanizeutil.IBytes(memoryLimit), diskQueueCacheMode, alwaysCompress, numBatches), func(t *testing.T) {
				// Create random input.
				batches := make([]coldata.Batch, 0, numBatches)
				op := NewRandomDataOp(testAllocator, rng, RandomDataOpArgs{
					NumBatches: cap(batches),
					BatchSize:  1 + rng.Intn(coldata.BatchSize()),
					Nulls:      true,
					BatchAccumulator: func(b coldata.Batch) {
						batches = append(batches, CopyBatch(testAllocator, b))
					},
				})
				typs := op.Typs()

				queueCfg.CacheMode = diskQueueCacheMode
				queueCfg.SetDefaultBufferSizeBytesForCacheMode()
				queueCfg.TestingKnobs.AlwaysCompress = alwaysCompress

				// Create queue.
				var q *spillingQueue
				if rewindable {
					q = newRewindableSpillingQueue(
						testAllocator, typs, memoryLimit, queueCfg,
						NewTestingSemaphore(2), coldata.BatchSize(),
						testDiskAcc,
					)
				} else {
					q = newSpillingQueue(
						testAllocator, typs, memoryLimit, queueCfg,
						NewTestingSemaphore(2), coldata.BatchSize(),
						testDiskAcc,
					)
				}

				// Run verification.
				var (
					b   coldata.Batch
					err error
				)
				ctx := context.Background()
				for {
					b = op.Next(ctx)
					require.NoError(t, q.enqueue(ctx, b))
					if b.Length() == 0 {
						break
					}
					if rng.Float64() < dequeuedProbabilityBeforeAllEnqueuesAreDone {
						if b, err = q.dequeue(ctx); err != nil {
							t.Fatal(err)
						} else if b.Length() == 0 {
							t.Fatal("queue incorrectly considered empty")
						}
						coldata.AssertEquivalentBatches(t, batches[0], b)
						batches = batches[1:]
					}
				}
				numReadIterations := 1
				if rewindable {
					numReadIterations = 2
				}
				for i := 0; i < numReadIterations; i++ {
					batchIdx := 0
					for batches[batchIdx].Length() > 0 {
						if b, err = q.dequeue(ctx); err != nil {
							t.Fatal(err)
						} else if b == nil {
							t.Fatal("unexpectedly dequeued nil batch")
						} else if b.Length() == 0 {
							t.Fatal("queue incorrectly considered empty")
						}
						coldata.AssertEquivalentBatches(t, batches[batchIdx], b)
						batchIdx++
					}

					if b, err := q.dequeue(ctx); err != nil {
						t.Fatal(err)
					} else if b.Length() != 0 {
						t.Fatal("queue should be empty")
					}

					if rewindable {
						require.NoError(t, q.rewind())
					}
				}

				// Close queue.
				require.NoError(t, q.close(ctx))

				// Verify no directories are left over.
				directories, err := queueCfg.FS.ListDir(queueCfg.Path)
				require.NoError(t, err)
				require.Equal(t, 0, len(directories))
			})
		}
	}
}
