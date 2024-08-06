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

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func setupMVCCPebble(b testing.TB, dir string) Engine {
	opts := DefaultPebbleOptions()
	opts.FS = vfs.Default
	opts.Cache = pebble.NewCache(testCacheSize)
	defer opts.Cache.Unref()

	peb, err := NewPebble(
		context.Background(),
		PebbleConfig{
			StorageConfig: base.StorageConfig{
				Dir: dir,
			},
			Opts: opts,
		})
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return peb
}

func setupMVCCInMemPebble(b testing.TB, loc string) Engine {
	opts := DefaultPebbleOptions()
	opts.FS = vfs.NewMem()
	opts.Cache = pebble.NewCache(testCacheSize)
	defer opts.Cache.Unref()

	peb, err := NewPebble(
		context.Background(),
		PebbleConfig{
			Opts: opts,
		})
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}

func BenchmarkMVCCScan_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}

	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(ctx, b, setupMVCCPebble, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: false,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCReverseScan_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}

	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(ctx, b, setupMVCCPebble, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: true,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCScanTransactionalData_Pebble(b *testing.B) {
	ctx := context.Background()
	runMVCCScan(ctx, b, setupMVCCPebble, benchScanOptions{
		numRows: 10000,
		benchDataOptions: benchDataOptions{
			numVersions:   2,
			valueBytes:    8,
			transactional: true,
		},
	})
}

func BenchmarkMVCCGet_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, numVersions := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, valueSize := range []int{8} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCGet(ctx, b, setupMVCCPebble, benchDataOptions{
						numVersions: numVersions,
						valueBytes:  valueSize,
					})
				})
			}
		})
	}
}

func BenchmarkMVCCComputeStats_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCComputeStats(ctx, b, setupMVCCPebble, valueSize)
		})
	}
}

func BenchmarkMVCCFindSplitKey_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(ctx, b, setupMVCCPebble, valueSize)
		})
	}
}

func BenchmarkMVCCPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCBlindPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCPutDelete_Pebble(b *testing.B) {
	ctx := context.Background()
	db := setupMVCCInMemPebble(b, "put_delete")
	defer db.Close()

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if err := MVCCPut(ctx, db, nil, key, hlc.Timestamp{}, value, nil /* txn */); err != nil {
			b.Fatal(err)
		}
		if err := MVCCDelete(ctx, db, nil, key, hlc.Timestamp{}, nil /* txn */); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCBatchPut_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range []int{1, 100, 10000, 100000} {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					runMVCCBatchPut(ctx, b, setupMVCCInMemPebble, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(ctx, b, setupMVCCInMemPebble, batchSize)
		})
	}
}

// BenchmarkMVCCGetMergedTimeSeries computes performance of reading merged
// time series data using `MVCCGet()`. Uses an in-memory engine.
func BenchmarkMVCCGetMergedTimeSeries_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, numKeys := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, mergesPerKey := range []int{1, 16, 256} {
				b.Run(fmt.Sprintf("mergesPerKey=%d", mergesPerKey), func(b *testing.B) {
					runMVCCGetMergedValue(ctx, b, setupMVCCInMemPebble, numKeys, mergesPerKey)
				})
			}
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).
//
// TODO(peter): Benchmark{MVCCDeleteRange,ClearRange,ClearIterRange}_Pebble
// give nonsensical results (DeleteRange is absurdly slow and ClearRange
// reports a processing speed of 481 million MB/s!). We need to take a look at
// what these benchmarks are trying to measure, and fix them.

func BenchmarkMVCCDeleteRange_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(ctx, b, setupMVCCPebble, valueSize)
		})
	}
}

func BenchmarkClearRange_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	runClearRange(ctx, b, setupMVCCPebble, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearRange(start, end)
	})
}

func BenchmarkClearIterRange_Pebble(b *testing.B) {
	ctx := context.Background()
	runClearRange(ctx, b, setupMVCCPebble, func(eng Engine, batch Batch, start, end MVCCKey) error {
		iter := eng.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		return batch.ClearIterRange(iter, start.Key, end.Key)
	})
}

func BenchmarkMVCCGarbageCollect_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}

	// NB: To debug #16068, test only 128-128-15000-6.
	ctx := context.Background()
	for _, keySize := range []int{128} {
		b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
			for _, valSize := range []int{128} {
				b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
					for _, numKeys := range []int{1, 1024} {
						b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
							for _, numVersions := range []int{2, 1024} {
								b.Run(fmt.Sprintf("numVersions=%d", numVersions), func(b *testing.B) {
									runMVCCGarbageCollect(ctx, b, setupMVCCInMemPebble, benchGarbageCollectOptions{
										benchDataOptions: benchDataOptions{
											numKeys:     numKeys,
											numVersions: numVersions,
											valueBytes:  valSize,
										},
										keyBytes:       keySize,
										deleteVersions: numVersions - 1,
									})
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkBatchApplyBatchRepr_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			for _, sequential := range []bool{false, true} {
				b.Run(fmt.Sprintf("seq=%t", sequential), func(b *testing.B) {
					for _, valueSize := range []int{10} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, batchSize := range []int{10000} {
								b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
									runBatchApplyBatchRepr(ctx, b, setupMVCCInMemPebble,
										indexed, sequential, valueSize, batchSize)
								})
							}
						})
					}
				})
			}
		})
	}
}
