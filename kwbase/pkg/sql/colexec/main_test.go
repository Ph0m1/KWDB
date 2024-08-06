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
	"fmt"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

var (
	// testAllocator is an Allocator with an unlimited budget for use in tests.
	testAllocator *Allocator

	// testMemMonitor and testMemAcc are a test monitor with an unlimited budget
	// and a memory account bound to it for use in tests.
	testMemMonitor *mon.BytesMonitor
	testMemAcc     *mon.BoundAccount

	// testDiskMonitor and testDiskmAcc are a test monitor with an unlimited budget
	// and a disk account bound to it for use in tests.
	testDiskMonitor *mon.BytesMonitor
	testDiskAcc     *mon.BoundAccount
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	os.Exit(func() int {
		ctx := context.Background()
		testMemMonitor = execinfra.NewTestMemMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testMemMonitor.Stop(ctx)
		memAcc := testMemMonitor.MakeBoundAccount()
		testMemAcc = &memAcc
		testAllocator = NewAllocator(ctx, testMemAcc)
		defer testMemAcc.Close(ctx)

		testDiskMonitor = execinfra.NewTestDiskMonitor(ctx, cluster.MakeTestingClusterSettings())
		defer testDiskMonitor.Stop(ctx)
		diskAcc := testDiskMonitor.MakeBoundAccount()
		testDiskAcc = &diskAcc
		defer testDiskAcc.Close(ctx)

		// Pick a random batch size in [minBatchSize, coldata.MaxBatchSize]
		// range. The randomization can be disabled using KWBASE_RANDOMIZE_BATCH_SIZE=false.
		randomBatchSize := generateBatchSize()
		fmt.Printf("coldata.BatchSize() is set to %d\n", randomBatchSize)
		if err := coldata.SetBatchSizeForTests(randomBatchSize); err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return m.Run()
	}())
}

// minBatchSize is the minimum acceptable size of batches for tests in this
// package.
const minBatchSize = 3

func generateBatchSize() int {
	randomizeBatchSize := envutil.EnvOrDefaultBool("KWBASE_RANDOMIZE_BATCH_SIZE", true)
	if randomizeBatchSize {
		rng, _ := randutil.NewPseudoRand()
		batchSize := minBatchSize +
			rng.Intn(coldata.MaxBatchSize-minBatchSize)
		return batchSize
	}
	return coldata.BatchSize()
}
