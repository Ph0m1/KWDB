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
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestHashFunctionFamily verifies the assumption that our vectorized hashing
// function (the combination of initHash, rehash, and finalizeHash) actually
// defines a function family and that changing the initial hash value is
// sufficient to get a "different" hash function.
func TestHashFunctionFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	bucketsA, bucketsB := make([]uint64, coldata.BatchSize()), make([]uint64, coldata.BatchSize())
	nKeys := coldata.BatchSize()
	keyTypes := []coltypes.T{coltypes.Int64}
	keys := []coldata.Vec{testAllocator.NewMemColumn(keyTypes[0], coldata.BatchSize())}
	for i := int64(0); i < int64(coldata.BatchSize()); i++ {
		keys[0].Int64()[i] = i
	}
	numBuckets := uint64(16)
	var (
		cancelChecker  CancelChecker
		decimalScratch decimalOverloadScratch
	)

	for initHashValue, buckets := range [][]uint64{bucketsA, bucketsB} {
		// We need +1 here because 0 is not a valid initial hash value.
		initHash(buckets, nKeys, uint64(initHashValue+1))
		for i, typ := range keyTypes {
			rehash(ctx, buckets, typ, keys[i], nKeys, nil /* sel */, cancelChecker, decimalScratch)
		}
		finalizeHash(buckets, nKeys, numBuckets)
	}

	numKeysInSameBucket := 0
	for key := range bucketsA {
		if bucketsA[key] == bucketsB[key] {
			numKeysInSameBucket++
		}
	}
	// We expect that about 1/numBuckets keys remained in the same bucket, so if
	// the actual number deviates by more than a factor of 3, we fail the test.
	if nKeys*3/int(numBuckets) < numKeysInSameBucket {
		t.Fatal(fmt.Sprintf("too many keys remained in the same bucket: expected about %d, actual %d",
			nKeys/int(numBuckets), numKeysInSameBucket))
	}
}
