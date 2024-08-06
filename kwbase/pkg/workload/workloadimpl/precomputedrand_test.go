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

package workloadimpl_test

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/workload/workloadimpl"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

func TestPrecomputedRand(t *testing.T) {
	const precomputeLen = 100

	seed0 := workloadimpl.PrecomputedRandInit(rand.NewSource(0), precomputeLen, alphabet)()
	seed1 := workloadimpl.PrecomputedRandInit(rand.NewSource(1), precomputeLen, alphabet)()
	numbers := workloadimpl.PrecomputedRandInit(rand.NewSource(0), precomputeLen, `0123456789`)()

	const shorterThanPrecomputed, longerThanPrecomputed = precomputeLen / 10, precomputeLen + 7

	offset := 0
	fillBytes := func(pr workloadimpl.PrecomputedRand, length int) []byte {
		buf := make([]byte, length)
		offset = pr.FillBytes(offset, buf)
		return buf
	}

	short0 := fillBytes(seed0, shorterThanPrecomputed)
	long0 := fillBytes(seed0, longerThanPrecomputed)

	// Offset has advanced, we should get a different result.
	short0Different := fillBytes(seed0, shorterThanPrecomputed)
	require.NotEqual(t, short0, short0Different)

	// Reset the offset and verify that the results are repeatable
	offset = 0
	short0B := fillBytes(seed0, shorterThanPrecomputed)
	long0B := fillBytes(seed0, longerThanPrecomputed)
	require.Equal(t, short0, short0B)
	require.Equal(t, long0, long0B)

	// Reset the offset and verify that a different seed gets different results.
	offset = 0
	short1 := fillBytes(seed1, shorterThanPrecomputed)
	long1 := fillBytes(seed1, longerThanPrecomputed)
	require.NotEqual(t, short0, short1)
	require.NotEqual(t, long0, long1)

	// Reset the offset and verify that a different alphabet gets different
	// results.
	offset = 0
	shortNumbers := fillBytes(numbers, shorterThanPrecomputed)
	longNumbers := fillBytes(numbers, longerThanPrecomputed)
	require.NotEqual(t, short0, shortNumbers)
	require.NotEqual(t, long0, longNumbers)
}

func BenchmarkPrecomputedRand(b *testing.B) {
	const precomputeLen = 10000
	pr := workloadimpl.PrecomputedRandInit(
		rand.NewSource(uint64(timeutil.Now().UnixNano())), precomputeLen, alphabet)()

	const shortLen, mediumLen, longLen = 2, 100, 100000
	scratch := make([]byte, longLen)
	var randOffset int

	for _, l := range []int{shortLen, mediumLen, longLen} {
		b.Run(fmt.Sprintf(`len=%d`, l), func(b *testing.B) {
			randOffset = 0
			buf := scratch[:l]
			for i := 0; i < b.N; i++ {
				randOffset = pr.FillBytes(randOffset, buf)
			}
			b.SetBytes(int64(len(buf)))
		})
	}
}
