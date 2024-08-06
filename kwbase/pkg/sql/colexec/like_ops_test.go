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
	"fmt"
	"regexp"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestLikeOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		pattern  string
		negate   bool
		tups     tuples
		expected tuples
	}{
		{
			pattern:  "def",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "def",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "de%",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "de%",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "%ef",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "%ef",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "_e_",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "_e_",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "%e%",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "%e%",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
	} {
		runTests(
			t, []tuples{tc.tups}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				ctx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
				return GetLikeOperator(&ctx, input[0], 0, tc.pattern, tc.negate)
			})
	}
}

func BenchmarkLikeOps(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Bytes})
	col := batch.ColVec(0).Bytes()
	width := 64
	for i := 0; i < coldata.BatchSize(); i++ {
		col.Set(i, randutil.RandBytes(rng, width))
	}

	// Set a known prefix and suffix on half the batch so we're not filtering
	// everything out.
	prefix := "abc"
	suffix := "xyz"
	for i := 0; i < coldata.BatchSize()/2; i++ {
		copy(col.Get(i)[:3], prefix)
		copy(col.Get(i)[width-3:], suffix)
	}

	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()

	base := selConstOpBase{
		OneInputNode: NewOneInputNode(source),
		colIdx:       0,
	}
	prefixOp := &selPrefixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(prefix),
	}
	suffixOp := &selSuffixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(suffix),
	}
	pattern := fmt.Sprintf("^%s.*%s$", prefix, suffix)
	regexpOp := &selRegexpBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       regexp.MustCompile(pattern),
	}

	testCases := []struct {
		name string
		op   Operator
	}{
		{name: "selPrefixBytesBytesConstOp", op: prefixOp},
		{name: "selSuffixBytesBytesConstOp", op: suffixOp},
		{name: "selRegexpBytesBytesConstOp", op: regexpOp},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tc.op.Init()
			b.SetBytes(int64(width * coldata.BatchSize()))
			for i := 0; i < b.N; i++ {
				tc.op.Next(ctx)
			}
		})
	}
}
