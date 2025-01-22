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

package opt

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util"
)

func BenchmarkColSet(b *testing.B) {
	// Verify that the wrapper doesn't add overhead (as was the case with earlier
	// go versions which couldn't do mid-stack inlining).
	const n = 50
	b.Run("fastintset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c util.FastIntSet
			for j := 0; j < n; j++ {
				c.Add(j)
			}
		}
	})
	b.Run("colset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var c ColSet
			for j := 0; j < n; j++ {
				c.Add(ColumnID(j))
			}
		}
	})
}

func TestTranslateColSet(t *testing.T) {
	test := func(t *testing.T, colSetIn ColSet, from ColList, to ColList, expected ColSet) {
		t.Helper()

		actual := TranslateColSet(colSetIn, from, to)
		if !actual.Equals(expected) {
			t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
		}
	}

	colSetIn, from, to := MakeColSet(1, 2, 3), ColList{1, 2, 3}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(4, 5, 6))

	colSetIn, from, to = MakeColSet(2, 3), ColList{1, 2, 3}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(5, 6))

	// colSetIn and colSetOut might not be the same length.
	colSetIn, from, to = MakeColSet(1, 2), ColList{1, 1, 2}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(4, 5, 6))

	colSetIn, from, to = MakeColSet(1, 2, 3), ColList{1, 2, 3}, ColList{4, 5, 4}
	test(t, colSetIn, from, to, MakeColSet(4, 5))

	colSetIn, from, to = MakeColSet(2), ColList{1, 2, 2}, ColList{4, 5, 6}
	test(t, colSetIn, from, to, MakeColSet(5, 6))
}

func TestColSet(t *testing.T) {
	test1 := MakeColSet(65, 68, 129)
	test3 := MakeColSet(4, 576, 4000)
	if test1.Empty() {
		t.Fatalf("\nexpected: %s\nactual  : %s", "empty", "not empty")
	}

	if test1.Len() != 3 {
		t.Fatalf("\nexpected: %s\nactual  : %v", "3", test1.Len())
	}

	test2 := test1.Copy()

	test2.Remove(68)

	if test2.Contains(69) {
		t.Fatalf("\nexpected: %v\nactual  : %v", 69, test2.String())
	}

	test2 = MakeColSet(129)
	if !test2.SubsetOf(test1) {
		t.Fatalf("\nexpected: %v\nactual  : %v", test1.String(), test2.String())
	}

	if colNext, ok := test2.Next(68); !ok || colNext != 129 {
		t.Fatalf("\nexpected: %v\nactual  : %v", test1.String(), test2.String())
	}

	// test contain 65 68 129, test3 contain 4, 576, 4000
	testUnion := test3.Union(test1)
	if !testUnion.Contains(4) || !testUnion.Contains(68) || !testUnion.Contains(129) || !testUnion.Contains(4000) {
		t.Fatalf("\nexpected: %v\nactual  : %v", test1.String(), testUnion.String())
	}

	test1 = MakeColSet(64, 576, 1280)
	test3 = MakeColSet(4, 576, 4000)
	testIntersection := test3.Intersection(test1)
	if testIntersection.Len() != 1 {
		t.Fatalf("Intersection find Intersection : %v left : %v, right : %v", testIntersection.String(),
			test1.String(), test3.String())
	}

	test1 = MakeColSet(8, 12, 18)
	test3 = MakeColSet(4, 12, 40)
	if test3.Difference(test1).Contains(12) {
		t.Fatalf("Difference find left : %v, right : %v", test1.String(), test3.String())
	}

	test1 = MakeColSet(64, 576, 1280)
	test3 = MakeColSet(4, 576, 4000)
	if test3.Difference(test1).Contains(576) {
		t.Fatalf("Difference find left : %v, right : %v", test1.String(), test3.String())
	}

	test3.DifferenceWith(test1)

	test1 = MakeColSet(64, 576, 1280)
	test2 = MakeColSet(54, 76, 128)
	if test2.Intersects(test1) {
		t.Fatalf("has Intersects find left : %v, right : %v", test1.String(), test2.String())
	}

	// test ForEach
	test1 = MakeColSet(64, 576, 1280)
	test1.Add(5)
	hasLow := false
	test1.ForEach(func(col ColumnID) {
		if col == 5 {
			hasLow = true
		}
	})
	if !hasLow {
		t.Fatalf("has not find 5")
	}

	// test equals
	t1 := MakeColSet(129)
	t2 := MakeColSet(129)
	if !t1.Equals(t2) {
		t.Fatalf("\nexpected: %v\nactual  : %v, not equal", t1.String(), t2.String())
	}
}

func TestColSet1(t *testing.T) {
	var test ColSet
	test.Add(10)
	test.Add(119)
	test.Add(138)
	test.Add(149)
	test.Add(123)
	test.Add(135)
	test.Add(87)
	test.Add(120)
	test.Add(127)
	test.Add(79)
	test.Add(145)
	test.Add(66)
	test.Add(31)
	test.Add(94)
	test.Add(151)
	test.Add(139)
	test.Add(4000)
	if !test.Contains(119) {
		t.Fatalf("\nexpected: %v\n", test.String())
	}
}

func TestColSetUnion(t *testing.T) {
	test1 := MakeColSet(1, 10, 17, 31, 53, 66, 77, 79, 87, 94, 113, 119, 120)
	test2 := MakeColSet(121)
	for i := 122; i <= 187; i++ {
		test2.AddInt(i)
	}
	cols := test1.Union(test2)
	if !cols.Contains(66) {
		t.Fatalf("\nexpected: %v\n", cols.String())
	}

	if test1.Contains(4000) {
		t.Fatalf("\nset : %v contain 4000\n", test1.String())
	}

	test1.Remove(4000)

	test1 = MakeColSet(1, 2, 3)
	test2 = MakeColSet(1000, 2000, 3000)
	if test1.Equals(test2) {
		t.Fatalf("\ntest1 : %v test2 : %v can not equal\n", test1.String(), test2.String())
	}

	if test2.Equals(test1) {
		t.Fatalf("\ntest1 : %v test2 : %v can not equal\n", test1.String(), test2.String())
	}

	test1 = MakeColSet(1, 2000)
	test2 = MakeColSet(1000, 2000, 3000)
	if test1.Equals(test2) {
		t.Fatalf("\ntest1 : %v test2 : %v can not equal\n", test1.String(), test2.String())
	}
}
