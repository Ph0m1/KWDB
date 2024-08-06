// Copyright 2016 The Cockroach Authors.
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

package zerofields

import "testing"

func TestNoZeroField(t *testing.T) {
	type foo struct {
		A int
		B int
	}
	type bar struct {
		X, Y int
		Z    foo
	}
	testFooNonZero := bar{1, 2, foo{3, 4}}
	testFoo := testFooNonZero
	if err := NoZeroField(&testFoo); err != nil {
		t.Fatal(err)
	}
	if err := NoZeroField(interface{}(testFoo)); err != nil {
		t.Fatal(err)
	}
	testFoo = testFooNonZero
	testFoo.Y = 0
	if err, exp := NoZeroField(&testFoo), (zeroFieldErr{"Y"}); err != exp {
		t.Fatalf("expected error %v, found %v", exp, err)
	}
	testFoo = testFooNonZero
	testFoo.Z.B = 0
	if err, exp := NoZeroField(&testFoo), (zeroFieldErr{"Z.B"}); err != exp {
		t.Fatalf("expected error %v, found %v", exp, err)
	}
}
