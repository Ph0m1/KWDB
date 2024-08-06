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

package memo_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
)

func TestCostLess(t *testing.T) {
	testCases := []struct {
		left, right memo.Cost
		expected    bool
	}{
		{0.0, 1.0, true},
		{0.0, 1e-20, true},
		{0.0, 0.0, false},
		{1.0, 0.0, false},
		{1e-20, 1.0000000000001e-20, false},
		{1e-20, 1.000001e-20, true},
		{1, 1.00000000000001, false},
		{1, 1.00000001, true},
		{1000, 1000.00000000001, false},
		{1000, 1000.00001, true},
	}
	for _, tc := range testCases {
		if tc.left.Less(tc.right) != tc.expected {
			t.Errorf("expected %v.Less(%v) to be %v", tc.left, tc.right, tc.expected)
		}
	}
}

func TestCostSub(t *testing.T) {
	testSub := func(left, right memo.Cost, expected memo.Cost) {
		actual := left.Sub(right)
		if actual != expected {
			t.Errorf("expected %v.Sub(%v) to be %v, got %v", left, right, expected, actual)
		}
	}

	testSub(memo.Cost(10.0), memo.Cost(3.0), memo.Cost(7.0))
	testSub(memo.Cost(3.0), memo.Cost(10.0), memo.Cost(-7.0))
	testSub(memo.Cost(10.0), memo.Cost(10.0), memo.Cost(0.0))
}
