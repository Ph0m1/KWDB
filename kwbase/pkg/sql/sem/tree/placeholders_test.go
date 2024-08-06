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

package tree

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestPlaceholderTypesEquals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		a, b  PlaceholderTypes
		equal bool
	}{
		{ // 0
			PlaceholderTypes{},
			PlaceholderTypes{},
			true,
		},
		{ // 1
			PlaceholderTypes{types.Int, types.Int},
			PlaceholderTypes{types.Int, types.Int},
			true,
		},
		{ // 2
			PlaceholderTypes{types.Int},
			PlaceholderTypes{types.Int, types.Int},
			false,
		},
		{ // 3
			PlaceholderTypes{types.Int, nil},
			PlaceholderTypes{types.Int, types.Int},
			false,
		},
		{ // 4
			PlaceholderTypes{types.Int, types.Int},
			PlaceholderTypes{types.Int, nil},
			false,
		},
		{ // 5
			PlaceholderTypes{types.Int, nil},
			PlaceholderTypes{types.Int, nil},
			true,
		},
		{ // 6
			PlaceholderTypes{types.Int},
			PlaceholderTypes{types.Int, nil},
			false,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			res := tc.a.Equals(tc.b)
			if res != tc.equal {
				t.Errorf("%v vs %v: expected %t, got %t", tc.a, tc.b, tc.equal, res)
			}
			res2 := tc.b.Equals(tc.a)
			if res != res2 {
				t.Errorf("%v vs %v: not commutative", tc.a, tc.b)
			}
		})
	}
}
