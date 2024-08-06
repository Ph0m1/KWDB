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

package norm

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestCommuteJoinFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := [][2]memo.JoinFlags{
		{0, 0},

		{memo.AllowLookupJoinIntoLeft, memo.AllowLookupJoinIntoRight},

		{
			memo.AllowLookupJoinIntoLeft | memo.AllowLookupJoinIntoRight,
			memo.AllowLookupJoinIntoLeft | memo.AllowLookupJoinIntoRight,
		},

		{memo.AllowHashJoinStoreLeft, memo.AllowHashJoinStoreRight},

		{
			memo.AllowHashJoinStoreLeft | memo.AllowHashJoinStoreRight,
			memo.AllowHashJoinStoreLeft | memo.AllowHashJoinStoreRight,
		},

		{
			memo.AllowMergeJoin | memo.AllowHashJoinStoreLeft | memo.AllowLookupJoinIntoRight,
			memo.AllowMergeJoin | memo.AllowHashJoinStoreRight | memo.AllowLookupJoinIntoLeft,
		},
	}

	var funcs CustomFuncs
	for _, tc := range cases {
		// The result of commuting flags should be symmetrical, so test each case in
		// both directions.
		for dir := 0; dir <= 1; dir++ {
			in, out := tc[dir], tc[dir^1]
			res := funcs.CommuteJoinFlags(&memo.JoinPrivate{Flags: in})
			if res.Flags != out {
				t.Errorf("input: '%s'  expected: '%s'  got: '%s'", in, out, res.Flags)
			}
		}
	}
}
