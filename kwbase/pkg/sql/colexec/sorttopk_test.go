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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var topKSortTestCases []sortTestCase

func init() {
	topKSortTestCases = []sortTestCase{
		{
			description: "k < input length",
			tuples:      tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    tuples{{1}, {2}, {3}},
			logTypes:    []types.T{*types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "k > input length",
			tuples:      tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			expected:    tuples{{1}, {2}, {3}, {4}, {5}, {6}, {7}},
			logTypes:    []types.T{*types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           10,
		},
		{
			description: "nulls",
			tuples:      tuples{{1}, {2}, {nil}, {3}, {4}, {5}, {6}, {7}, {nil}},
			expected:    tuples{{nil}, {nil}, {1}},
			logTypes:    []types.T{*types.Int},
			ordCols:     []execinfrapb.Ordering_Column{{ColIdx: 0}},
			k:           3,
		},
		{
			description: "descending",
			tuples:      tuples{{0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5}, {1, 5}},
			expected:    tuples{{0, 5}, {1, 5}, {0, 4}},
			logTypes:    []types.T{*types.Int, *types.Int},
			ordCols: []execinfrapb.Ordering_Column{
				{ColIdx: 1, Direction: execinfrapb.Ordering_Column_DESC},
				{ColIdx: 0, Direction: execinfrapb.Ordering_Column_ASC},
			},
			k: 3,
		},
	}
}

func TestTopKSorter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range topKSortTestCases {
		t.Run(tc.description, func(t *testing.T) {
			runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(input []Operator) (Operator, error) {
				physTypes, err := typeconv.FromColumnTypes(tc.logTypes)
				if err != nil {
					return nil, err
				}
				return NewTopKSorter(testAllocator, input[0], physTypes, tc.ordCols, tc.k), nil
			})
		})
	}
}
