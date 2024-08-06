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

package cli

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestTestServerArgsForTransientCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		nodeID            roachpb.NodeID
		joinAddr          string
		sqlPoolMemorySize int64
		cacheSize         int64

		expected base.TestServerArgs
	}{
		{
			nodeID:            roachpb.NodeID(1),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 2 << 10,
			cacheSize:         1 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:     true,
				JoinAddr:          "127.0.0.1",
				DisableTLSForHTTP: true,
				SQLMemoryPoolSize: 2 << 10,
				CacheSize:         1 << 10,
				Insecure:          true,
			},
		},
		{
			nodeID:            roachpb.NodeID(3),
			joinAddr:          "127.0.0.1",
			sqlPoolMemorySize: 4 << 10,
			cacheSize:         4 << 10,
			expected: base.TestServerArgs{
				PartOfCluster:     true,
				JoinAddr:          "127.0.0.1",
				DisableTLSForHTTP: true,
				SQLMemoryPoolSize: 4 << 10,
				CacheSize:         4 << 10,
				Insecure:          true,
			},
		},
	}

	for _, tc := range testCases {
		demoCtxTemp := demoCtx
		demoCtx.sqlPoolMemorySize = tc.sqlPoolMemorySize
		demoCtx.cacheSize = tc.cacheSize

		actual := testServerArgsForTransientCluster(unixSocketDetails{}, tc.nodeID, tc.joinAddr, "")

		assert.Len(t, actual.StoreSpecs, 1)
		assert.Equal(
			t,
			fmt.Sprintf("demo-node%d", tc.nodeID),
			actual.StoreSpecs[0].StickyInMemoryEngineID,
		)

		// We cannot compare these.
		actual.Stopper = nil
		actual.StoreSpecs = nil

		assert.Equal(t, tc.expected, actual)

		// Restore demoCtx state after each test.
		demoCtx = demoCtxTemp
	}
}
