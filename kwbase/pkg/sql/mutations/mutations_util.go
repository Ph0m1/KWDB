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

package mutations

import (
	"sync/atomic"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
)

var maxBatchSize int64 = defaultMaxBatchSize

const defaultMaxBatchSize = 10000

// MaxBatchSize returns the max number of entries in the KV batch for a
// mutation operation (delete, insert, update, upsert) - including secondary
// index updates, FK cascading updates, etc - before the current KV batch is
// executed and a new batch is started.
func MaxBatchSize() int {
	return int(atomic.LoadInt64(&maxBatchSize))
}

// SetMaxBatchSizeForTests modifies maxBatchSize variable. It
// should only be used in tests.
func SetMaxBatchSizeForTests(newMaxBatchSize int) {
	atomic.SwapInt64(&maxBatchSize, int64(newMaxBatchSize))
}

// ResetMaxBatchSizeForTests resets the maxBatchSize variable to
// the default mutation batch size. It should only be used in tests.
func ResetMaxBatchSizeForTests() {
	atomic.SwapInt64(&maxBatchSize, defaultMaxBatchSize)
}

// MutationsTestingMaxBatchSize is a testing cluster setting that sets the
// default max mutation batch size. A low max batch size is useful to test
// batching logic of the mutations.
var MutationsTestingMaxBatchSize = settings.RegisterNonNegativeIntSetting(
	"sql.testing.mutations.max_batch_size",
	"the max number of rows that are processed by a single KV batch when performing a mutation operation (0=default)",
	0,
)
