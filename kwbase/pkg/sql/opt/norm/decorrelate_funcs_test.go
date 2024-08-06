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

package norm_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestAllAggsIgnoreNullsOrNullOnEmpty verifies the assumption made in
// TranslateNonIgnoreAggs, that every aggregate will be handled somehow by it.
// If that restriction is lifted this test can be deleted.
func TestAllAggsIgnoreNullsOrNullOnEmpty(t *testing.T) {
	for op := range opt.AggregateOpReverseMap {
		if op == opt.CountRowsOp {
			// CountRows is the only aggregate allowed to break this rule.
			continue
		}
		if !opt.AggregateIgnoresNulls(op) && !opt.AggregateIsNullOnEmpty(op) {
			panic(errors.AssertionFailedf(
				"%s does not ignore nulls and is not null on empty", log.Safe(op),
			))
		}
	}
}
