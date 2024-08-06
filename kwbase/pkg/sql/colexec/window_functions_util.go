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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/execerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
)

const columnOmitted = -1

// SupportedWindowFns contains all window functions supported by the
// vectorized engine.
var SupportedWindowFns = map[execinfrapb.WindowerSpec_WindowFunc]struct{}{
	execinfrapb.WindowerSpec_ROW_NUMBER:   {},
	execinfrapb.WindowerSpec_RANK:         {},
	execinfrapb.WindowerSpec_DENSE_RANK:   {},
	execinfrapb.WindowerSpec_PERCENT_RANK: {},
	execinfrapb.WindowerSpec_CUME_DIST:    {},
}

// windowFnNeedsPeersInfo returns whether a window function pays attention to
// the concept of "peers" during its computation ("peers" are tuples within the
// same partition - from PARTITION BY clause - that are not distinct on the
// columns in ORDER BY clause). For most window functions, the result of
// computation should be the same for "peers", so most window functions do need
// this information.
func windowFnNeedsPeersInfo(windowFn execinfrapb.WindowerSpec_WindowFunc) bool {
	switch windowFn {
	case execinfrapb.WindowerSpec_ROW_NUMBER:
		// row_number doesn't pay attention to the concept of "peers."
		return false
	case
		execinfrapb.WindowerSpec_RANK,
		execinfrapb.WindowerSpec_DENSE_RANK,
		execinfrapb.WindowerSpec_PERCENT_RANK,
		execinfrapb.WindowerSpec_CUME_DIST:
		return true
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("window function %s is not supported", windowFn.String()))
		// This code is unreachable, but the compiler cannot infer that.
		return false
	}
}
