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

package execinfra

import "gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the table reader
// disables batch limits in the dist sender. This results in the parallelization
// of these scans.
const ParallelScanResultThreshold = 10000

// ScanShouldLimitBatches returns whether the scan should pace itself.
func ScanShouldLimitBatches(maxResults uint64, limitHint int64, flowCtx *FlowCtx) bool {
	// We don't limit batches if the scan doesn't have a limit, and if the
	// spans scanned will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if we limit batches, distsender
	// does *not* parallelize multi-range scan requests.
	if maxResults != 0 &&
		maxResults < ParallelScanResultThreshold &&
		limitHint == 0 &&
		sqlbase.ParallelScans.Get(&flowCtx.Cfg.Settings.SV) {
		return false
	}
	return true
}
