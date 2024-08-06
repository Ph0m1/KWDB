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

package sqlbase

import "gitee.com/kwbasedb/kwbase/pkg/settings"

const (
	// ClusterSettingReadOnly key of cluster setting 'default_transaction_read_only.enabled'
	ClusterSettingReadOnly = "default_transaction_read_only.enabled"
)

// ReadOnlyInternal Internal read-only mode restriction parameter, used for internal code use
// Set the current node to read-only mode internally in the code
// ReadOnlyInternal = true
// Only allow users to perform read operations on the current node, prohibit all user write operations,
// including DDL, DCL, and prohibit other cluster setting settings
var ReadOnlyInternal bool

// ReadOnly wraps "default_transaction_read_only.enabled",
// Set the current node to read-only mode
// ReadOnly = true
// Only allow users to perform read operations on the current node, prohibit all user write operations,
// including DDL, DCL, and prohibit other cluster setting settings
var ReadOnly = settings.RegisterPublicBoolSetting(
	ClusterSettingReadOnly,
	"the read-only mode,"+
		"false: default can read and write"+
		"true: can only read can not write include DDL、DCL and other cluster setting",
	false,
)

// ParallelScans controls parallelizing multi-range scans when the maximum size
// of the result set is known.
var ParallelScans = settings.RegisterBoolSetting(
	"sql.parallel_scans.enabled",
	"parallelizes scanning different ranges when the maximum result size can be deduced",
	true,
)
