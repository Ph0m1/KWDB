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

package opt

import "gitee.com/kwbasedb/kwbase/pkg/settings"

// DefaultJoinOrderLimit denotes the default limit on the number of joins to
// reorder.
const DefaultJoinOrderLimit = 4

// SaveTablesDatabase is the name of the database where tables created by
// the saveTableNode are stored.
const SaveTablesDatabase = "savetables"

const (
	// Placeholder0 holder
	Placeholder0 = 1 << 0

	// Placeholder1 holder
	Placeholder1 = 1 << 1

	// Placeholder2 holder
	Placeholder2 = 1 << 2

	// HasLast is set when query includes last
	HasLast = 1 << 3

	// IsPrepare is set when it's a prepare statement.
	IsPrepare = 1 << 4

	// HasFirst is set when query includes last
	HasFirst = 1 << 5
)

// PushdownAll is true when we want to push down the whole query to specific engines
var PushdownAll = settings.RegisterPublicBoolSetting(
	"sql.all_push_down.enabled", "push down entire query", true,
)

// TSParallelDegree ts engine parallel exec degree
var TSParallelDegree = settings.RegisterPublicIntSetting(
	"ts.parallel_degree",
	"degree of parallelism in ts",
	0)

// memo ts flags
const (
	// IncludeTSTable for marking query includes ts table
	IncludeTSTable = 1 << 0

	// NeedTSTypeCheck is set to true when we need to check in ts mode.
	NeedTSTypeCheck = 1 << 1

	// SingleMode start by single node
	SingleMode = 1 << 2

	// ExecInTSEngine  is set when all processors can exec in ts engine.
	ExecInTSEngine = 1 << 3

	// OrderGroupBy is set when sort is before group by.
	OrderGroupBy = 1 << 4

	// HasGapFill is set when use time_bucket_gapfill function in SQL.
	HasGapFill = 1 << 5

	// HasSubquery is set when use subquery in SQL.
	HasSubquery = 1 << 6
)
