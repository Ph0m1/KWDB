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

import (
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
)

// DefaultJoinOrderLimit denotes the default limit on the number of joins to
// reorder.
const DefaultJoinOrderLimit = 4

// DefaultMMJoinOrderLimit denotes the default limit on the number of joins to
// reorder in multi-model processing.
const DefaultMMJoinOrderLimit = 8

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

// AutoLimitQuantity is quantity of autolimit
var AutoLimitQuantity = settings.RegisterPublicIntSetting(
	"sql.auto_limit.quantity", "quantity of autolimit", 0)

// PushdownAll is true when we want to push down the whole query to specific engines
var PushdownAll = settings.RegisterPublicBoolSetting(
	"sql.all_push_down.enabled", "push down entire query", true,
)

// TSParallelDegree ts engine parallel exec degree
var TSParallelDegree = settings.RegisterPublicIntSetting(
	"ts.parallel_degree",
	"degree of parallelism in ts",
	0)

// TSHashScanMode ts engine hash tag scan mode enforcement
var TSHashScanMode = settings.RegisterPublicIntSetting(
	"ts.hash_scan_mode",
	"hash scan mode enforcement in ts",
	0)

// TSQueryOptMode is a cluster setting that controls each optimization switch.
// The value of cluster setting ts.sql.query_opt_mode represents
// the level of the four optimization items, and its value is a four-digit int value,
// each indicating the optimization switch at the corresponding position.
// - 1 indicating on
// - 0 indicating off
// The four optimization items are, in order from left to right:
// -- 1. Multi-predicate sequential optimization
// -- 2. Scalar quantum query optimization
// -- 3. inside-out push down aggregation optimization
// -- 4. inside-out push down time_bucket optimization
// -- 5. reduce explore cross join
//
// For example:
// If you want to turn on
// "multi-predicate sequential optimization" and
// "inside-out push down aggregation optimization",
// you need to set this cluster setting to 1010
// such as:
// "set cluster setting ts.sql.query_opt_mode = 1010"
//
// default value: 1110
// turn on the first three optimizations.
var TSQueryOptMode = settings.RegisterPublicIntSetting(
	"ts.sql.query_opt_mode", "ts query optimize mode", DefaultQueryOptMode,
)

// DefaultQueryOptMode is the default value of TSQueryOptMode
const DefaultQueryOptMode = 1110

// CheckOptMode checks whether the query opt mode is enabled.
//
// input params:
// csValue:  The value of cluster setting ts.sql.query_opt_mode
// mode:     To check whether the optimization mode is turned on
func CheckOptMode(csValue int64, mode int) bool {
	binaryValue := strconv.Itoa(int(csValue))
	v, err := strconv.ParseInt(binaryValue, 2, 64)
	if err != nil {
		return false
	}
	return int(v)&mode > 0
}

const (
	// JoinPushTimeBucket indicates join push down time_bucket
	JoinPushTimeBucket = 1 << 0

	// JoinPushAgg indicates join push down agg
	JoinPushAgg = 1 << 1

	// PushScalarSubQuery indicates push down ScalarSubQuery
	PushScalarSubQuery = 1 << 2

	// FilterOptOrder indicates that the order of filtering conditions will be optimized
	FilterOptOrder = 1 << 3

	// ReduceCrossJoinExplore use for avoid exploring invalid cross join.
	ReduceCrossJoinExplore = 1 << 4
)

// TSOrderedTable ts get ordered table data
var TSOrderedTable = settings.RegisterPublicBoolSetting(
	"ts.ordered_table.enabled",
	"get ordered table in ts",
	false)

// memo ts flags
const (
	// IncludeTSTable for marking query includes ts table
	IncludeTSTable = 1 << 0

	// NeedTSTypeCheck is set to true when we need to check in ts mode.
	NeedTSTypeCheck = 1 << 1

	// SingleMode start by single node
	SingleMode = 1 << 2

	// OrderGroupBy is set when sort is before group by.
	OrderGroupBy = 1 << 3

	// HasGapFill is set when use time_bucket_gapfill function in SQL.
	HasGapFill = 1 << 4

	// DiffUseOrderScan is set when diff Function exec in AE.
	DiffUseOrderScan = 1 << 5

	// HasMuiltDiff is set when has multi diff Function in SQL.
	HasMuiltDiff = 1 << 6

	// HasAutoLimit is set when the limit is autoLimit
	HasAutoLimit = 1 << 7

	// FinishOptInsideOut is set when the optimization of inside-out is done.
	FinishOptInsideOut = 1 << 8

	// ScalarSubQueryPush is set when the switch of push-scalar-subQuery is turned on
	ScalarSubQueryPush = 1 << 9
)

// OrderedTableType TSScanOrderedType
type OrderedTableType uint8

// TSScanOrderedType
const (
	NoOrdered OrderedTableType = iota
	// OrderedScan represents use ordered scan iterator
	OrderedScan
	// SortAfterScan represents sort table after scan
	SortAfterScan
	// ForceOrderedScan represents use ordered scan iterator
	ForceOrderedScan
)

// Ordered is ordered type
func (v OrderedTableType) Ordered() bool {
	return v != NoOrdered
}

// UserOrderedScan return use ordered scan
func (v OrderedTableType) UserOrderedScan() bool {
	return v == OrderedScan || v == ForceOrderedScan
}

// NeedReverse return need reverse
func (v OrderedTableType) NeedReverse() bool {
	return v == OrderedScan
}

// GroupOptType represents Group expr opt type
type GroupOptType uint64

// GroupOptType
const (
	// TimeBucketPushAgg represents push local agg to scan compute
	TimeBucketPushAgg = 1 << 0
	// PruneLocalAgg represents prune local agg
	PruneLocalAgg = 1 << 1
	// PruneFinalAgg represents prune final agg
	PruneFinalAgg = 1 << 2
	// ScalarGroupByWithSumInt is set when scalarGroupBy with sum_Int agg in inside_out case,
	// it must return 0 when the table is empty, because sum_int is the twice agg of count.
	ScalarGroupByWithSumInt = 1 << 3
	// UseStatistic represents push local agg to scan and use statistic scan
	UseStatistic = 1 << 4
)

// TimeBucketOpt return true if has TimeBucketPushAgg opt
func (v GroupOptType) TimeBucketOpt() bool {
	return v&TimeBucketPushAgg > 0
}

// PushLocalAggToScanOpt return true if has TimeBucketPushAgg opt
func (v GroupOptType) PushLocalAggToScanOpt() bool {
	return v&TimeBucketPushAgg > 0 || v&UseStatistic > 0
}

// PruneLocalAggOpt return true if has PruneLocalAgg opt
func (v GroupOptType) PruneLocalAggOpt() bool {
	return v&PruneLocalAgg > 0
}

// PruneFinalAggOpt return true if has PruneFinalAgg opt
func (v GroupOptType) PruneFinalAggOpt() bool {
	return v&PruneFinalAgg > 0
}

// WithSumInt return true if scalarGroupBy with sum_Int agg in inside_out case.
func (v GroupOptType) WithSumInt() bool {
	return v&ScalarGroupByWithSumInt > 0
}

// UseStatisticOpt return true that has UseStatistic opt
func (v GroupOptType) UseStatisticOpt() bool {
	return v&UseStatistic > 0
}

// LimitOptType represents Limit expr opt type
type LimitOptType uint64

// LimitOptType
const (
	// TSPushLimitToAggScan represents push limit to aggScan in time series
	TSPushLimitToAggScan = 1 << 0
)

// TSPushLimitToAggScan return true that has TSPushLimitToAggScan
func (l LimitOptType) TSPushLimitToAggScan() bool {
	return l&TSPushLimitToAggScan > 0
}
