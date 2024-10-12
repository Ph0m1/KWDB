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

package sqltelemetry

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
)

// CteUseCounter is to be incremented every time a CTE (WITH ...)
// is planned without error in a query (this includes both recursive and
// non-recursive CTEs).
var CteUseCounter = telemetry.GetCounterOnce("sql.plan.cte")

// RecursiveCteUseCounter is to be incremented every time a recursive CTE (WITH
// RECURSIVE...) is planned without error in a query.
var RecursiveCteUseCounter = telemetry.GetCounterOnce("sql.plan.cte.recursive")

// SubqueryUseCounter is to be incremented every time a subquery is
// planned.
var SubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery")

// CorrelatedSubqueryUseCounter is to be incremented every time a
// correlated subquery has been processed during planning.
var CorrelatedSubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery.correlated")

// ForeignKeyChecksUseCounter is to be incremented every time a mutation has
// foreign key checks and the checks are planned by the optimizer.
var ForeignKeyChecksUseCounter = telemetry.GetCounterOnce("sql.plan.fk.checks")

// ForeignKeyCascadesUseCounter is to be incremented every time a mutation
// involves a cascade. Currently, cascades use the legacy paths, so the
// ForeignKeyLegacyUseCounter would also be incremented in these cases.
var ForeignKeyCascadesUseCounter = telemetry.GetCounterOnce("sql.plan.fk.cascades")

// ForeignKeyLegacyUseCounter is to be incremented every time a mutation
// involves foreign key checks or cascades but uses the legacy execution path
// (either because it has cascades or because the optimizer_foreign_keys setting
// is off).
var ForeignKeyLegacyUseCounter = telemetry.GetCounterOnce("sql.plan.fk.legacy")

// LateralJoinUseCounter is to be incremented whenever a query uses the
// LATERAL keyword.
var LateralJoinUseCounter = telemetry.GetCounterOnce("sql.plan.lateral-join")

// HashJoinHintUseCounter is to be incremented whenever a query specifies a
// hash join via a query hint.
var HashJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.hash-join")

// MergeJoinHintUseCounter is to be incremented whenever a query specifies a
// merge join via a query hint.
var MergeJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.merge-join")

// LookupJoinHintUseCounter is to be incremented whenever a query specifies a
// lookup join via a query hint.
var LookupJoinHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.lookup-join")

// IndexHintUseCounter is to be incremented whenever a query specifies an index
// hint. Incremented whenever one of the more specific variants below is
// incremented.
var IndexHintUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index")

// IndexHintSelectUseCounter is to be incremented whenever a query specifies an
// index hint in a SELECT.
var IndexHintSelectUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.select")

// IndexHintUpdateUseCounter is to be incremented whenever a query specifies an
// index hint in an UPDATE.
var IndexHintUpdateUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.update")

// IndexHintDeleteUseCounter is to be incremented whenever a query specifies an
// index hint in a DELETE.
var IndexHintDeleteUseCounter = telemetry.GetCounterOnce("sql.plan.hints.index.delete")

// InterleavedTableJoinCounter is to be incremented whenever an InterleavedTableJoin is planned.
var InterleavedTableJoinCounter = telemetry.GetCounterOnce("sql.plan.interleaved-table-join")

// ExplainPlanUseCounter is to be incremented whenever vanilla EXPLAIN is run.
var ExplainPlanUseCounter = telemetry.GetCounterOnce("sql.plan.explain")

// ExplainDistSQLUseCounter is to be incremented whenever EXPLAIN (DISTSQL) is
// run.
var ExplainDistSQLUseCounter = telemetry.GetCounterOnce("sql.plan.explain-distsql")

// ExplainAnalyzeUseCounter is to be incremented whenever EXPLAIN ANALYZE is run.
var ExplainAnalyzeUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze")

// ExplainAnalyzeDebugUseCounter is to be incremented whenever
// EXPLAIN ANALYZE (DEBUG) is run.
var ExplainAnalyzeDebugUseCounter = telemetry.GetCounterOnce("sql.plan.explain-analyze-debug")

// ExplainOptUseCounter is to be incremented whenever EXPLAIN (OPT) is run.
var ExplainOptUseCounter = telemetry.GetCounterOnce("sql.plan.explain-opt")

// ExplainVecUseCounter is to be incremented whenever EXPLAIN (VEC) is run.
var ExplainVecUseCounter = telemetry.GetCounterOnce("sql.plan.explain-vec")

// ExplainOptVerboseUseCounter is to be incremented whenever
// EXPLAIN (OPT, VERBOSE) is run.
var ExplainOptVerboseUseCounter = telemetry.GetCounterOnce("sql.plan.explain-opt-verbose")

// CreateStatisticsUseCounter is to be incremented whenever a non-automatic
// run of CREATE STATISTICS occurs.
var CreateStatisticsUseCounter = telemetry.GetCounterOnce("sql.plan.stats.created")

// TurnAutoStatsOnUseCounter is to be incremented whenever automatic stats
// collection is explicitly enabled.
var TurnAutoStatsOnUseCounter = telemetry.GetCounterOnce("sql.plan.automatic-stats.enabled")

// TurnAutoStatsOffUseCounter is to be incremented whenever automatic stats
// collection is explicitly disabled.
var TurnAutoStatsOffUseCounter = telemetry.GetCounterOnce("sql.plan.automatic-stats.disabled")

// JoinAlgoHashUseCounter is to be incremented whenever a hash join node is
// planned.
var JoinAlgoHashUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.hash")

// JoinAlgoMergeUseCounter is to be incremented whenever a merge join node is
// planned.
var JoinAlgoMergeUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.merge")

// JoinAlgoLookupUseCounter is to be incremented whenever a lookup join node is
// planned.
var JoinAlgoLookupUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.lookup")

// JoinAlgoCrossUseCounter is to be incremented whenever a cross join node is
// planned.
var JoinAlgoCrossUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.algo.cross")

// JoinTypeInnerUseCounter is to be incremented whenever an inner join node is
// planned.
var JoinTypeInnerUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.inner")

// JoinTypeLeftUseCounter is to be incremented whenever a left or right outer
// join node is planned.
var JoinTypeLeftUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.left-outer")

// JoinTypeFullUseCounter is to be incremented whenever a full outer join node is
// planned.
var JoinTypeFullUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.full-outer")

// JoinTypeSemiUseCounter is to be incremented whenever a semi-join node is
// planned.
var JoinTypeSemiUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.semi")

// JoinTypeAntiUseCounter is to be incremented whenever an anti-join node is
// planned.
var JoinTypeAntiUseCounter = telemetry.GetCounterOnce("sql.plan.opt.node.join.type.anti")

// CancelQueriesUseCounter is to be incremented whenever CANCEL QUERY or
// CANCEL QUERIES is run.
var CancelQueriesUseCounter = telemetry.GetCounterOnce("sql.session.cancel-queries")

// CancelSessionsUseCounter is to be incremented whenever CANCEL SESSION or
// CANCEL SESSIONS is run.
var CancelSessionsUseCounter = telemetry.GetCounterOnce("sql.session.cancel-sessions")

// We can't parameterize these telemetry counters, so just make a bunch of
// buckets for setting the join reorder limit since the range of reasonable
// values for the join reorder limit is quite small.
// reorderJoinLimitUseCounters is a list of counters. The entry at position i
// is the counter for SET reorder_join_limit = i.
var reorderJoinLimitUseCounters []telemetry.Counter
var multiModelReorderJoinLimitUseCounters []telemetry.Counter

const reorderJoinsCounters = 12

const multiModelReorderJoinsCounters = 12

func init() {
	reorderJoinLimitUseCounters = make([]telemetry.Counter, reorderJoinsCounters)
	multiModelReorderJoinLimitUseCounters = make([]telemetry.Counter, multiModelReorderJoinsCounters)

	for i := 0; i < reorderJoinsCounters; i++ {
		reorderJoinLimitUseCounters[i] = telemetry.GetCounterOnce(
			fmt.Sprintf("sql.plan.reorder-joins.set-limit-%d", i),
		)
	}

	for i := 0; i < multiModelReorderJoinsCounters; i++ {
		multiModelReorderJoinLimitUseCounters[i] = telemetry.GetCounterOnce(
			fmt.Sprintf("sql.plan.multi-model-reorder-joins.set-limit-%d", i),
		)
	}
}

// ReorderJoinLimitMoreCounter is the counter for the number of times someone
// set the join reorder limit above reorderJoinsCounters.
var reorderJoinLimitMoreCounter = telemetry.GetCounterOnce("sql.plan.reorder-joins.set-limit-more")

// MultiModelReorderJoinLimitMoreCounter is the counter for the number of times someone
// set the join reorder limit above reorderJoinsCounters.
var multiModelReorderJoinLimitMoreCounter = telemetry.GetCounterOnce("sql.plan.multi-model-reorder-joins.set-limit-more")

// ReportJoinReorderLimit is to be called whenever the reorder joins session variable
// is set.
func ReportJoinReorderLimit(value int) {
	if value < reorderJoinsCounters {
		telemetry.Inc(reorderJoinLimitUseCounters[value])
	} else {
		telemetry.Inc(reorderJoinLimitMoreCounter)
	}
}

// ReportMultiModelJoinReorderLimit is to be called whenever the reorder joins session variable for multi-model processing
// is set.
func ReportMultiModelJoinReorderLimit(value int) {
	if value < multiModelReorderJoinsCounters {
		telemetry.Inc(multiModelReorderJoinLimitUseCounters[value])
	} else {
		telemetry.Inc(multiModelReorderJoinLimitMoreCounter)
	}
}

// WindowFunctionCounter is to be incremented every time a window function is
// being planned.
func WindowFunctionCounter(wf string) telemetry.Counter {
	return telemetry.GetCounter("sql.plan.window_function." + wf)
}

// OptNodeCounter should be incremented every time a node of the given
// type is encountered at the end of the query optimization (i.e. it
// counts the nodes actually used for physical planning).
func OptNodeCounter(nodeType string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.opt.node.%s", nodeType))
}
