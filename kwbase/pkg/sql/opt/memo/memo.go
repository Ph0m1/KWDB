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

package memo

import (
	"context"
	"math"
	"runtime"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/shirou/gopsutil/mem"
)

// Memo is a data structure for efficiently storing a forest of query plans.
// Conceptually, the memo is composed of a numbered set of equivalency classes
// called groups where each group contains a set of logically equivalent
// expressions. Two expressions are considered logically equivalent if:
//
//  1. They return the same number and data type of columns. However, order and
//     naming of columns doesn't matter.
//  2. They return the same number of rows, with the same values in each row.
//     However, order of rows doesn't matter.
//
// The different expressions in a single group are called memo expressions
// (memo-ized expressions). The children of a memo expression can themselves be
// part of memo groups. Therefore, the memo forest is composed of every possible
// combination of parent expression with its possible child expressions,
// recursively applied.
//
// Memo expressions can be relational (e.g. join) or scalar (e.g. <). Operators
// are always both logical (specify results) and physical (specify results and
// a particular implementation). This means that even a "raw" unoptimized
// expression tree can be executed (naively). Both relational and scalar
// operators are uniformly represented as nodes in memo expression trees, which
// facilitates tree pattern matching and replacement. However, because scalar
// expression memo groups never have more than one expression, scalar
// expressions can use a simpler representation.
//
// Because memo groups contain logically equivalent expressions, all the memo
// expressions in a group share the same logical properties. However, it's
// possible for two logically equivalent expression to be placed in different
// memo groups. This occurs because determining logical equivalency of two
// relational expressions is too complex to perform 100% correctly. A
// correctness failure (i.e. considering two expressions logically equivalent
// when they are not) results in invalid transformations and invalid plans.
// But placing two logically equivalent expressions in different groups has a
// much gentler failure mode: the memo and transformations are less efficient.
// Expressions within the memo may have different physical properties. For
// example, a memo group might contain both hash join and merge join
// expressions which produce the same set of output rows, but produce them in
// different orders.
//
// Expressions are inserted into the memo by the factory, which ensure that
// expressions have been fully normalized before insertion (see the comment in
// factory.go for more details). A new group is created only when unique
// normalized expressions are created by the factory during construction or
// rewrite of the tree. Uniqueness is determined by "interning" each expression,
// which means that multiple equivalent expressions are mapped to a single
// in-memory instance. This allows interned expressions to be checked for
// equivalence by simple pointer comparison. For example:
//
//	SELECT * FROM a, b WHERE a.x = b.x
//
// After insertion into the memo, the memo would contain these six groups, with
// numbers substituted for pointers to the normalized expression in each group:
//
//	G6: [inner-join [G1 G2 G5]]
//	G5: [eq [G3 G4]]
//	G4: [variable b.x]
//	G3: [variable a.x]
//	G2: [scan b]
//	G1: [scan a]
//
// Each leaf expressions is interned by hashing its operator type and any
// private field values. Expressions higher in the tree can then rely on the
// fact that all children have been interned, and include their pointer values
// in its hash value. Therefore, the memo need only hash the expression's fields
// in order to determine whether the expression already exists in the memo.
// Walking the subtree is not necessary.
//
// The normalizing factory will never add more than one expression to a memo
// group. But the explorer does add denormalized expressions to existing memo
// groups, since oftentimes one of these equivalent, but denormalized
// expressions will have a lower cost than the initial normalized expression
// added by the factory. For example, the join commutativity transformation
// expands the memo like this:
//
//	G6: [inner-join [G1 G2 G5]] [inner-join [G2 G1 G5]]
//	G5: [eq [G3 G4]]
//	G4: [variable b.x]
//	G3: [variable a.x]
//	G2: [scan b]
//	G1: [scan a]
//
// See the comments in explorer.go for more details.
type Memo struct {
	// metadata provides information about the columns and tables used in this
	// particular query.
	metadata opt.Metadata

	// interner interns all expressions in the memo, ensuring that there is at
	// most one instance of each expression in the memo.
	interner interner

	// logPropsBuilder is inlined in the memo so that it can be reused each time
	// scalar or relational properties need to be built.
	logPropsBuilder logicalPropsBuilder

	// rootExpr is the root expression of the memo expression forest. It is set
	// via a call to SetRoot. After optimization, it is set to be the root of the
	// lowest cost tree in the forest.
	rootExpr opt.Expr

	// rootProps are the physical properties required of the root memo expression.
	// It is set via a call to SetRoot.
	rootProps *physical.Required

	// memEstimate is the approximate memory usage of the memo, in bytes.
	memEstimate int64

	// The following are selected fields from SessionData which can affect
	// planning. We need to cross-check these before reusing a cached memo.
	dataConversion    sessiondata.DataConversionConfig
	reorderJoinsLimit int
	zigzagJoinEnabled bool
	optimizerFKs      bool
	safeUpdates       bool
	saveTablesPrefix  string
	insertFastPath    bool

	// curID is the highest currently in-use scalar expression ID.
	curID opt.ScalarID

	// curWithID is the highest currently in-use WITH ID.
	curWithID opt.WithID

	newGroupFn func(opt.Expr)

	// WARNING: if you add more members, add initialization code in Init.

	// colsUsage is used to store the access pattern for all columns accessed by a statement
	ColsUsage []opt.ColumnUsage

	// CheckHelper used to check if the expr can execute in ts engine.
	CheckHelper TSCheckHelper

	// ts engine white list map
	TSWhiteListMap *sqlbase.WhiteListMap

	// tsDop represents degree of parallelism control parallelism in time series engine
	tsDop uint32
}

// TSCheckHelper ts check helper, helper check flags, push column, white list and so on
type TSCheckHelper struct {
	// flags record some flags used in TS query.
	flags int

	// ts white list map
	whiteList *sqlbase.WhiteListMap

	// pushHelper check expr can be pushed to ts engine
	pushHelper PushHelper

	// function to check if we run in multi node mode
	checkMultiNode CheckMultiNode

	// ctx, check multi node function param, context
	ctx context.Context

	// GroupHint is the hint that control the group by cannot be executed concurrently
	// or must be executed in the relationship engine
	// HintType: ForceNoSynchronizerGroup and ForceRelationalGroup
	GroupHint keys.GroupHintType
}

// init inits the TSCheckHelper
func (m *TSCheckHelper) init() {
	m.flags = 0
	m.pushHelper.MetaMap = make(MetaInfoMap, 0)
	m.GroupHint = keys.NoGroupHint
}

// isSingleNode check if the query is executed in single node.
func (m *TSCheckHelper) isSingleNode() bool {
	if m.checkMultiNode != nil {
		return !m.checkMultiNode(m.ctx)
	}
	return false
}

// CheckMultiNode check multi node function
type CheckMultiNode func(ctx context.Context) bool

// Init initializes a new empty memo instance, or resets existing state so it
// can be reused. It must be called before use (or reuse). The memo collects
// information about the context in which it is compiled from the evalContext
// argument. If any of that changes, then the memo must be invalidated (see the
// IsStale method for more details).
func (m *Memo) Init(evalCtx *tree.EvalContext) {
	m.metadata.Init()
	m.interner.Clear()
	m.logPropsBuilder.init(evalCtx, m)

	m.rootExpr = nil
	m.rootProps = nil
	m.memEstimate = 0

	m.dataConversion = evalCtx.SessionData.DataConversion
	m.reorderJoinsLimit = evalCtx.SessionData.ReorderJoinsLimit
	m.zigzagJoinEnabled = evalCtx.SessionData.ZigzagJoinEnabled
	m.optimizerFKs = evalCtx.SessionData.OptimizerFKs
	m.safeUpdates = evalCtx.SessionData.SafeUpdates
	m.saveTablesPrefix = evalCtx.SessionData.SaveTablesPrefix
	m.insertFastPath = evalCtx.SessionData.InsertFastPath

	m.curID = 0
	m.curWithID = 0
	m.ColsUsage = nil
	m.CheckHelper.init()

	m.tsDop = 0
}

// InitTS initializes some elements to determine if the memo exprs can be executed in the TS engine.
// hasTSTable is true when the query contain ts table.
// singleNode is true when the server is start single mode.
// whiteMap is a whitelist map for check expr can exec in ts engine.
// f is the function to check if we run in multi node mode.
// execInTSEngine is true when the sql.all_push_down.enabled is true.
// enableTSAutomaticCollection is true when cluster setting sql.stats.ts_automatic_collection.enabled is true.
func (m *Memo) InitTS(
	ctx context.Context,
	hasTSTable, singleNode bool,
	whiteMap *sqlbase.WhiteListMap,
	f CheckMultiNode,
	execInTSEngine bool,
	enableTSAutomaticCollection bool,
) {
	if hasTSTable {
		m.SetFlag(opt.IncludeTSTable)
	}
	if singleNode {
		m.SetFlag(opt.SingleMode)
	}
	if execInTSEngine {
		m.SetFlag(opt.ExecInTSEngine)
	}
	if !enableTSAutomaticCollection {
		m.CheckHelper.GroupHint = keys.ForceAEGroup
	}

	m.TSWhiteListMap = whiteMap
	m.tsDop = 0

	m.CheckHelper.whiteList = whiteMap
	m.CheckHelper.checkMultiNode = f
	m.CheckHelper.ctx = ctx
}

// GetWhiteList return the white list from memo.
func (m *Memo) GetWhiteList() *sqlbase.WhiteListMap {
	return m.CheckHelper.whiteList
}

// SetWhiteList set white list
func (m *Memo) SetWhiteList(src *sqlbase.WhiteListMap) {
	m.CheckHelper.whiteList = src
}

// NotifyOnNewGroup sets a callback function which is invoked each time we
// create a new memo group.
func (m *Memo) NotifyOnNewGroup(fn func(opt.Expr)) {
	m.newGroupFn = fn
}

// IsEmpty returns true if there are no expressions in the memo.
func (m *Memo) IsEmpty() bool {
	// Root expression can be nil before optimization and interner is empty after
	// exploration, so check both.
	return m.interner.Count() == 0 && m.rootExpr == nil
}

// MemoryEstimate returns a rough estimate of the memo's memory usage, in bytes.
// It only includes memory usage that is proportional to the size and complexity
// of the query, rather than constant overhead bytes.
func (m *Memo) MemoryEstimate() int64 {
	// Multiply by 2 to take rough account of allocation fragmentation, private
	// data, list overhead, properties, etc.
	return m.memEstimate * 2
}

// Metadata returns the metadata instance associated with the memo.
func (m *Memo) Metadata() *opt.Metadata {
	return &m.metadata
}

// RootExpr returns the root memo expression previously set via a call to
// SetRoot.
func (m *Memo) RootExpr() opt.Expr {
	return m.rootExpr
}

// RootProps returns the physical properties required of the root memo group,
// previously set via a call to SetRoot.
func (m *Memo) RootProps() *physical.Required {
	return m.rootProps
}

// SetRoot stores the root memo expression when it is a relational expression,
// and also stores the physical properties required of the root group.
func (m *Memo) SetRoot(e RelExpr, phys *physical.Required) {
	m.rootExpr = e
	if m.rootProps != phys {
		m.rootProps = m.InternPhysicalProps(phys)
	}

	// Once memo is optimized, release reference to the eval context and free up
	// the memory used by the interner.
	if m.IsOptimized() {
		m.logPropsBuilder.clear()
		m.interner.Clear()
	}
}

// SetScalarRoot stores the root memo expression when it is a scalar expression.
// Used only for testing.
func (m *Memo) SetScalarRoot(scalar opt.ScalarExpr) {
	m.rootExpr = scalar
}

// HasPlaceholders returns true if the memo contains at least one placeholder
// operator.
func (m *Memo) HasPlaceholders() bool {
	rel, ok := m.rootExpr.(RelExpr)
	if !ok {
		panic(errors.AssertionFailedf("placeholders only supported when memo root is relational"))
	}

	return rel.Relational().HasPlaceholder
}

// IsStale returns true if the memo has been invalidated by changes to any of
// its dependencies. Once a memo is known to be stale, it must be ejected from
// any query cache or prepared statement and replaced with a recompiled memo
// that takes into account the changes. IsStale checks the following
// dependencies:
//
//  1. Current database: this can change name resolution.
//  2. Current search path: this can change name resolution.
//  3. Current location: this determines time zone, and can change how time-
//     related types are constructed and compared.
//  4. Data source schema: this determines most aspects of how the query is
//     compiled.
//  5. Data source privileges: current user may no longer have access to one or
//     more data sources.
//
// This function cannot swallow errors and return only a boolean, as it may
// perform KV operations on behalf of the transaction associated with the
// provided catalog, and those errors are required to be propagated.
func (m *Memo) IsStale(
	ctx context.Context, evalCtx *tree.EvalContext, catalog cat.Catalog,
) (bool, error) {
	// Memo is stale if fields from SessionData that can affect planning have
	// changed.
	if !m.dataConversion.Equals(&evalCtx.SessionData.DataConversion) ||
		m.reorderJoinsLimit != evalCtx.SessionData.ReorderJoinsLimit ||
		m.zigzagJoinEnabled != evalCtx.SessionData.ZigzagJoinEnabled ||
		m.optimizerFKs != evalCtx.SessionData.OptimizerFKs ||
		m.safeUpdates != evalCtx.SessionData.SafeUpdates ||
		m.saveTablesPrefix != evalCtx.SessionData.SaveTablesPrefix ||
		m.insertFastPath != evalCtx.SessionData.InsertFastPath {
		return true, nil
	}

	// Memo is stale if the fingerprint of any object in the memo's metadata has
	// changed, or if the current user no longer has sufficient privilege to
	// access the object.
	if depsUpToDate, err := m.Metadata().CheckDependencies(ctx, catalog); err != nil {
		return true, err
	} else if !depsUpToDate {
		return true, nil
	}
	return false, nil
}

// InternPhysicalProps adds the given physical props to the memo if they haven't
// yet been added. If the same props was added previously, then return a pointer
// to the previously added props. This allows interned physical props to be
// compared for equality using simple pointer comparison.
func (m *Memo) InternPhysicalProps(phys *physical.Required) *physical.Required {
	// Special case physical properties that require nothing of operator.
	if !phys.Defined() {
		return physical.MinRequired
	}
	return m.interner.InternPhysicalProps(phys)
}

// SetBestProps updates the physical properties, provided ordering, and cost of
// a relational expression's memo group (see the relevant methods of RelExpr).
// It is called by the optimizer once it determines the expression in the group
// that is part of the lowest cost tree (for the overall query).
func (m *Memo) SetBestProps(
	e RelExpr, required *physical.Required, provided *physical.Provided, cost Cost,
) {
	if e.RequiredPhysical() != nil {
		if e.RequiredPhysical() != required ||
			!e.ProvidedPhysical().Equals(provided) ||
			e.Cost() != cost {
			panic(errors.AssertionFailedf(
				"cannot overwrite %s / %s (%.9g) with %s / %s (%.9g)",
				e.RequiredPhysical(),
				e.ProvidedPhysical(),
				log.Safe(e.Cost()),
				required.String(),
				provided.String(), // Call String() so provided doesn't escape.
				cost,
			))
		}
		return
	}
	bp := e.bestProps()
	bp.required = required
	bp.provided = *provided
	bp.cost = cost
}

// ResetCost updates the cost of a relational expression's memo group. It
// should *only* be called by Optimizer.RecomputeCost() for testing purposes.
func (m *Memo) ResetCost(e RelExpr, cost Cost) {
	e.bestProps().cost = cost
}

// IsOptimized returns true if the memo has been fully optimized.
func (m *Memo) IsOptimized() bool {
	// The memo is optimized once the root expression has its physical properties
	// assigned.
	rel, ok := m.rootExpr.(RelExpr)
	return ok && rel.RequiredPhysical() != nil
}

// NextID returns a new unique ScalarID to number expressions with.
func (m *Memo) NextID() opt.ScalarID {
	m.curID++
	return m.curID
}

// RequestColStat calculates and returns the column statistic calculated on the
// relational expression.
func (m *Memo) RequestColStat(
	expr RelExpr, cols opt.ColSet,
) (colStat *props.ColumnStatistic, ok bool) {
	// When SetRoot is called, the statistics builder may have been cleared.
	// If this happens, we can't serve the request anymore.
	if m.logPropsBuilder.sb.md != nil {
		return m.logPropsBuilder.sb.colStat(cols, expr), true
	}
	return nil, false
}

// RowsProcessed calculates and returns the number of rows processed by the
// relational expression. It is currently only supported for joins.
func (m *Memo) RowsProcessed(expr RelExpr) (_ float64, ok bool) {
	// When SetRoot is called, the statistics builder may have been cleared.
	// If this happens, we can't serve the request anymore.
	if m.logPropsBuilder.sb.md != nil {
		return m.logPropsBuilder.sb.rowsProcessed(expr), true
	}
	return 0, false
}

// NextWithID returns a not-yet-assigned identifier for a WITH expression.
func (m *Memo) NextWithID() opt.WithID {
	m.curWithID++
	return m.curWithID
}

// Detach is used when we detach a memo that is to be reused later (either for
// execbuilding or with AssignPlaceholders). New expressions should no longer be
// constructed in this memo.
func (m *Memo) Detach() {
	m.interner = interner{}
	// It is important to not hold on to the EvalCtx in the logicalPropsBuilder
	// (#57059).
	m.logPropsBuilder = logicalPropsBuilder{}

	// Clear all column statistics from every relational expression in the memo.
	// This is used to free up the potentially large amount of memory used by
	// histograms.
	var clearColStats func(parent opt.Expr)
	clearColStats = func(parent opt.Expr) {
		for i, n := 0, parent.ChildCount(); i < n; i++ {
			child := parent.Child(i)
			clearColStats(child)
		}

		switch t := parent.(type) {
		case RelExpr:
			t.Relational().Stats.ColStats = props.ColStatsMap{}
		}
	}
	clearColStats(m.RootExpr())
}

// AddColumn add column to map.
// col is the id of column, alias is the name of column.
// typ is type of column, pos is the position of column.
// hash is the hash value of the column name and type.
// isTimeBucket is true when the column is time_bucket.
func (m *Memo) AddColumn(
	col opt.ColumnID, alias string, typ ExprType, pos ExprPos, hash uint32, isTimeBucket bool,
) {
	m.CheckHelper.pushHelper.lock.Lock()
	m.CheckHelper.pushHelper.MetaMap[col] = ExprInfo{Alias: alias, Type: typ, Pos: pos, Hash: hash, IsTimeBucket: isTimeBucket}
	m.CheckHelper.pushHelper.lock.Unlock()
}

// GetPushHelperAddress return the push helper address
func (m *Memo) GetPushHelperAddress() *MetaInfoMap {
	return &m.CheckHelper.pushHelper.MetaMap
}

// CheckExecInTS check if the column can execute in ts engine, but
// the columns is the logical columns which may be an expr.
// col is the column ID of the logical column.
// pos is the position where the column appears, it can be ExprPosSelect,ExprPosProjList,ExprPosGroupBy
func (m *Memo) CheckExecInTS(col opt.ColumnID, pos ExprPos) bool {
	m.CheckHelper.pushHelper.lock.Lock()
	info, ok := m.CheckHelper.pushHelper.MetaMap[col]
	m.CheckHelper.pushHelper.lock.Unlock()
	if !ok {
		return false
	}

	// single column and const can always execute in ts engine.
	if info.Type == ExprTypCol || info.Type == ExprTypConst {
		return true
	}

	// check from whitelist
	return m.CheckHelper.whiteList.CheckWhiteListParam(info.Hash, uint32(pos))
}

// CheckFlag check if the flag is set.
// flag: flag that need to be checked
func (m *Memo) CheckFlag(flag int) bool {
	return m.CheckHelper.flags&flag > 0
}

// SetFlag set flag is true
func (m *Memo) SetFlag(flag int) {
	m.CheckHelper.flags |= flag
}

// SetAllFlag set all flag
func (m *Memo) SetAllFlag(flags int) {
	m.CheckHelper.flags = flags
}

// GetAllFlag return all flag
func (m *Memo) GetAllFlag() int {
	return m.CheckHelper.flags
}

// ClearFlag clear the flag.
func (m *Memo) ClearFlag(flag int) {
	m.CheckHelper.flags &= ^flag
}

// CheckWhiteListAndAddSynchronize check if the memo expr can execute in ts engine
// according to white list and set flag to add Synchronizer.
// src is the expr of memo tree.
func (m *Memo) CheckWhiteListAndAddSynchronize(src *RelExpr) error {
	if !m.CheckFlag(opt.IncludeTSTable) {
		return nil
	}
	// the main implementation of checking white list and setting flag for adding Synchronizer.
	execInTSEngine, hasAddSynchronizer, _, err := m.CheckWhiteListAndAddSynchronizeImp(src)
	if err != nil {
		return err
	}
	if execInTSEngine && !hasAddSynchronizer {
		(*src).SetAddSynchronizer()
	}
	return nil
}

// DealTSScanFunc deal with ts scan expr
type DealTSScanFunc func(expr *TSScanExpr)

// addColForTSScan add ts col to TSScanExpr when only
// TSScanExpr can execute in ts engine.
// case: select 1 from tstable
func addColForTSScan(expr *TSScanExpr) {
	if expr.Cols.Empty() {
		expr.Cols.Add(expr.Table.ColumnID(0))
	}
}

// walkDealTSScan find the TsScanExpr from the memo tree.
func walkDealTSScan(expr opt.Expr, f DealTSScanFunc) {
	if expr.Op() == opt.TSScanOp {
		f(expr.(*TSScanExpr))
		return
	}

	for i := 0; i < expr.ChildCount(); i++ {
		walkDealTSScan(expr.Child(i), f)
	}
}

// DealTSScan find the memo.TSScanExpr from the memo tree.
// And add ts col to TSScanExpr when only TSScanExpr can execute in ts engine.
func (m *Memo) DealTSScan(src RelExpr) {
	if !m.CheckFlag(opt.IncludeTSTable) {
		return
	}
	walkDealTSScan(src, addColForTSScan)
}

// dealWithGroupBy
// src is GroupByExpr or ScalarGroupByExpr
// input is the child expr of GroupByExpr or ScalarGroupByExpr
// execInTSEngine is true when(GroupByExpr / ScalarGroupByExpr) can execute in ts engine
// aggParallel is true when (GroupByExpr / ScalarGroupByExpr) can be paralleled
// hasDistinct is true when agg with distinct
// hasSynchronizer is true when the child have added the flag
// optTimeBucket is true when optimizing query efficiency in time_bucket case
func (m *Memo) dealWithGroupBy(
	src RelExpr,
	child RelExpr,
	execInTSEngine *bool,
	aggParallel bool,
	aggWithDistinct bool,
	hasSynchronizer bool,
	optTimeBucket bool,
) bool {
	aggs := make([]AggregationsItem, 0)
	var gp *GroupingPrivate
	switch t := src.(type) {
	case *GroupByExpr:
		aggs = t.Aggregations
		gp = &t.GroupingPrivate
		if m.CheckHelper.GroupHint != keys.ForceNoSynchronizerGroup {
			t.engine = tree.EngineTypeRelational
		}
	case *ScalarGroupByExpr:
		aggs = t.Aggregations
		gp = &t.GroupingPrivate
		if m.CheckHelper.GroupHint != keys.ForceNoSynchronizerGroup {
			t.engine = tree.EngineTypeRelational
		}
	}

	// do nothing when group by can not execute in ts engine.
	if !(*execInTSEngine) {
		return false
	}

	// case: agg with distinct
	if aggWithDistinct {
		// multi node, agg with distinct can not execute in ts engine.
		if !(m.CheckFlag(opt.SingleMode) || m.CheckHelper.isSingleNode()) {
			if !hasSynchronizer {
				m.setSynchronizerForChild(child, &hasSynchronizer)
			}
			*execInTSEngine = false
			return false
		}
	}
	src.SetEngineTS()

	// fill statistics
	m.fillStatistic(&child, aggs, gp)

	if aggParallel {
		if !hasSynchronizer {
			if m.CheckHelper.GroupHint != keys.ForceNoSynchronizerGroup {
				src.SetAddSynchronizer()
			}
			hasSynchronizer = true
		}
	}

	if optTimeBucket {
		if private, ok := src.Private().(*GroupingPrivate); ok {
			private.AggPushDown = true
		}
	}

	return hasSynchronizer
}

// dealWithOrderBy set engine and add flag for the child of order by
// when it's child can exec in ts engine.
// sort is memo.SortExpr of memo tree.
// execInTSEngine is true when the child of sort can exec in ts engine.
// hasAddSynchronizer is true when the child have added the flag.
// props is the bestProps of (memo.GroupByExpr or memo.DistinctOnExpr),
// props is not nil when there is a OrderGroupBy.
func (m *Memo) dealWithOrderBy(
	sort *SortExpr, execInTSEngine bool, hasAddSynchronizer *bool, props *bestProps,
) {
	if execInTSEngine {
		// only single node, order by can exec in ts engine.
		if m.CheckFlag(opt.SingleMode) || m.CheckHelper.isSingleNode() {
			sort.SetEngineTS()
		}

		if !(*hasAddSynchronizer) {
			sort.Input.SetAddSynchronizer()
			*hasAddSynchronizer = true
		}
	}
	// OrderGroupBy case, reset bestProps of (memo.GroupByExpr or memo.DistinctOnExpr)
	if props != nil {
		sort.best.required = props.required
		props.required = &physical.Required{}
		props.provided = physical.Provided{}
	}
}

// fillStatistic fill statistics for the child of group by if group by can execute
// in ts engine and the agg can use statistics collected by storage engine.
// child is the Input of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// aggs is the Aggregations of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// gp is the GroupingPrivate of (memo.GroupByExpr / memo.ScalarGroupByExpr).
func (m *Memo) fillStatistic(child *RelExpr, aggs []AggregationsItem, gp *GroupingPrivate) {
	if !m.checkAggStatisticUsable(aggs) {
		return
	}
	switch src := (*child).(type) {
	case *TSScanExpr:
		m.tsScanFillStatistic(src, aggs, gp)
	case *SelectExpr:
		m.selectExprFillStatistic(src, aggs, gp)
	}
}

// tsScanFillStatistic fill tsScan's statistics.
// tsScan is memo.TSScanExpr.
// aggs is the Aggregations of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// gp is the GroupingPrivate of (memo.GroupByExpr / memo.ScalarGroupByExpr).
func (m *Memo) tsScanFillStatistic(
	tsScan *TSScanExpr, aggs []AggregationsItem, gp *GroupingPrivate,
) {
	allColsPrimary := true
	gp.GroupingCols.ForEach(func(colID opt.ColumnID) {
		colMeta := m.Metadata().ColumnMeta(colID)
		allColsPrimary = allColsPrimary && colMeta.IsPrimaryTag()
	})
	tableMeta := m.Metadata().TableMeta(tsScan.Table)
	if !allColsPrimary || (gp.GroupingCols.Len() != 0 && gp.GroupingCols.Len() != tableMeta.PrimaryTagCount) || tsScan.HintType == keys.TagOnlyHint {
		return
	}

	gp.GroupingCols.ForEach(func(colID opt.ColumnID) {
		gp.AggIndex = append(gp.AggIndex, []uint32{uint32(len(tsScan.ScanAggs))})
		tsScan.ScanAggs = append(tsScan.ScanAggs, ScanAgg{ParamColID: colID, AggSpecTyp: execinfrapb.AggregatorSpec_ANY_NOT_NULL})
	})

	for i := range aggs {
		m.addScanAggs(aggs[i].Agg, &tsScan.ScanAggs, gp, tsScan.Table.ColumnID(0))
	}
}

// tsScanFillStatistic fill filter's statistics.
// selectExpr is memo.SelectExpr.
// aggs is the Aggregations of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// gp is the GroupingPrivate of (memo.GroupByExpr / memo.ScalarGroupByExpr).
func (m *Memo) selectExprFillStatistic(
	selectExpr *SelectExpr, aggs []AggregationsItem, gp *GroupingPrivate,
) {

	for _, filter := range selectExpr.Filters {
		if !m.checkFiltersStatisticUsable(filter.Condition) {
			return
		}
	}

	if tsScanExpr, ok := selectExpr.Input.(*TSScanExpr); ok {
		m.tsScanFillStatistic(tsScanExpr, aggs, gp)
	}
}

// isTsColumnOrConst checks whether the column is
// the first column in the ts table or a constant column.
// Returns:
//   - uint32: 0: It's neither a constant column nor the first column in ts table.
//     1: It's the first column in ts table.
//     2: It's a const column.
func (m *Memo) isTsColumnOrConst(src opt.ScalarExpr) uint32 {
	switch source := src.(type) {
	case *VariableExpr:
		tblID := m.Metadata().ColumnMeta(source.Col).Table
		if tblID != 0 {
			table := m.Metadata().Table(tblID)
			tsColID := table.Column(0).ColID()
			// k_timestamp column can use statistic
			if source.Col == opt.ColumnID(tsColID) {
				return 1
			}
		}
		return 0
	case *ConstExpr:
		return 2
	}
	return 0
}

// checkFiltersStatisticUsable checks whether the filtering conditions
// meet the requirements for using statistics collected by storage engine.
func (m *Memo) checkFiltersStatisticUsable(src opt.ScalarExpr) bool {
	if src.Op() == opt.LtOp || src.Op() == opt.LeOp || src.Op() == opt.EqOp || src.Op() == opt.GtOp || src.Op() == opt.GeOp {
		var sum uint32
		for i := 0; i < src.ChildCount(); i++ {
			sum |= m.isTsColumnOrConst(src.Child(i).(opt.ScalarExpr))
		}

		return sum == 3
	}
	switch source := src.(type) {
	case *VariableExpr:
		tblID := m.Metadata().ColumnMeta(source.Col).Table
		if tblID != 0 {
			table := m.Metadata().Table(tblID)
			tsColID := table.Column(0).ColID()
			// k_timestamp column can use statistic
			if source.Col == opt.ColumnID(tsColID) {
				return true
			}
		}
		return false
	case *ConstExpr:
		return true
	case *RangeExpr:
		return m.checkFiltersStatisticUsable(source.And)
	case *OrExpr:
		lCan := m.checkFiltersStatisticUsable(source.Left)
		rCan := m.checkFiltersStatisticUsable(source.Right)
		return lCan && rCan
	case *AndExpr:
		lCan := m.checkFiltersStatisticUsable(source.Left)
		rCan := m.checkFiltersStatisticUsable(source.Right)
		return lCan && rCan
	default:
		return false
	}
}

// checkAggStatisticUsable checks that statistics are available for aggregation functions
func (m *Memo) checkAggStatisticUsable(aggs []AggregationsItem) bool {
	if len(aggs) == 0 {
		return false
	}
	for i := range aggs {
		switch aggs[i].Agg.(type) {
		case *SumExpr, *MinExpr, *MaxExpr, *CountExpr, *FirstExpr, *FirstTimeStampExpr, *FirstRowExpr,
			*FirstRowTimeStampExpr, *LastExpr, *LastTimeStampExpr, *LastRowExpr, *LastRowTimeStampExpr, *CountRowsExpr:
		default:
			return false
		}
	}

	return true
}

// statisticAgg is the aggregate function which can use statistic.
type statisticAgg struct {
	LocalStage []execinfrapb.AggregatorSpec_Func
}

// StatisticAggTable is a map of the aggregate functions which can use statistic.
var StatisticAggTable = map[opt.Operator]statisticAgg{
	opt.SumOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM},
	},
	opt.MinOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_MIN},
	},
	opt.MaxOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_MAX},
	},
	opt.AvgOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_SUM, execinfrapb.AggregatorSpec_COUNT},
	},
	opt.CountOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_COUNT},
	},
	opt.CountRowsOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_COUNT},
	},
	opt.FirstOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_FIRST, execinfrapb.AggregatorSpec_FIRSTTS},
	},
	opt.FirstTimeStampOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_FIRST, execinfrapb.AggregatorSpec_FIRSTTS},
	},
	opt.FirstRowOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_FIRST_ROW, execinfrapb.AggregatorSpec_FIRST_ROW_TS},
	},
	opt.FirstRowTimeStampOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_FIRST_ROW, execinfrapb.AggregatorSpec_FIRST_ROW_TS},
	},
	opt.LastOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_LAST, execinfrapb.AggregatorSpec_LASTTS},
	},
	opt.LastTimeStampOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_LAST, execinfrapb.AggregatorSpec_LASTTS},
	},
	opt.LastRowOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_LAST_ROW, execinfrapb.AggregatorSpec_LAST_ROW_TS},
	},
	opt.LastRowTimeStampOp: {
		LocalStage: []execinfrapb.AggregatorSpec_Func{execinfrapb.AggregatorSpec_LAST_ROW, execinfrapb.AggregatorSpec_LAST_ROW_TS},
	},
}

// addScanAggs adds scan Aggs to tsExpr.ScanAggs.
// agg is the one of the Aggregations of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// scanAggs is ScanAggs of memo.TSScanExpr.
// gp is the GroupingPrivate of (memo.GroupByExpr / memo.ScalarGroupByExpr).
// tsCol is the logical column ID of the timestamp colimn of the ts table.
func (m *Memo) addScanAggs(
	agg opt.ScalarExpr, scanAggs *ScanAggArray, gp *GroupingPrivate, tsCol opt.ColumnID,
) {
	if val, ok := StatisticAggTable[agg.Op()]; ok {
		colID := tsCol
		if agg.ChildCount() > 0 {
			colID = m.getArgColID(agg.Child(0).(opt.ScalarExpr))
		}

		var index []uint32
		for _, v := range val.LocalStage {
			// exists agg , use it
			if idx := m.fillScanAggs(scanAggs, colID, v); idx != -1 {
				index = append(index, uint32(idx))
			} else {
				index = append(index, uint32(len(*scanAggs)-1))
			}
		}
		gp.AggIndex = append(gp.AggIndex, index)
	}
	return
}

// getArgColID return the column ID of the parameter of the agg function.
// expr is the parameter of the agg function.
func (m *Memo) getArgColID(expr opt.ScalarExpr) opt.ColumnID {
	if arg, ok := expr.(*VariableExpr); ok {
		return arg.Col
	}
	return 1
}

// fillScanAggs adds the column id of argument of agg and
// the spec type of agg to the tsScan.ScanAggs.
// scanAggs is ScanAggs of memo.TSScanExpr.
// colID is the column ID of the parameter of the agg function.
// aggTyp is one of StatisticAggTable.
func (m *Memo) fillScanAggs(
	scanAggs *ScanAggArray, colID opt.ColumnID, aggTyp execinfrapb.AggregatorSpec_Func,
) int {
	var agg ScanAgg
	agg.ParamColID = colID
	agg.AggSpecTyp = aggTyp
	return addDistinctScanAgg(scanAggs, &agg)
}

// addDistinctScanAgg append distinct ScanAgg to tsScan.ScanAggs
// scanAggs is ScanAggs of memo.TSScanExpr.
func addDistinctScanAgg(scanAggs *ScanAggArray, scanAgg *ScanAgg) int {
	for i, v := range *scanAggs {
		if v.ParamColID == scanAgg.ParamColID && v.AggSpecTyp == scanAgg.AggSpecTyp {
			return i
		}
	}
	*scanAggs = append(*scanAggs, *scanAgg)
	return -1
}

// CheckWhiteListAndAddSynchronizeImp check if each expr of memo tree can execute in ts engine,
// and set the engine of expr to opt.EngineTS when it can execute in ts engine,
// and set the addSynchronizer of expr to true when it needs to be synchronized.
// src is the expr of memo tree.
// returns:
// param1: return true when the expr can execute in ts engine.
// param2: return true when the expr is set the addSynchronizer to true.
// param3: return true when optimizing query efficiency in time_bucket case.
// param4: is the error.
func (m *Memo) CheckWhiteListAndAddSynchronizeImp(src *RelExpr) (bool, bool, bool, error) {
	switch source := (*src).(type) {
	case *TSScanExpr:
		execInTSEngine, hasAddSynchronizer, err := m.CheckTSScan(source)
		return execInTSEngine, hasAddSynchronizer, true, err
	case *SelectExpr:
		return m.checkSelect(source)
	case *ProjectExpr:
		return m.checkProject(source)
	case *ProjectSetExpr:
		_, _, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
		return false, false, false, err
	case *GroupByExpr:
		sort, ok := (*src).Child(0).(*SortExpr)
		if ok {
			m.SetFlag(opt.OrderGroupBy)
		}
		execInTSEngine, canMerge, aggWithDistinct, hasAddSynchronizer, optTimeBucket, err := m.checkGroupBy(source.Input, &source.Aggregations,
			&source.GroupingPrivate)
		if err != nil {
			return false, false, false, err
		}
		selfAdd := m.dealWithGroupBy(source, source.Input, &execInTSEngine, canMerge, aggWithDistinct, hasAddSynchronizer, optTimeBucket)
		if ok {
			if execInTSEngine {
				// swap the positions of GroupByExpr and OrderExpr, when GroupByExpr exec
				// in ts engine and there is the OrderGroupBy.
				source.Input = sort.Input
				sort.Input = source
				m.dealWithOrderBy(sort, execInTSEngine, &hasAddSynchronizer, source.bestProps())
				*src = sort
			} else {
				// group by can not exec in ts engine, clear flag and not need set root.
				m.ClearFlag(opt.OrderGroupBy)
			}
		}

		return execInTSEngine, selfAdd, optTimeBucket, nil
	case *ScalarGroupByExpr:
		execInTSEngine, canMerge, aggWithDistinct, hasAddSynchronizer, optTimeBucket, err := m.checkGroupBy(source.Input, &source.Aggregations,
			&source.GroupingPrivate)
		if err != nil {
			return false, false, false, err
		}
		selfAdd := m.dealWithGroupBy(source, source.Input, &execInTSEngine, canMerge, aggWithDistinct, hasAddSynchronizer, optTimeBucket)
		return execInTSEngine, selfAdd, optTimeBucket, nil
	case *InnerJoinExpr:
		execInTSEngine, hasAddSynchronizer, err := m.checkJoin(source)
		return execInTSEngine, hasAddSynchronizer, false, err
	case *UnionAllExpr, *UnionExpr, *IntersectExpr, *IntersectAllExpr, *ExceptAllExpr, *ExceptExpr:
		execInTSEngine, hasAddSynchronizer, err := m.checkSetop((*src).Child(0).(RelExpr), (*src).Child(1).(RelExpr))
		return execInTSEngine, hasAddSynchronizer, false, err
	case *AntiJoinExpr, *AntiJoinApplyExpr, *SemiJoinExpr, *SemiJoinApplyExpr, *MergeJoinExpr,
		*LeftJoinApplyExpr, *LeftJoinExpr, *RightJoinExpr, *InnerJoinApplyExpr, *FullJoinExpr:
		execInTSEngine, hasAddSynchronizer, err := m.checkOtherJoin(source)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *LookupJoinExpr:
		execInTSEngine, hasAddSynchronizer, err := m.checkLookupJoin(source)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *DistinctOnExpr:
		sort, ok := (*src).Child(0).(*SortExpr)
		if ok {
			m.SetFlag(opt.OrderGroupBy)
		}
		execInTSEngine, _, _, hasAddSynchronizer, _, err := m.checkGroupBy(source.Input, &source.Aggregations, &source.GroupingPrivate)
		if err != nil {
			return false, false, false, err
		}
		if execInTSEngine {
			if m.CheckFlag(opt.SingleMode) || m.CheckHelper.isSingleNode() {
				source.SetEngineTS()
			}
			if !hasAddSynchronizer {
				if !ok {
					source.Input.SetAddSynchronizer()
					hasAddSynchronizer = true
				} else {
					// swap the positions of DistinctOnExpr and OrderExpr, when DistinctOnExpr can exec
					// in ts engine and there is the OrderGroupBy.
					sort.Input.SetAddSynchronizer()
					hasAddSynchronizer = true
					source.Input = sort.Input
					sort.Input = source
					m.dealWithOrderBy(sort, execInTSEngine, &hasAddSynchronizer, source.bestProps())
					*src = sort
				}
			}
		} else {
			// distinct can not exec in ts engine, clear flag and not need set root.
			if ok {
				m.ClearFlag(opt.OrderGroupBy)
			}
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *LimitExpr:
		execInTSEngine, hasAddSynchronizer, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		if execInTSEngine {
			if !hasAddSynchronizer {
				source.SetAddSynchronizer()
				hasAddSynchronizer = true
			}
			source.SetEngineTS()
			if !hasAddSynchronizer {
				source.SetAddSynchronizer()
				hasAddSynchronizer = true
			}
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *ScanExpr:
		return false, false, false, nil
	case *OffsetExpr:
		_, hasAddSynchronizer, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return false, hasAddSynchronizer, false, nil
	case *ValuesExpr:
		return false, false, false, nil
	case *Max1RowExpr: // local plan ,so can not add synchronizer
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *OrdinalityExpr: // local plan ,so can not add synchronizer
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *VirtualScanExpr:
		return false, false, false, nil
	case *ExplainExpr:
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *ExportExpr:
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *OpaqueRelExpr:
		return false, false, false, nil
	case *SortExpr:
		execInTSEngine, hasAddSynchronizer, optTimeBucket, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
		if err != nil {
			return false, false, false, err
		}

		if !m.CheckFlag(opt.OrderGroupBy) {
			m.dealWithOrderBy(source, execInTSEngine, &hasAddSynchronizer, nil)
		}
		return execInTSEngine, hasAddSynchronizer, optTimeBucket, nil
	case *WithExpr:
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Binding)
		if err != nil {
			return false, false, false, err
		}

		execInTSEngine, hasAddSynchronizer, err = m.dealCanNotAddSynchronize(&source.Main)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *WindowExpr:
		execInTSEngine, hasAddSynchronizer, err := m.dealCanNotAddSynchronize(&source.Input)
		if err != nil {
			return false, false, false, err
		}
		return execInTSEngine, hasAddSynchronizer, false, nil
	case *WithScanExpr:
		return false, false, false, nil
	default:
		execInTSEngine := false
		hasAddSynchronizer := false
		var err error
		for i := 0; i < source.ChildCount(); i++ {
			if val, ok := source.Child(i).(RelExpr); ok {
				execInTSEngine, hasAddSynchronizer, _, err = m.CheckWhiteListAndAddSynchronizeImp(&val)
				if err != nil {
					return false, false, false, err
				}
				if execInTSEngine && !hasAddSynchronizer {
					val.SetAddSynchronizer()
					hasAddSynchronizer = true
				}
			}
		}

		return false, false, false, nil
	}
}

// dealCanNotAddSynchronize check if the child of the memo expr can execute in ts engine
// when the memo expr itself can not execute in ts engine.
// child is the child of memo expr.
// returns:
// param1: the memo expr can not execute in ts engine, always false.
// param2: return true when the child of memo expr is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) dealCanNotAddSynchronize(child *RelExpr) (bool, bool, error) {
	execInTSEngine, hasAddSynchronizer, _, err := m.CheckWhiteListAndAddSynchronizeImp(child)
	if err != nil {
		return false, false, err
	}
	if execInTSEngine && !hasAddSynchronizer {
		(*child).SetAddSynchronizer()
		hasAddSynchronizer = true
	}

	return false, true, nil
}

// CheckTSScan deal with memo.TSScanExpr of memo tree.
// Record the columns in pushHelper for future memo expr to
// determine if they can be executed in ts engine.
// returns:
// param1: the memo.TSScanExpr can execute in ts engine, always true.
// param2: return true when there is TagOnlyHint.
// param3: is the error.
func (m *Memo) CheckTSScan(source *TSScanExpr) (bool, bool, error) {
	hasNotTag := false
	source.Cols.ForEach(func(colID opt.ColumnID) {
		colMeta := m.metadata.ColumnMeta(colID)
		if colMeta.IsNormalCol() {
			hasNotTag = true
		}
		m.AddColumn(colID, colMeta.Alias, ExprTypCol, ExprPosNone, 0, false)
	})

	if source.HintType == keys.TagOnlyHint && hasNotTag {
		return true, source.HintType == keys.TagOnlyHint, pgerror.New(pgcode.FeatureNotSupported, "TAG_ONLY can only query tag columns")
	}
	source.SetEngineTS()
	// only tag mode should not add synchronizer, so param2 will be true.
	return true, source.HintType == keys.TagOnlyHint, nil
}

// GetSubQueryExpr save expr all info
type GetSubQueryExpr struct {
	m      *Memo
	hasSub bool
}

// IsTargetExpr checks if it's target expr to handle
func (p *GetSubQueryExpr) IsTargetExpr(self opt.Expr) bool {
	switch self.(type) {
	case *SubqueryExpr, *ExistsExpr, *ArrayFlattenExpr, *AnyExpr:
		child := self.Child(0).(RelExpr)
		_ = p.m.CheckWhiteListAndAddSynchronize(&child)
		p.hasSub = true
		return true
	}

	return false
}

// NeedToHandleChild checks if children expr need to be handled
func (p *GetSubQueryExpr) NeedToHandleChild() bool {
	return true
}

// HandleChildExpr deals with all child expr
func (p *GetSubQueryExpr) HandleChildExpr(parent opt.Expr, child opt.Expr) bool {
	return true
}

// checkSelect check if memo.SelectExpr can execute in ts engine.
// source is the memo.SelectExpr of memo tree.
// returns:
// param1: return true when the memo.SelectExpr can execute in ts engine.
// param2: return true when the memo.SelectExpr is set the addSynchronizer to true.
// param3: return true when optimizing query efficiency in time_bucket case.
// param4: is the error.
func (m *Memo) checkSelect(source *SelectExpr) (bool, bool, bool, error) {
	childExecInTS, hasAddSynchronizer, optTimeBucket, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
	if err != nil {
		return false, false, false, err
	}

	// scan or group by
	param := GetSubQueryExpr{m: m}
	selfExecInTS := childExecInTS
	for i, filter := range source.Filters {
		filter.Walk(&param)
		if param.hasSub {
			// can not break ,  need deal with all sub query
			selfExecInTS = false
			optTimeBucket = false
		}

		if childExecInTS {
			if CheckFilterExprCanExecInTSEngine(filter.Condition, ExprPosSelect, m.CheckHelper.whiteList.CheckWhiteListParam) {
				if optTimeBucket {
					optTimeBucket = m.checkFilterOptTimeBucket(filter.Condition)
				}
				source.Filters[i].SetEngineTS()
			} else {
				selfExecInTS = false
				optTimeBucket = false
			}
		}
	}

	// has the columns that not belong to this table, so memo.SelectExpr can not execute in ts engine
	if !source.Relational().OuterCols.Empty() {
		if !hasAddSynchronizer {
			source.Input.SetAddSynchronizer()
		}
		return false, true, false, nil
	}

	// all condition can execute in ts engine
	if childExecInTS {
		if selfExecInTS {
			source.SetEngineTS()
		} else if !hasAddSynchronizer {
			source.Input.SetAddSynchronizer()
			hasAddSynchronizer = true
		}
	}

	return selfExecInTS, hasAddSynchronizer, optTimeBucket, nil
}

// checkProject check if memo.ProjectExpr can execute in ts engine.
// source is the memo.ProjectExpr of memo tree.
// returns:
// param1: return true when the memo.ProjectExpr can execute in ts engine.
// param2: return true when the child of memo.ProjectExpr is set the addSynchronizer to true.
// param3: return true when optimizing query efficiency in time_bucket case.
// param4: is the error.
func (m *Memo) checkProject(source *ProjectExpr) (bool, bool, bool, error) {
	childExecInTS, hasAddSynchronizer, optTimeBucket, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
	if err != nil {
		return false, false, false, err
	}

	var param GetSubQueryExpr
	param.m = m
	selfExecInTS := childExecInTS
	for _, proj := range source.Projections {
		proj.Walk(&param)
		if param.hasSub {
			selfExecInTS = false
		}
		if childExecInTS {
			// check if element of ProjectionExpr can execute in ts engine.
			if execInTSEngine, hashcode := CheckExprCanExecInTSEngine(proj.Element.(opt.Expr), ExprPosProjList,
				m.CheckHelper.whiteList.CheckWhiteListParam, false); execInTSEngine {
				m.AddColumn(proj.Col, "", GetExprType(proj.Element), ExprPosProjList, hashcode, false)
			} else {
				selfExecInTS = false
			}
		}

		// case: check if the element is time_bucket function when where need optimize time_bucket.
		if optTimeBucket {
			if proj.Element.Op() == opt.FunctionOp {
				f := proj.Element.(*FunctionExpr)
				if f.Name != tree.FuncTimeBucket {
					optTimeBucket = false
				} else {
					if v, ok := m.CheckHelper.pushHelper.Find(proj.Col); ok {
						m.AddColumn(proj.Col, v.Alias, v.Type, v.Pos, v.Hash, true)
					}
				}
			} else {
				optTimeBucket = false
			}
		}
	}

	if selfExecInTS {
		source.SetEngineTS()
	} else {
		if childExecInTS && !hasAddSynchronizer {
			source.Input.SetAddSynchronizer()
			hasAddSynchronizer = true
		}
	}

	return selfExecInTS, hasAddSynchronizer, optTimeBucket, nil
}

// checkGrouping check if group cols can execute in ts engine.
// cols is the GroupingCols of (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr).
// optTimeBucket is true when optimizing query efficiency in time_bucket case,
// optTimeBucket will set true when only group by time_bucket or primary tag column.
func (m *Memo) checkGrouping(cols opt.ColSet, optTimeBucket *bool) bool {
	execInTSEngine := true
	cols.ForEach(func(colID opt.ColumnID) {
		colMeta := m.metadata.ColumnMeta(colID)
		if !m.CheckExecInTS(colID, ExprPosGroupBy) || colMeta.Type.Family() == types.BytesFamily {
			execInTSEngine = false
		}
		if v, ok := m.CheckHelper.pushHelper.Find(colID); ok {
			if !v.IsTimeBucket && !m.metadata.ColumnMeta(colID).IsPrimaryTag() {
				*optTimeBucket = false
			}
		} else {
			*optTimeBucket = false
		}
	})
	if cols.Empty() && (*optTimeBucket) {
		*optTimeBucket = false
	}

	return execInTSEngine
}

// checkChildExecInTS return true if the child of agg can execute in ts engine.
// srcExpr is the expr of agg
func (m *Memo) checkChildExecInTS(srcExpr opt.ScalarExpr, hashCode uint32) bool {
	execInTSEngine := false
	if srcExpr.ChildCount() == 0 {
		execInTSEngine = m.CheckHelper.whiteList.CheckWhiteListAll(hashCode, ExprPosProjList, uint32(ExprTypConst))
	} else {
		for j := 0; j < srcExpr.ChildCount(); j++ {
			// case agg(column)
			val, ok := srcExpr.Child(j).(*VariableExpr)
			if ok {
				execInTSEngine = m.CheckExecInTS(val.Col, ExprPosProjList)
				if !execInTSEngine {
					break
				}
				continue
			}

			// case agg(distinct column)
			aggDistinct, ok1 := srcExpr.(*AggDistinctExpr)
			if ok1 {
				execInTSEngine = m.checkChildExecInTS(aggDistinct.Input, hashCode)
			}
		}
	}
	return execInTSEngine
}

// checkParallelAgg check agg can parallel execute.
// expr is the agg function expr.
// returns:
// param1: return true when the agg can be parallel execute.
// param2: return true when the agg with distinct.
func checkParallelAgg(expr opt.Expr) (bool, bool) {
	switch t := expr.(type) {
	case *MaxExpr, *MinExpr, *SumExpr, *AvgExpr, *CountExpr, *CountRowsExpr,
		*FirstExpr, *FirstRowExpr, *FirstTimeStampExpr, *FirstRowTimeStampExpr,
		*LastExpr, *LastRowExpr, *LastTimeStampExpr, *LastRowTimeStampExpr, *ConstAggExpr:
		return true, false
	case *AggDistinctExpr:
		ok, _ := checkParallelAgg(t.Input)
		return ok, true
	}
	return false, false
}

// checkGroupBy check if memo.SelectExpr can execute in ts engine.
// input is the child of (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr) of memo tree.
// aggs is the AggregationsExpr of (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr).
// gp is the GroupingPrivate of (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr).
// returns:
// param1: return true when the (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr) can execute in ts engine.
// param2: return true when the (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr) can parallel in ts engine.
// param3: return true when the agg functions with distinct.
// param4: return true when the input of (memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr) is set the addSynchronizer to true.
// param5: return true when optimizing query efficiency in time_bucket case.
// param6: is the error.
func (m *Memo) checkGroupBy(
	input RelExpr, aggs *AggregationsExpr, gp *GroupingPrivate,
) (bool, bool, bool, bool, bool, error) {
	isParallel := true
	execInTSEngine, hasSynchronizer, optTimeBucket, err := m.CheckWhiteListAndAddSynchronizeImp(&input)
	if err != nil || m.CheckHelper.GroupHint == keys.ForceRelationalGroup {
		// case: error or hint force group by can not execute in ts engine.
		return false, false, false, false, false, err
	}

	// memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr
	// should not parallel when the rows less than ten hundred.
	if input.Relational().Stats.RowCount < 1000 && !optTimeBucket && m.CheckHelper.GroupHint != keys.ForceAEGroup {
		if execInTSEngine && !hasSynchronizer {
			input.SetAddSynchronizer()
			hasSynchronizer = true
		}
	}

	aggExecParallel, hasDistinct := false, false

	m.checkOptTimeBucketFlag(input, &optTimeBucket)

	if execInTSEngine {
		// check if group cols can execute in ts engine
		execInTSEngine = m.checkGrouping(gp.GroupingCols, &optTimeBucket)
		if execInTSEngine {
			// case: child of memo.GroupByExpr or memo.ScalarGroupByExpr or memo.DistinctOnExpr and group cols can execute in ts engine
			// then check if the aggs can execute in ts engine
			for i := 0; i < len(*aggs); i++ {
				srcExpr := (*aggs)[i].Agg
				hashCode := GetExprHash(srcExpr)

				// first: check if child of agg can execute in ts engine.
				// second: check if agg itself can execute in ts engine.
				if !m.checkChildExecInTS(srcExpr, hashCode) || !m.CheckHelper.whiteList.CheckWhiteListParam(hashCode, ExprPosProjList) {
					m.setSynchronizerForChild(input, &hasSynchronizer)
					return false, false, false, hasSynchronizer, false, nil
				}
				m.AddColumn((*aggs)[i].Col, "", ExprTypeAggOp, ExprPosGroupBy, hashCode, false)
				var aggWithDistinct bool
				aggExecParallel, aggWithDistinct = checkParallelAgg((*aggs)[i].Agg)
				if aggWithDistinct {
					hasDistinct = true
					aggExecParallel = false
				}
				isParallel = isParallel && aggExecParallel
			}
			// case: group by can execute in ts engine, but agg can not Parallel.
			if !isParallel && !hasSynchronizer {
				m.setSynchronizerForChild(input, &hasSynchronizer)
			}
		} else {
			// case: group cols can not execute in ts engine.
			if !hasSynchronizer {
				m.setSynchronizerForChild(input, &hasSynchronizer)
			}
		}

		return execInTSEngine, isParallel, hasDistinct, hasSynchronizer, optTimeBucket, nil
	}

	return false, false, hasDistinct, hasSynchronizer, optTimeBucket, nil
}

// setSynchronizerForChild add flag for the child of memo.OrderBy in OrderGroupBy case,
// otherwise , add flag for the child of (memo.GroupByExpr or memo.ScalarGroupByExpr).
// child is the child of (memo.GroupByExpr or memo.ScalarGroupByExpr).
// hasSynchronizer is true when the child have added the flag.
func (m *Memo) setSynchronizerForChild(child RelExpr, hasSynchronizer *bool) {
	if m.CheckFlag(opt.OrderGroupBy) {
		// only single node, order by can exec in ts engine.
		if m.CheckFlag(opt.SingleMode) || m.CheckHelper.isSingleNode() {
			child.SetEngineTS()
		}
		child.Child(0).(RelExpr).SetAddSynchronizer()
	} else {
		child.SetAddSynchronizer()
	}
	*hasSynchronizer = true
}

// checkJoin check if memo.InnerJoinExpr can execute in ts engine.
// join can not execute in ts engine, so just check child of InnerJoinExpr and add synchronize Expr.
// source is the memo.InnerJoinExpr of memo tree.
// returns:
// param1: the memo.InnerJoinExpr can not execute in ts engine, always false.
// param2: return true when the child of memo.InnerJoinExpr is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) checkJoin(source *InnerJoinExpr) (bool, bool, error) {
	execInTSEngineL, hasAddSynchronizerL, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Left)
	if err != nil {
		return false, false, err
	}

	execInTSEngineR, hasAddSynchronizerR, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Right)
	if err != nil {
		return false, false, err
	}

	if execInTSEngineL && !hasAddSynchronizerL {
		source.Left.SetAddSynchronizer()
		hasAddSynchronizerL = true
	}

	if execInTSEngineR && !hasAddSynchronizerR {
		source.Right.SetAddSynchronizer()
		hasAddSynchronizerR = true
	}

	return false, hasAddSynchronizerL || hasAddSynchronizerR, nil
}

// checkSetop check if (UnionAllExpr, UnionExpr, IntersectExpr,
// IntersectAllExpr, ExceptAllExpr, ExceptExpr) can execute in ts engine.
// They can not execute in ts engine, check their child
// left, right: childs of setop expr
// returns:
// param1: can not execute in ts engine, always false.
// param2: return true when the child of (UnionAllExpr, UnionExpr,
// IntersectExpr,IntersectAllExpr, ExceptAllExpr, ExceptExpr) is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) checkSetop(left, right RelExpr) (bool, bool, error) {
	execInTSEngineL, hasAddSynchronizerL, _, err := m.CheckWhiteListAndAddSynchronizeImp(&left)
	if err != nil {
		return false, false, err
	}

	execInTSEngineR, hasAddSynchronizerR, _, err := m.CheckWhiteListAndAddSynchronizeImp(&right)
	if err != nil {
		return false, false, err
	}

	if execInTSEngineL && !hasAddSynchronizerL {
		left.SetAddSynchronizer()
		hasAddSynchronizerL = true
	}

	if execInTSEngineR && !hasAddSynchronizerR {
		right.SetAddSynchronizer()
		hasAddSynchronizerR = true
	}

	return false, hasAddSynchronizerL || hasAddSynchronizerR, nil
}

// checkOtherJoinChildExpr check if the child of (SemiJoinExpr,MergeJoinExpr,LeftJoinApplyExpr,
// RightJoinExpr,InnerJoinApplyExpr,FullJoinExpr,LeftJoinExpr) can execute in ts engine.
// left is the left child of memo.**JoinExpr, right is the right child of memo.**JoinExpr.
// returns:
// param1: the memo.**JoinExpr can not execute in ts engine, always false.
// param2: return true when the child of memo.**JoinExpr is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) checkOtherJoinChildExpr(left, right RelExpr) (bool, bool, error) {
	execInTSEngineL, hasAddSynchronizerL, _, err := m.CheckWhiteListAndAddSynchronizeImp(&left)
	if err != nil {
		return false, false, err
	}

	execInTSEngineR, hasAddSynchronizerR, _, err := m.CheckWhiteListAndAddSynchronizeImp(&right)
	if err != nil {
		return false, false, err
	}

	if execInTSEngineL && !hasAddSynchronizerL {
		left.SetAddSynchronizer()
		hasAddSynchronizerL = true
	}

	if execInTSEngineR && !hasAddSynchronizerR {
		right.SetAddSynchronizer()
		hasAddSynchronizerR = true
	}

	return false, hasAddSynchronizerL || hasAddSynchronizerR, nil
}

// checkOtherJoin check if (SemiJoinExpr,MergeJoinExpr,LeftJoinApplyExpr,RightJoinExpr,InnerJoinApplyExpr,FullJoinExpr,LeftJoinExpr)
// can execute in ts engine. they can not execute in ts engine, so just check their child node and add synchronize Expr.
// source is the memo.**JoinExpr of memo tree.
// returns:
// param1: the memo.**JoinExpr can not execute in ts engine, always false.
// param2: return true when the child of memo.**JoinExpr is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) checkOtherJoin(
	source RelExpr,
) (execInTSEngine bool, hasAddSynchronizer bool, err error) {
	switch s := source.(type) {
	case *SemiJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *SemiJoinApplyExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *MergeJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *LeftJoinApplyExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *RightJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *InnerJoinApplyExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *FullJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *LeftJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *AntiJoinExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	case *AntiJoinApplyExpr:
		execInTSEngine, hasAddSynchronizer, err = m.checkOtherJoinChildExpr(s.Left, s.Right)
	}
	if err != nil {
		return false, hasAddSynchronizer, err
	}
	return false, hasAddSynchronizer, nil
}

// checkLookupJoin check if memo.LookupJoinExpr can execute in ts engine.
// join can not execute in ts engine, so just check child of LookupJoinExpr and add synchronize Expr.
// source is the memo.LookupJoinExpr of memo tree.
// returns:
// param1: the memo.LookupJoinExpr can not execute in ts engine, always false.
// param2: return true when the child of memo.LookupJoinExpr is set the addSynchronizer to true.
// param3: is the error.
func (m *Memo) checkLookupJoin(source *LookupJoinExpr) (bool, bool, error) {
	execInTSEngine, hasAddSynchronizer, _, err := m.CheckWhiteListAndAddSynchronizeImp(&source.Input)
	if err != nil {
		return false, false, err
	}
	if execInTSEngine && !hasAddSynchronizer {
		source.Input.SetAddSynchronizer()
		hasAddSynchronizer = true
	}
	return false, hasAddSynchronizer, nil
}

// checkFilterOptTimeBucket check if only timestamp col in filter.
// Only in this way can time_bucket optimization be used.
// expr is filter expr.
func (m *Memo) checkFilterOptTimeBucket(expr opt.Expr) bool {
	switch expr.Op() {
	case opt.AndOp, opt.OrOp, opt.RangeOp:
		for i := 0; i < expr.ChildCount(); i++ {
			if !m.checkFilterOptTimeBucket(expr.Child(i)) {
				return false
			}
		}
		return true
	case opt.EqOp, opt.GeOp, opt.GtOp, opt.LeOp, opt.LtOp, opt.NeOp:
		for i := 0; i < expr.ChildCount(); i++ {
			if expr.Child(i).Op() == opt.VariableOp {
				v := expr.Child(i).(*VariableExpr)
				tableID := m.Metadata().ColumnMeta(v.Col).Table
				if v.Col == tableID.ColumnID(0) {
					return true
				}
			}
		}
	}
	return false
}

// checkOptTimeBucketFlag set optTimeBucket = false when haven't time_bucket, should not use special operator.
// input is the child expr of group by expr
func (m *Memo) checkOptTimeBucketFlag(input RelExpr, optTimeBucket *bool) {
	checkProject := func(pro *ProjectExpr) {
		for _, v := range pro.Projections {
			if tb, ok := m.CheckHelper.pushHelper.Find(v.Col); ok {
				if !tb.IsTimeBucket {
					*optTimeBucket = false
				}
			} else {
				*optTimeBucket = false
			}
		}
	}
	if project, ok := input.(*ProjectExpr); ok {
		checkProject(project)
	} else if sort, ok := input.(*SortExpr); ok {
		if project, ok := sort.Input.(*ProjectExpr); ok {
			checkProject(project)
		} else {
			*optTimeBucket = false
		}
	} else {
		*optTimeBucket = false
	}
}

// CalculateDop is used to calculate degree dynamically based on statistics
// rowCount is the RowCount of memo.TSScanExpr.
// pTagCount is the PTagCount of memo.TSScanExpr.
// allColsWidth is width of all columns.
func (m *Memo) CalculateDop(rowCount float64, pTagCount float64, allColsWidth uint32) {
	var parallelNum uint32

	// Gets the number of cores for the cpu
	cpuCores := runtime.NumCPU()

	// Adjust the parallel num based on the amount of data
	switch {
	case rowCount <= sqlbase.LowDataThreshold:
		parallelNum = sqlbase.MaxDopForLowData
	case rowCount < sqlbase.HighDataThreshold:
		scaleFactor := (rowCount - sqlbase.LowDataThreshold) / (sqlbase.HighDataThreshold - sqlbase.LowDataThreshold)
		parallelNum = uint32(2 + scaleFactor*(pTagCount-2))
		if parallelNum > sqlbase.MaxDopForHighData {
			parallelNum = sqlbase.MaxDopForHighData
		}
	default:
		// If the number of rows exceeds the high threshold, set the parallel num to the number of PTags
		parallelNum = uint32(pTagCount)
	}

	// Adjust the parallel num based on the wait memory and thread
	v, err := mem.VirtualMemory()
	if err != nil {
		m.SetTsDop(sqlbase.DefaultDop)
		return
	}
	availableRAM := v.Free
	eachParallelMemory := (rowCount / float64(parallelNum)) * float64(allColsWidth)
	maxParallelNum := float64(availableRAM) / eachParallelMemory

	// The parallel num cannot exceed the minimum number of devices and CPU cores
	parallelNum = uint32(math.Min(float64(parallelNum), math.Min(float64(cpuCores), maxParallelNum)))
	if parallelNum > m.tsDop {
		m.tsDop = parallelNum
	}
}

// GetTsDop is used to get degree of parallelism
func (m *Memo) GetTsDop() uint32 {
	return m.tsDop
}

// SetTsDop is used to set degree of parallelism
func (m *Memo) SetTsDop(num uint32) {
	m.tsDop = num
}
