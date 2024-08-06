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

package optbuilder

import (
	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

func (b *Builder) buildExplain(explain *tree.Explain, inScope *scope) (outScope *scope) {
	b.pushWithFrame()

	// We don't allow the statement under Explain to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	stmtScope := b.buildStmtAtRoot(explain.Statement, nil /* desiredTypes */, b.allocScope())

	b.popWithFrame(stmtScope)
	outScope = inScope.push()

	var cols sqlbase.ResultColumns
	switch explain.Mode {
	case tree.ExplainPlan:
		telemetry.Inc(sqltelemetry.ExplainPlanUseCounter)
		if explain.Flags[tree.ExplainFlagVerbose] || explain.Flags[tree.ExplainFlagTypes] {
			cols = sqlbase.ExplainPlanVerboseColumns
		} else {
			cols = sqlbase.ExplainPlanColumns
		}

	case tree.ExplainDistSQL:
		analyze := explain.Flags[tree.ExplainFlagAnalyze]
		if analyze {
			telemetry.Inc(sqltelemetry.ExplainAnalyzeUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainDistSQLUseCounter)
		}
		if analyze && tree.IsStmtParallelized(explain.Statement) {
			panic(pgerror.Newf(pgcode.FeatureNotSupported,
				"EXPLAIN ANALYZE does not support RETURNING NOTHING statements"))
		}
		cols = sqlbase.ExplainDistSQLColumns

	case tree.ExplainOpt:
		if explain.Flags[tree.ExplainFlagVerbose] {
			telemetry.Inc(sqltelemetry.ExplainOptVerboseUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainOptUseCounter)
		}
		cols = sqlbase.ExplainOptColumns

	case tree.ExplainVec:
		telemetry.Inc(sqltelemetry.ExplainVecUseCounter)
		cols = sqlbase.ExplainVecColumns

	default:
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"EXPLAIN ANALYZE does not support RETURNING NOTHING statements"))
	}
	b.synthesizeResultColumns(outScope, cols)

	input := stmtScope.expr
	private := memo.ExplainPrivate{
		Options:  explain.ExplainOptions,
		ColList:  colsToColList(outScope.cols),
		Props:    stmtScope.makePhysicalProps(),
		StmtType: explain.Statement.StatementType(),
	}
	outScope.expr = b.factory.ConstructExplain(input, &private)
	return outScope
}
