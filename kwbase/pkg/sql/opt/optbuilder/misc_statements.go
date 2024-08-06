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

package optbuilder

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func (b *Builder) buildControlJobs(n *tree.ControlJobs, inScope *scope) (outScope *scope) {
	if err := b.catalog.RequireAdminRole(b.ctx, n.StatementTag()); err != nil {
		panic(err)
	}

	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.Int}
	inputScope := b.buildStmt(n.Jobs, colTypes, emptyScope)

	checkInputColumns(
		fmt.Sprintf("%s JOBS", tree.JobCommandToStatement[n.Command]),
		inputScope,
		[]string{"job_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructControlJobs(
		inputScope.expr,
		&memo.ControlJobsPrivate{
			Props:   inputScope.makePhysicalProps(),
			Command: n.Command,
		},
	)
	return outScope
}

func (b *Builder) buildCancelQueries(n *tree.CancelQueries, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.String}
	inputScope := b.buildStmt(n.Queries, colTypes, emptyScope)

	checkInputColumns(
		"CANCEL QUERIES",
		inputScope,
		[]string{"query_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructCancelQueries(
		inputScope.expr,
		&memo.CancelPrivate{
			Props:    inputScope.makePhysicalProps(),
			IfExists: n.IfExists,
		},
	)
	return outScope
}

func (b *Builder) buildCancelSessions(n *tree.CancelSessions, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	colTypes := []*types.T{types.String}
	inputScope := b.buildStmt(n.Sessions, colTypes, emptyScope)

	checkInputColumns(
		"CANCEL SESSIONS",
		inputScope,
		[]string{"session_id"},
		colTypes,
		1, /* minPrefix */
	)
	outScope = inScope.push()
	outScope.expr = b.factory.ConstructCancelSessions(
		inputScope.expr,
		&memo.CancelPrivate{
			Props:    inputScope.makePhysicalProps(),
			IfExists: n.IfExists,
		},
	)
	return outScope
}
