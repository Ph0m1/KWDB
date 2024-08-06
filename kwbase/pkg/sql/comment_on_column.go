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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type commentOnColumnNode struct {
	n         *tree.CommentOnColumn
	tableDesc *ImmutableTableDescriptor
}

// CommentOnColumn add comment on a column.
// Privileges: CREATE on table.
func (p *planner) CommentOnColumn(ctx context.Context, n *tree.CommentOnColumn) (planNode, error) {
	var tableName tree.TableName
	if n.ColumnItem.TableName != nil {
		tableName = n.ColumnItem.TableName.ToTableName()
	}
	tableDesc, err := p.ResolveUncachedTableDescriptor(ctx, &tableName, true, ResolveRequireTableDesc)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &commentOnColumnNode{n: n, tableDesc: tableDesc}, nil
}

func (n *commentOnColumnNode) startExec(params runParams) error {
	col, _, err := n.tableDesc.FindColumnByName(n.n.ColumnItem.ColumnName)
	if err != nil {
		return err
	}

	if n.n.Comment != nil {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"set-column-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
			keys.ColumnCommentType,
			n.tableDesc.ID,
			col.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		_, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
			params.ctx,
			"delete-column-comment",
			params.p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
			keys.ColumnCommentType,
			n.tableDesc.ID,
			col.ID)
		if err != nil {
			return err
		}
	}
	return nil
	// FIXME: comment support
	// return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
	// 	params.ctx,
	// 	params.p.txn,
	// 	EventLogCommentOnColumn,
	// 	int32(n.tableDesc.ID),
	// 	int32(params.extendedEvalCtx.NodeID),
	// 	params.p.curPlan.auditInfo.Info,
	// )
}

func (n *commentOnColumnNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnColumnNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnColumnNode) Close(context.Context)        {}
