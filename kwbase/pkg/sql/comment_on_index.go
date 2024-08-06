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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

type commentOnIndexNode struct {
	n         *tree.CommentOnIndex
	tableDesc *sqlbase.TableDescriptor
	indexDesc *sqlbase.IndexDescriptor
}

// CommentOnIndex adds a comment on an index.
// Privileges: CREATE on table.
func (p *planner) CommentOnIndex(ctx context.Context, n *tree.CommentOnIndex) (planNode, error) {
	tableDesc, indexDesc, err := p.getTableAndIndex(ctx, &n.Index, privilege.CREATE)
	if err != nil {
		return nil, err
	}

	return &commentOnIndexNode{n: n, tableDesc: tableDesc.TableDesc(), indexDesc: indexDesc}, nil
}

func (n *commentOnIndexNode) startExec(params runParams) error {
	if n.n.Comment != nil {
		err := params.p.upsertIndexComment(
			params.ctx,
			n.tableDesc.ID,
			n.indexDesc.ID,
			*n.n.Comment)
		if err != nil {
			return err
		}
	} else {
		err := params.p.removeIndexComment(params.ctx, n.tableDesc.ID, n.indexDesc.ID)
		if err != nil {
			return err
		}
	}
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCommentOnIndex,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID),
		struct {
			TableName string
			IndexName string
			Statement string
			User      string
			Comment   *string
		}{
			n.tableDesc.Name,
			string(n.n.Index.Index),
			n.n.String(),
			params.SessionData().User,
			n.n.Comment},
	)

}

func (p *planner) upsertIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID, comment string,
) error {
	_, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"set-index-comment",
		p.Txn(),
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
		keys.IndexCommentType,
		tableID,
		indexID,
		comment)

	return err
}

func (p *planner) removeIndexComment(
	ctx context.Context, tableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	_, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-index-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
		keys.IndexCommentType,
		tableID,
		indexID)

	return err
}

func (n *commentOnIndexNode) Next(runParams) (bool, error) { return false, nil }
func (n *commentOnIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (n *commentOnIndexNode) Close(context.Context)        {}
