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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

type dropAuditNode struct {
	names    []string
	ifExists bool
}

// DropAudit creates a DropAudit planNode.
func (p *planner) DropAudit(ctx context.Context, n *tree.DropAudit) (planNode, error) {
	if isAdmin, err := p.HasAdminRole(ctx); !isAdmin {
		if err != nil {
			return nil, err
		}
		return nil, errors.Errorf("%s is not superuser or membership of admin, has no privilege to DROP AUDIT",
			p.User())
	}

	return &dropAuditNode{
		names:    n.Names.ToStrings(),
		ifExists: n.IfExists,
	}, nil
}

func (n *dropAuditNode) startExec(params runParams) error {
	for _, name := range n.names {
		rowsAffected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.Exec(
			params.ctx,
			"drop-audit",
			params.p.txn,
			`DELETE FROM system.audits WHERE audit_name = $1`,
			name,
		)
		if err != nil {
			return err
		}
		if rowsAffected == 0 && !n.ifExists {
			return errors.Errorf("audit %s does not exists", name)
		}
		params.p.SetAuditTarget(0, name, nil)
	}

	if err := params.p.BumpAuditsTableVersion(params.ctx); err != nil {
		return err
	}

	return nil
}

func (*dropAuditNode) Next(params runParams) (bool, error) { return false, nil }

func (*dropAuditNode) Values() tree.Datums { return nil }

func (*dropAuditNode) Close(ctx context.Context) {}

func (*dropAuditNode) SetUpsert() error { return nil }
