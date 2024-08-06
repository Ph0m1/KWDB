// Copyright 2020 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
)

// RevokeRoleNode removes entries from the system.role_members table.
// This is called from REVOKE <ROLE>
type RevokeRoleNode struct {
	roles       tree.NameList
	members     tree.NameList
	adminOption bool

	run revokeRoleRun
}

type revokeRoleRun struct {
	rowsAffected int
}

// RevokeRole represents a GRANT ROLE statement.
func (p *planner) RevokeRole(ctx context.Context, n *tree.RevokeRole) (planNode, error) {
	return p.RevokeRoleNode(ctx, n)
}

func (p *planner) RevokeRoleNode(ctx context.Context, n *tree.RevokeRole) (*RevokeRoleNode, error) {
	sqltelemetry.IncIAMRevokeCounter(n.AdminOption)

	ctx, span := tracing.ChildSpan(ctx, n.StatementTag(), p.GetNodeIDNumber())
	defer tracing.FinishSpan(span)

	hasAdminRole, err := p.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	// check permissions on each role.
	allRoles, err := p.MemberOfWithAdminOption(ctx, p.User())
	if err != nil {
		return nil, err
	}
	for _, r := range n.Roles {
		// If the user is an admin, don't check if the user is allowed to add/drop
		// roles in the role. However, if the role being modified is the admin role, then
		// make sure the user is an admin with the admin option.
		if hasAdminRole && string(r) != sqlbase.AdminRole {
			continue
		}
		if isAdmin, ok := allRoles[string(r)]; !ok || !isAdmin {
			errMsg := "%s is not a superuser or role admin for role %s"
			if string(r) == sqlbase.AdminRole {
				errMsg = "%s is not a role admin for role %s"
			}
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege, errMsg, p.User(), r)
		}
	}

	// Check that roles exist.
	// TODO(mberhault): just like GRANT/REVOKE privileges, we fetch the list of all roles.
	// This is wasteful when we have a LOT of roles compared to the number of roles being operated on.
	roles, err := p.GetAllRoles(ctx)
	if err != nil {
		return nil, err
	}

	for _, r := range n.Roles {
		if _, ok := roles[string(r)]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", r)
		}
	}

	for _, m := range n.Members {
		if _, ok := roles[string(m)]; !ok {
			return nil, pgerror.Newf(pgcode.UndefinedObject, "role/user %s does not exist", m)
		}
	}

	return &RevokeRoleNode{
		roles:       n.Roles,
		members:     n.Members,
		adminOption: n.AdminOption,
	}, nil
}

func (n *RevokeRoleNode) startExec(params runParams) error {
	opName := "revoke-role"

	var memberStmt string
	if n.adminOption {
		// ADMIN OPTION FOR is specified, we don't remove memberships just remove the admin option.
		memberStmt = `UPDATE system.role_members SET "isAdmin" = false WHERE "role" = $1 AND "member" = $2`
	} else {
		// Admin option not specified: remove membership if it exists.
		memberStmt = `DELETE FROM system.role_members WHERE "role" = $1 AND "member" = $2`
	}

	var rowsAffected int

	for _, r := range n.roles {
		for _, m := range n.members {
			if string(r) == sqlbase.AdminRole && string(m) == security.RootUser {
				// We use CodeObjectInUseError which is what happens if you tried to delete the current user in pg.
				return pgerror.Newf(pgcode.ObjectInUse,
					"role/user %s cannot be removed from role %s or lose the ADMIN OPTION",
					security.RootUser, sqlbase.AdminRole)
			}
			affected, err := params.extendedEvalCtx.ExecCfg.InternalExecutor.ExecEx(
				params.ctx,
				opName,
				params.p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
				memberStmt,
				r, m,
			)
			if err != nil {
				return err
			}

			rowsAffected += affected
		}
		params.p.SetAuditTarget(0, string(r), nil)
	}

	// We need to bump the table version to trigger a refresh if anything changed.
	if rowsAffected > 0 {
		if err := params.p.BumpRoleMembershipTableVersion(params.ctx); err != nil {
			return err
		}
	}

	n.run.rowsAffected += rowsAffected

	return nil
}

// Next implements the planNode interface.
func (*RevokeRoleNode) Next(runParams) (bool, error) { return false, nil }

// Values implements the planNode interface.
func (*RevokeRoleNode) Values() tree.Datums { return tree.Datums{} }

// Close implements the planNode interface.
func (*RevokeRoleNode) Close(context.Context) {}
