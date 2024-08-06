// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package tree

// RoleType indicates edge node role type.
type RoleType uint8

const (
	// RoleTypeDefault default node
	RoleTypeDefault RoleType = iota
	// RoleTypePrimary primary node
	RoleTypePrimary
	// RoleTypeSecondary secondary node
	RoleTypeSecondary
)

// ReplicateSetRole represents a SET REPLICA ROLE statement.
type ReplicateSetRole struct {
	RoleType RoleType
}

// Format implements the NodeFormatter interface.
func (node *ReplicateSetRole) Format(ctx *FmtCtx) {
	ctx.WriteString("REPLICATE SET ROLE")
	switch node.RoleType {
	case RoleTypeDefault:
		ctx.WriteString(" DEFAULT")
	case RoleTypePrimary:
		ctx.WriteString(" PRIMARY")
	case RoleTypeSecondary:
		ctx.WriteString(" SECONDARY")
	}
}
