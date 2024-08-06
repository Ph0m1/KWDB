// Copyright 2012, Google Inc. All rights reserved.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package tree

import "gitee.com/kwbasedb/kwbase/pkg/sql/privilege"

// Revoke represents a REVOKE statement.
// PrivilegeList and TargetList are defined in grant.go
type Revoke struct {
	Privileges privilege.List
	Targets    TargetList
	Grantees   NameList
}

// Format implements the NodeFormatter interface.
func (node *Revoke) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	node.Privileges.Format(&ctx.Buffer)
	ctx.WriteString(" ON ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Grantees)
}

// RevokeRole represents a REVOKE <role> statement.
type RevokeRole struct {
	Roles       NameList
	Members     NameList
	AdminOption bool
}

// Format implements the NodeFormatter interface.
func (node *RevokeRole) Format(ctx *FmtCtx) {
	ctx.WriteString("REVOKE ")
	if node.AdminOption {
		ctx.WriteString("ADMIN OPTION FOR ")
	}
	ctx.FormatNode(&node.Roles)
	ctx.WriteString(" FROM ")
	ctx.FormatNode(&node.Members)
}
