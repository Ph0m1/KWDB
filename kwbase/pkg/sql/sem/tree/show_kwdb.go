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

// ShowTags represents a SHOW TAGS statement.
type ShowTags struct {
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (node *ShowTags) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TAGS FROM ")
	ctx.FormatNode(node.Table)
}

// ShowRetentions represents a SHOW AUDITS statement.
type ShowRetentions struct {
	// Table Name
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (n *ShowRetentions) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW RETENTIONS ON TABLE ")
	ctx.FormatNode(n.Table)
}

// ShowTagValues represents a SHOW TAG VALUES statement.
type ShowTagValues struct {
	// Table Name
	Table *UnresolvedObjectName
}

// Format implements the NodeFormatter interface.
func (n *ShowTagValues) Format(ctx *FmtCtx) {
	ctx.WriteString("SHOW TAG VALUES FROM ")
	ctx.FormatNode(n.Table)
}
