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

// AlterTSDatabase represents an ALTER TS DATABASE statement.
type AlterTSDatabase struct {
	Database          Name
	LifeTime          *TimeInput
	PartitionInterval *TimeInput
}

// Format implements the NodeFormatter interface
func (node *AlterTSDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TS DATABASE SET")
	if node.LifeTime != nil {
		ctx.WriteString(" LIFETIME = ")
		ctx.FormatNode(node.LifeTime)
	}
	if node.PartitionInterval != nil {
		ctx.WriteString(" PARTITION INTERVAL = ")
		ctx.FormatNode(node.PartitionInterval)
	}
}

var _ Statement = &AlterTSDatabase{}
