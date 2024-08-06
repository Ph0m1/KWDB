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

import (
	"encoding/hex"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// HintForest is the top-level encapsulation of HintTree.
type HintForest []HintTree

// HintTree represents a HintInfo message.
type HintTree HintTreeNode

// Reset is a part of the Message interface.
func (h HintForest) Reset() {
	panic("HintForest error")
}

// String is a part of the Message interface.
func (h HintForest) String() string {
	var builder strings.Builder
	for _, hintTree := range h {
		switch node := hintTree.(type) {
		case *HintTreeScanNode:
			fmt.Fprintf(&builder, "%s\n", node.FormatNode())
		case *HintTreeJoinNode:
			fmt.Fprintf(&builder, "%s\n", node.FormatNode())
		}
	}
	return builder.String()
}

// ProtoMessage is a part of the Message interface.
func (h HintForest) ProtoMessage() {
	panic("implement me")
}

// GetGroupHint return group hint.
func (h HintForest) GetGroupHint() keys.GroupHintType {
	for _, hintTree := range h {
		switch node := hintTree.(type) {
		case *HintTreeGroupNode:
			return node.HintType
		}
	}
	return keys.NoGroupHint
}

// HintTreeNode implement external hint.
type HintTreeNode interface {
	HintTreeNodeType() string
	FormatNode() string
}

// HintTreeScanNode implement scan hint.
type HintTreeScanNode struct {
	HintType             keys.ScanMethodHintType
	IndexName            []string
	TableName            Name
	TotalCardinality     float64
	EstimatedCardinality float64
	LeadingTable         bool
}

// HintTreeNodeType return external ScanHint.
func (node *HintTreeScanNode) HintTreeNodeType() string {
	return "ScanHint"
}

// FormatNode return HintTreeScanNode.
func (node *HintTreeScanNode) FormatNode() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "ScanNode( \n\tHintType: %s,\n\ttablename: %s,\n\tindexname: [", node.HintType, node.TableName)
	for i, index := range node.IndexName {
		fmt.Fprintf(&builder, "%s", index)
		if i < len(node.IndexName)-1 {
			fmt.Fprintf(&builder, ", ")
		}
	}
	fmt.Fprintf(&builder, "],\n\tEstCardinality: %f,\n\tTotalCardinality: %f,", node.EstimatedCardinality, node.TotalCardinality)
	if node.LeadingTable {
		fmt.Fprintf(&builder, "\n\tLeadingTable: true,")
	}
	fmt.Fprintf(&builder, "\n)")
	return builder.String()
}

// HintTreeJoinNode implement external join hint
type HintTreeJoinNode struct {
	HintType             keys.JoinMethodHintType
	Left                 HintTreeNode
	Right                HintTreeNode
	RealJoinOrder        bool
	IndexName            string
	TotalCardinality     float64
	EstimatedCardinality float64
	LeadingTable         bool
}

// HintTreeNodeType return JoinNode.
func (node *HintTreeJoinNode) HintTreeNodeType() string {
	return "JoinNode"
}

// FormatNode return HintTreeJoinNode.
func (node *HintTreeJoinNode) FormatNode() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "JoinNode( \n\tHintType: %s,", node.HintType)
	if node.RealJoinOrder {
		fmt.Fprintf(&builder, "\n\tRealJoinOrder: true,")
	}
	fmt.Fprintf(&builder, "\n\tLeftNode: %s,", strings.Replace(node.Left.FormatNode(), "\n", "\n\t", -1))
	fmt.Fprintf(&builder, "\n\tRightNode: %s,", strings.Replace(node.Right.FormatNode(), "\n", "\n\t", -1))

	if node.IndexName != "" {
		fmt.Fprintf(&builder, "\n\tindexname: %s,", node.IndexName)
	}
	fmt.Fprintf(&builder, "\n\tEstCardinality: %f,\n\tTotalCardinality: %f,", node.EstimatedCardinality, node.TotalCardinality)

	fmt.Fprintf(&builder, "\n)")
	return builder.String()
}

// HintHex2go convert the protobuf form of the converted Hint to HintForest.
func HintHex2go(hintinfo string) (HintForest, error) {
	DecodeHintInfo, err := hex.DecodeString(hintinfo)
	if err != nil {
		return nil, err
	}
	return HintProto2go(DecodeHintInfo)
}

// HintProto2go convert the protobuf form of Hint to HintForest.
func HintProto2go(protoHint []byte) (HintForest, error) {
	queryHintTree := &roachpb.QueryHintTree{}
	err := protoutil.Unmarshal(protoHint, queryHintTree)
	if err != nil {
		return nil, err
	}

	var hintForest HintForest
	accroot := queryHintTree.RootAccessNode
	joinroot := queryHintTree.RootJoinNode
	for _, v := range accroot {
		scanNode := scanProto2Go(v)
		hintForest = append(hintForest, scanNode)
	}
	for _, v := range joinroot {
		joinNode := joinProto2Go(v)
		hintForest = append(hintForest, joinNode)
	}

	return hintForest, nil
}

// scanProto2Go convert Protobuf form Hint's AccessiNode to HintTreeScanNode.
func scanProto2Go(node *roachpb.AccessNode) (hintTreeScanNode *HintTreeScanNode) {
	var scanMethod keys.ScanMethodHintType
	switch node.AccessMethod {
	case roachpb.AccessNode_USE_TABLE_SCAN:
		scanMethod = keys.UseTableScan
	case roachpb.AccessNode_IGNORE_TABLE_SCAN:
		scanMethod = keys.IgnoreTableScan
	case roachpb.AccessNode_FORCE_TABLE_SCAN:
		scanMethod = keys.ForceTableScan
	case roachpb.AccessNode_USE_INDEX_SCAN:
		scanMethod = keys.UseIndexScan
	case roachpb.AccessNode_IGNORE_INDEX_SCAN:
		scanMethod = keys.IgnoreIndexScan
	case roachpb.AccessNode_FORCE_INDEX_SCAN:
		scanMethod = keys.ForceIndexScan
	case roachpb.AccessNode_USE_INDEX_ONLY:
		scanMethod = keys.UseIndexOnly
	case roachpb.AccessNode_IGNORE_INDEX_ONLY:
		scanMethod = keys.IgnoreIndexOnly
	case roachpb.AccessNode_FORCE_INDEX_ONLY:
		scanMethod = keys.ForceIndexOnly
	default:
		scanMethod = keys.NoScanHint

	}
	hintTreeScanNode = &HintTreeScanNode{
		HintType:         scanMethod,
		IndexName:        node.IndexName,
		TableName:        Name(node.TableName),
		TotalCardinality: node.Cardinality,
		LeadingTable:     node.LeadingTable,
	}
	return hintTreeScanNode
}

// joinProto2Go convert the JoinNode of Protobuf form Hint to HintTreeJoinNode.
func joinProto2Go(node *roachpb.JoinNode) *HintTreeJoinNode {
	var joinMethod keys.JoinMethodHintType
	switch node.JoinMethod {
	case roachpb.JoinNode_USE_LOOKUP:
		joinMethod = keys.UseLookup
	case roachpb.JoinNode_DISALLOW_LOOKUP:
		joinMethod = keys.DisallowLookup
	case roachpb.JoinNode_FORCE_LOOKUP:
		joinMethod = keys.ForceLookup
	case roachpb.JoinNode_USE_MERGE:
		joinMethod = keys.UseMerge
	case roachpb.JoinNode_DISALLOW_MERGE:
		joinMethod = keys.DisallowMerge
	case roachpb.JoinNode_FORCE_MERGE:
		joinMethod = keys.ForceMerge
	case roachpb.JoinNode_USE_HASH:
		joinMethod = keys.UseHash
	case roachpb.JoinNode_DISALLOW_HASH:
		joinMethod = keys.DisallowHash
	case roachpb.JoinNode_FORCE_HASH:
		joinMethod = keys.ForceHash
	default:
		joinMethod = keys.NoJoinHint
	}
	var left, right HintTreeNode

	leftjoinNode := node.GetLeftJoinNode()
	if leftjoinNode != nil {
		left = joinProto2Go(leftjoinNode)
	} else {
		left = scanProto2Go(node.GetLeftAccessNode())
	}

	rightjoinNode := node.GetRightJoinNode()
	if rightjoinNode != nil {
		right = joinProto2Go(rightjoinNode)
	} else {
		right = scanProto2Go(node.GetRightAccessNode())
	}

	treeJoinNode := &HintTreeJoinNode{
		Left:             left,
		Right:            right,
		RealJoinOrder:    node.RealJoinOrder,
		HintType:         joinMethod,
		IndexName:        node.IndexName,
		TotalCardinality: node.Cardinality,
		LeadingTable:     node.LeadingTable,
	}
	return treeJoinNode
}

// HintTreeGroupNode  implement group hint.
type HintTreeGroupNode struct {
	HintType keys.GroupHintType
}

// HintTreeNodeType return GroupNode.
func (node *HintTreeGroupNode) HintTreeNodeType() string {
	return "GroupNode"
}

// FormatNode return HintTreeGroupNode.
func (node *HintTreeGroupNode) FormatNode() string {
	var builder strings.Builder

	fmt.Fprintf(&builder, "GroupNode( \n\tHintType: %v,", node.HintType)

	return builder.String()
}
