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
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

func TestHintHex2go(t *testing.T) {
	defer leaktest.AfterTest(t)()
	//quertText := `SELECT
	//c_id, o_id, ol_number
	//FROM
	//customer c, tpcc.order o, order_line ol
	//WHERE
	//c.c_id=o.o_c_id
	//AND c.c_w_id=o.o_w_id
	//AND c.c_d_id=o.o_d_id
	//AND o.o_id=ol.ol_o_id
	//AND o.o_w_id=ol.ol_w_id
	//AND o.o_d_id=ol.ol_d_id
	//AND c.c_w_id=0
	//AND c.c_d_id=3;`
	//
	//fmt.Println(quertText)
	//
	//var shaPrint = sha1.New()
	//_,err := io.WriteString(shaPrint, quertText)
	//if err !=nil{
	//	t.Fatal(err)
	//}
	//queryFingerPrint := fmt.Sprintf("%x", shaPrint.Sum(nil))
	//fmt.Println(queryFingerPrint)

	t1 := &roachpb.AccessNode{
		TableName:    "customer",
		AccessMethod: roachpb.AccessNode_USE_TABLE_SCAN,
		Cardinality:  1,
		IndexName:    []string{"customer_idx"},
	}
	t2 := &roachpb.AccessNode{
		TableName:    "order_line",
		AccessMethod: roachpb.AccessNode_FORCE_INDEX_SCAN,
		Cardinality:  2,
		IndexName:    []string{"order_line_fk"},
	}
	j1 := &roachpb.JoinNode{
		LeftSide:      &roachpb.JoinNode_LeftAccessNode{LeftAccessNode: t1},
		RightSide:     &roachpb.JoinNode_RightAccessNode{RightAccessNode: t2},
		RealJoinOrder: false,
		JoinMethod:    roachpb.JoinNode_FORCE_HASH,
		IndexName:     "index1",
		Cardinality:   3,
	}
	t3 := &roachpb.AccessNode{
		TableName:    "order",
		AccessMethod: roachpb.AccessNode_FORCE_INDEX_SCAN,
		Cardinality:  4,
		IndexName:    []string{"order_idx"},
	}
	forestlist := &roachpb.QueryHintTree{
		RootJoinNode:   []*roachpb.JoinNode{j1},
		RootAccessNode: []*roachpb.AccessNode{t3},
	}
	testData, err := protoutil.Marshal(forestlist)
	if err != nil {
		panic(err)
	}
	//Hexadecimal code is stored in pseudo_catalog
	hintInfo := hex.EncodeToString(testData)

	var wantTree HintForest
	wt1 := &HintTreeScanNode{
		HintType:         keys.UseTableScan,
		IndexName:        []string{"customer_idx"},
		TableName:        "customer",
		TotalCardinality: 1,
	}
	wt2 := &HintTreeScanNode{
		HintType:         keys.ForceIndexScan,
		IndexName:        []string{"order_line_fk"},
		TableName:        "order_line",
		TotalCardinality: 2,
	}
	wt3 := &HintTreeScanNode{
		HintType:         keys.ForceIndexScan,
		IndexName:        []string{"order_idx"},
		TableName:        "order",
		TotalCardinality: 4,
	}
	wj1 := &HintTreeJoinNode{
		HintType:         keys.ForceHash,
		Left:             wt1,
		Right:            wt2,
		RealJoinOrder:    false,
		IndexName:        "index1",
		TotalCardinality: 3,
	}

	wantTree = append(wantTree, wt3)
	wantTree = append(wantTree, wj1)
	//hintTree := proto2go(testbs)
	t.Run("Hex", func(t *testing.T) {
		hintTree, err := HintHex2go(hintInfo)
		if err != nil {
			t.Fatal(err)
		}
		eq := reflect.DeepEqual(hintTree, wantTree)
		if !eq {
			t.Fatalf("not equal")
		}
	})
}
