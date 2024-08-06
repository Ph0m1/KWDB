// Copyright 2017 The Cockroach Authors.
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

package physicalplan_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/physicalplanutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestFakeSpanResolver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlutils.CreateTable(
		t, tc.ServerConn(0), "t",
		"k INT PRIMARY KEY, v INT",
		100,
		func(row int) []tree.Datum {
			return []tree.Datum{
				tree.NewDInt(tree.DInt(row)),
				tree.NewDInt(tree.DInt(row * row)),
			}
		},
	)

	resolver := physicalplanutils.FakeResolverForTestCluster(tc)

	db := tc.Server(0).DB()

	txn := kv.NewTxn(ctx, db, tc.Server(0).NodeID())
	it := resolver.NewSpanResolverIterator(txn)

	tableDesc := sqlbase.GetTableDescriptor(db, "test", "t")
	primIdxValDirs := sqlbase.IndexKeyValDirs(&tableDesc.PrimaryIndex)

	span := tableDesc.PrimaryIndexSpan()

	// Make sure we see all the nodes. It will not always happen (due to
	// randomness) but it should happen most of the time.
	for attempt := 0; attempt < 10; attempt++ {
		nodesSeen := make(map[roachpb.NodeID]struct{})
		it.Seek(ctx, span, kvcoord.Ascending)
		lastKey := span.Key
		for {
			if !it.Valid() {
				t.Fatal(it.Error())
			}
			desc := it.Desc()
			rinfo, err := it.ReplicaInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}

			prettyStart := keys.PrettyPrint(primIdxValDirs, desc.StartKey.AsRawKey())
			prettyEnd := keys.PrettyPrint(primIdxValDirs, desc.EndKey.AsRawKey())
			t.Logf("%d %s %s", rinfo.NodeID, prettyStart, prettyEnd)

			if !lastKey.Equal(desc.StartKey.AsRawKey()) {
				t.Errorf("unexpected start key %s, should be %s", prettyStart, keys.PrettyPrint(primIdxValDirs, span.Key))
			}
			if !desc.StartKey.Less(desc.EndKey) {
				t.Errorf("invalid range %s to %s", prettyStart, prettyEnd)
			}
			lastKey = desc.EndKey.AsRawKey()
			nodesSeen[rinfo.NodeID] = struct{}{}

			if !it.NeedAnother() {
				break
			}
			it.Next(ctx)
		}

		if !lastKey.Equal(span.EndKey) {
			t.Errorf("last key %s, should be %s", keys.PrettyPrint(primIdxValDirs, lastKey), keys.PrettyPrint(primIdxValDirs, span.EndKey))
		}

		if len(nodesSeen) == tc.NumServers() {
			// Saw all the nodes.
			break
		}
		t.Logf("not all nodes seen; retrying")
	}
}
