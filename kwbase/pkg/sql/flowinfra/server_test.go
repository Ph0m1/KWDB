// Copyright 2016 The Cockroach Authors.
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

package flowinfra

import (
	"context"
	"fmt"
	"io"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestServer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	conn, err := s.RPCContext().GRPCDialNode(s.ServingRPCAddr(), s.NodeID(),
		rpc.DefaultClass).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	r := sqlutils.MakeSQLRunner(sqlDB)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `CREATE TABLE test.t (a INT PRIMARY KEY, b INT)`)
	r.Exec(t, `INSERT INTO test.t VALUES (1, 10), (2, 20), (3, 30)`)

	td := sqlbase.GetTableDescriptor(kvDB, "test", "t")

	ts := execinfrapb.TableReaderSpec{
		Table:    *td,
		IndexIdx: 0,
		Reverse:  false,
		Spans:    []execinfrapb.TableReaderSpan{{Span: td.PrimaryIndexSpan()}},
	}
	post := execinfrapb.PostProcessSpec{
		Filter:        execinfrapb.Expression{Expr: "@1 != 2"}, // a != 2
		Projection:    true,
		OutputColumns: []uint32{0, 1}, // a
	}

	txn := kv.NewTxn(ctx, kvDB, s.NodeID())
	leafInputState := txn.GetLeafTxnInputState(ctx)

	req := &execinfrapb.SetupFlowRequest{
		Version:           execinfra.Version,
		LeafTxnInputState: &leafInputState,
	}
	req.Flow = execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{{
			Core: execinfrapb.ProcessorCoreUnion{TableReader: &ts},
			Post: post,
			Output: []execinfrapb.OutputRouterSpec{{
				Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
				Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_SYNC_RESPONSE}},
			}},
		}},
	}

	distSQLClient := execinfrapb.NewDistSQLClient(conn)
	stream, err := distSQLClient.RunSyncFlow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&execinfrapb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
		t.Fatal(err)
	}

	var decoder StreamDecoder
	var rows sqlbase.EncDatumRows
	var metas []execinfrapb.ProducerMetadata
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		err = decoder.AddMessage(context.TODO(), msg)
		if err != nil {
			t.Fatal(err)
		}
		rows, metas = testGetDecodedRows(t, &decoder, rows, metas)
	}
	metas = ignoreLeafTxnState(metas)
	metas = ignoreMetricsMeta(metas)
	if len(metas) != 0 {
		t.Errorf("unexpected metadata: %v", metas)
	}
	str := rows.String(sqlbase.TwoIntCols)
	expected := "[[1 10] [3 30]]"
	if str != expected {
		t.Errorf("invalid results: %s, expected %s'", str, expected)
	}

	// Verify version handling.
	t.Run("version", func(t *testing.T) {
		testCases := []struct {
			version     execinfrapb.DistSQLVersion
			expectedErr string
		}{
			{
				version:     execinfra.Version + 1,
				expectedErr: "version mismatch",
			},
			{
				version:     execinfra.MinAcceptedVersion - 1,
				expectedErr: "version mismatch",
			},
			{
				version:     execinfra.MinAcceptedVersion,
				expectedErr: "",
			},
		}
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%d", tc.version), func(t *testing.T) {
				distSQLClient := execinfrapb.NewDistSQLClient(conn)
				stream, err := distSQLClient.RunSyncFlow(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				req.Version = tc.version
				if err := stream.Send(&execinfrapb.ConsumerSignal{SetupFlowRequest: req}); err != nil {
					t.Fatal(err)
				}
				_, err = stream.Recv()
				if !testutils.IsError(err, tc.expectedErr) {
					t.Errorf("expected error '%s', got %v", tc.expectedErr, err)
				}
				// In the expectedErr == nil case, we leave a flow hanging; we're not
				// consuming it. It will get canceled by the draining process.
			})
		}
	})
}

// Test that a node gossips its DistSQL version information.
func TestDistSQLServerGossipsVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	var v execinfrapb.DistSQLVersionGossipInfo
	if err := s.GossipI().(*gossip.Gossip).GetInfoProto(
		gossip.MakeDistSQLNodeVersionKey(s.NodeID()), &v,
	); err != nil {
		t.Fatal(err)
	}

	if v.Version != execinfra.Version || v.MinAcceptedVersion != execinfra.MinAcceptedVersion {
		t.Fatalf("node is gossipping the wrong version. Expected: [%d-%d], got [%d-%d",
			execinfra.Version, execinfra.MinAcceptedVersion, v.Version, v.MinAcceptedVersion)
	}
}
