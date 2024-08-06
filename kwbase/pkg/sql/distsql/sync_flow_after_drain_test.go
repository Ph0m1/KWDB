// Copyright 2019 The Cockroach Authors.
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

package distsql

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/distsqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// Test that we can register send a sync flow to the distSQLSrv after the
// FlowRegistry is draining and the we can also clean that flow up (the flow
// will get a draining error). This used to crash.
func TestSyncFlowAfterDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	// We'll create a server just so that we can extract its distsql ServerConfig,
	// so we can use it for a manually-built DistSQL Server below. Otherwise, too
	// much work to create that ServerConfig by hand.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	cfg := s.DistSQLServer().(*ServerImpl).ServerConfig

	distSQLSrv := NewServer(ctx, cfg)
	distSQLSrv.flowRegistry.Drain(
		time.Duration(0) /* flowDrainWait */, time.Duration(0) /* minFlowDrainWait */, nil /* reporter */)

	// We create some flow; it doesn't matter what.
	req := execinfrapb.SetupFlowRequest{Version: execinfra.Version}
	req.Flow = execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{
			{
				Core: execinfrapb.ProcessorCoreUnion{Values: &execinfrapb.ValuesCoreSpec{}},
				Output: []execinfrapb.OutputRouterSpec{{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{StreamID: 1, Type: execinfrapb.StreamEndpointType_REMOTE}},
				}},
			},
			{
				Input: []execinfrapb.InputSyncSpec{{
					Type:    execinfrapb.InputSyncSpec_UNORDERED,
					Streams: []execinfrapb.StreamEndpointSpec{{StreamID: 1, Type: execinfrapb.StreamEndpointType_REMOTE}},
				}},
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Output: []execinfrapb.OutputRouterSpec{{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointType_SYNC_RESPONSE}},
				}},
			},
		},
	}

	types := make([]types.T, 0)
	rb := distsqlutils.NewRowBuffer(types, nil /* rows */, distsqlutils.RowBufferArgs{})
	ctx, flow, err := distSQLSrv.SetupSyncFlow(ctx, &distSQLSrv.memMonitor, &req, rb)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.Start(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	flow.Wait()
	_, meta := rb.Next()
	if meta == nil {
		t.Fatal("expected draining err, got no meta")
	}
	if !testutils.IsError(meta.Err, "the registry is draining") {
		t.Fatalf("expected draining err, got: %v", meta.Err)
	}
	flow.Cleanup(ctx)
}
