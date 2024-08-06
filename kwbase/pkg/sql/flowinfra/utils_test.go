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

package flowinfra

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// createDummyStream creates the server and client side of a FlowStream stream.
// This can be use by tests to pretend that then have received a FlowStream RPC.
// The stream can be used to send messages (ConsumerSignal's) on it (within a
// gRPC window limit since nobody's reading from the stream), for example
// Handshake messages.
//
// We do this by creating a mock server, dialing into it and capturing the
// server stream. The server-side RPC call will be blocked until the caller
// calls the returned cleanup function.
func createDummyStream() (
	serverStream execinfrapb.DistSQL_FlowStreamServer,
	clientStream execinfrapb.DistSQL_FlowStreamClient,
	cleanup func(),
	err error,
) {
	stopper := stop.NewStopper()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	clusterID, mockServer, addr, err := execinfrapb.StartMockDistSQLServer(clock, stopper, execinfra.StaticNodeID)
	if err != nil {
		return nil, nil, nil, err
	}

	rpcContext := rpc.NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
	conn, err := rpcContext.GRPCDialNode(addr.String(), execinfra.StaticNodeID,
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		return nil, nil, nil, err
	}
	client := execinfrapb.NewDistSQLClient(conn)
	clientStream, err = client.FlowStream(context.TODO())
	if err != nil {
		return nil, nil, nil, err
	}
	streamNotification := <-mockServer.InboundStreams
	serverStream = streamNotification.Stream
	cleanup = func() {
		close(streamNotification.Donec)
		stopper.Stop(context.TODO())
	}
	return serverStream, clientStream, cleanup, nil
}
