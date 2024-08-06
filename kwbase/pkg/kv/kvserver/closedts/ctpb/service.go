// Copyright 2020 The Cockroach Authors.
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

package ctpb

import (
	"context"

	"google.golang.org/grpc"
)

// ClosedTimestampClient192 is like ClosedTimestampClient, except its Get192()
// method uses the RPC service that 19.2 had, not the 20.1 one. In 20.1 we've
// changed the name of the RPC service.
type ClosedTimestampClient192 interface {
	// Get192 calls Get() on the RPC service exposed by 19.2 nodes.
	Get192(ctx context.Context, opts ...grpc.CallOption) (ClosedTimestamp_GetClient, error)
}

func (c *closedTimestampClient) Get192(
	ctx context.Context, opts ...grpc.CallOption,
) (ClosedTimestamp_GetClient, error) {
	// Instead of "/kwbase.kv.kvserver.ctupdate.ClosedTimestamp/Get".
	stream, err := c.cc.NewStream(ctx, &_ClosedTimestamp_serviceDesc.Streams[0],
		"/kwbase.storage.ctupdate.ClosedTimestamp/Get", opts...)
	if err != nil {
		return nil, err
	}
	x := &closedTimestampGetClient{stream}
	return x, nil
}

// _ClosedTimestampServiceDesc192 is like _ClosedTimestamp_serviceDesc, except
// it uses the service name that 19.2 nodes were using. We've changed the
// service name in 20.1 by mistake.
var _ClosedTimestampServiceDesc192 = grpc.ServiceDesc{
	// Instead of "kwbase.kv.kvserver.ctupdate.ClosedTimestamp".
	ServiceName: "kwbase.storage.ctupdate.ClosedTimestamp",
	HandlerType: (*ClosedTimestampServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Get",
			Handler:       _ClosedTimestamp_Get_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "kv/kvserver/closedts/ctpb/service.proto",
}

// RegisterClosedTimestampServerUnder192Name is like
// RegisterClosedTimestampServer, except it uses a different service name - the
// old name that 19.2 nodes are using.
func RegisterClosedTimestampServerUnder192Name(s *grpc.Server, srv ClosedTimestampServer) {
	s.RegisterService(&_ClosedTimestampServiceDesc192, srv)
}
