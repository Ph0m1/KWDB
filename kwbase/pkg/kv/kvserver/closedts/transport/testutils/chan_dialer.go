// Copyright 2018 The Cockroach Authors.
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

package testutils

import (
	"context"
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"google.golang.org/grpc"
)

// ChanDialer is an implementation of closedts.Dialer that connects clients
// directly via a channel to a Server.
type ChanDialer struct {
	stopper *stop.Stopper
	server  ctpb.Server

	mu struct {
		syncutil.Mutex
		transcripts map[roachpb.NodeID][]interface{}
	}
}

// NewChanDialer sets up a ChanDialer.
func NewChanDialer(stopper *stop.Stopper, server ctpb.Server) *ChanDialer {
	d := &ChanDialer{
		stopper: stopper,
		server:  server,
	}
	d.mu.transcripts = make(map[roachpb.NodeID][]interface{})
	return d
}

// Transcript returns a slice of messages sent over the "wire".
func (d *ChanDialer) Transcript(nodeID roachpb.NodeID) []interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]interface{}(nil), d.mu.transcripts[nodeID]...)
}

// Dial implements closedts.Dialer.
func (d *ChanDialer) Dial(
	ctx context.Context, nodeID roachpb.NodeID,
) (closedts.BackwardsCompatibleClosedTimestampClient, error) {
	c := &client{
		ctx:     ctx,
		send:    make(chan *ctpb.Reaction),
		recv:    make(chan *ctpb.Entry),
		stopper: d.stopper,
		observe: func(msg interface{}) {
			d.mu.Lock()
			if d.mu.transcripts == nil {
				d.mu.transcripts = map[roachpb.NodeID][]interface{}{}
			}
			d.mu.transcripts[nodeID] = append(d.mu.transcripts[nodeID], msg)
			d.mu.Unlock()
		},
	}

	d.stopper.RunWorker(ctx, func(ctx context.Context) {
		_ = d.server.Get((*incomingClient)(c))
	})
	return chanWrapper{c}, nil
}

type chanWrapper struct {
	c *client
}

func (c chanWrapper) Get(ctx context.Context, opts ...grpc.CallOption) (ctpb.Client, error) {
	return c.c, nil
}

func (c chanWrapper) Get192(ctx context.Context, opts ...grpc.CallOption) (ctpb.Client, error) {
	panic("unimplemented")
}

var _ closedts.BackwardsCompatibleClosedTimestampClient = chanWrapper{}

// Ready implements closedts.Dialer by always returning true.
func (d *ChanDialer) Ready(nodeID roachpb.NodeID) bool {
	return true
}

type client struct {
	ctx     context.Context
	stopper *stop.Stopper
	send    chan *ctpb.Reaction
	recv    chan *ctpb.Entry

	observe func(interface{})
}

func (c *client) Send(msg *ctpb.Reaction) error {
	select {
	case <-c.stopper.ShouldQuiesce():
		return io.EOF
	case c.send <- msg:
		c.observe(msg)
		return nil
	}
}

func (c *client) Recv() (*ctpb.Entry, error) {
	select {
	case <-c.stopper.ShouldQuiesce():
		return nil, io.EOF
	case msg := <-c.recv:
		c.observe(msg)
		return msg, nil
	}
}

func (c *client) CloseSend() error {
	close(c.send)
	return nil
}

func (c *client) Context() context.Context {
	return c.ctx
}

type incomingClient client

func (c *incomingClient) Send(msg *ctpb.Entry) error {
	select {
	case <-c.stopper.ShouldQuiesce():
		return io.EOF
	case c.recv <- msg:
		return nil
	}
}

func (c *incomingClient) Recv() (*ctpb.Reaction, error) {
	select {
	case <-c.stopper.ShouldQuiesce():
		return nil, io.EOF
	case msg, ok := <-c.send:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

func (c *incomingClient) Context() context.Context {
	return c.ctx
}
