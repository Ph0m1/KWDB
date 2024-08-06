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

package container

import (
	"context"
	"errors"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/closedts/ctpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

type noopEverything struct{}

var _ closedts.Dialer = noopEverything{}

// NoopContainer returns a Container for which all parts of the subsystem are
// mocked out. This is for usage in testing where there just needs to be a
// structure that stays out of the way.
//
// The returned container will behave correctly. It will never allow any time-
// stamps to be closed out, so it never makes any promises; it doesn't use any
// locking and it does not consume any (nontrivial) resources.
func NoopContainer() *Container {
	return &Container{
		Config: Config{
			Settings: cluster.MakeTestingClusterSettings(),
			Stopper:  stop.NewStopper(),
			Clock: func(roachpb.NodeID) (hlc.Timestamp, ctpb.Epoch, error) {
				return hlc.Timestamp{}, 0, errors.New("closed timestamps disabled for testing")
			},
			Refresh: func(...roachpb.RangeID) {},
			Dialer:  noopEverything{},
		},
		Tracker:  noopEverything{},
		Storage:  noopEverything{},
		Provider: noopEverything{},
		Server:   noopEverything{},
		Clients:  noopEverything{},
		noop:     true,
	}
}

func (noopEverything) Get(client ctpb.InboundClient) error {
	return errors.New("closed timestamps disabled")
}
func (noopEverything) Close(
	next hlc.Timestamp, expCurEpoch ctpb.Epoch,
) (hlc.Timestamp, map[roachpb.RangeID]ctpb.LAI, bool) {
	return hlc.Timestamp{}, nil, false
}
func (noopEverything) Track(ctx context.Context) (hlc.Timestamp, closedts.ReleaseFunc) {
	return hlc.Timestamp{}, func(context.Context, ctpb.Epoch, roachpb.RangeID, ctpb.LAI) {}
}
func (noopEverything) VisitAscending(roachpb.NodeID, func(ctpb.Entry) (done bool))  {}
func (noopEverything) VisitDescending(roachpb.NodeID, func(ctpb.Entry) (done bool)) {}
func (noopEverything) Add(roachpb.NodeID, ctpb.Entry)                               {}
func (noopEverything) Clear()                                                       {}
func (noopEverything) Notify(roachpb.NodeID) chan<- ctpb.Entry {
	return nil // will explode when used, but nobody would use this
}
func (noopEverything) Subscribe(context.Context, chan<- ctpb.Entry) {}
func (noopEverything) Start()                                       {}
func (noopEverything) MaxClosed(
	roachpb.NodeID, roachpb.RangeID, ctpb.Epoch, ctpb.LAI,
) hlc.Timestamp {
	return hlc.Timestamp{}
}
func (noopEverything) Request(roachpb.NodeID, roachpb.RangeID) {}
func (noopEverything) EnsureClient(roachpb.NodeID)             {}

// Dial implements the closedts.Dialer interface.
func (noopEverything) Dial(
	context.Context, roachpb.NodeID,
) (closedts.BackwardsCompatibleClosedTimestampClient, error) {
	return nil, errors.New("closed timestamps disabled")
}

func (noopEverything) Ready(roachpb.NodeID) bool { return false }
