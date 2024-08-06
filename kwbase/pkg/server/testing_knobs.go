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

package server

import (
	"net"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
)

// TestingKnobs groups testing knobs for the Server.
type TestingKnobs struct {
	// DisableAutomaticVersionUpgrade, if set, temporarily disables the server's
	// automatic version upgrade mechanism.
	DisableAutomaticVersionUpgrade int32 // accessed atomically
	// DefaultZoneConfigOverride, if set, overrides the default zone config defined in `pkg/config/zone.go`
	DefaultZoneConfigOverride *zonepb.ZoneConfig
	// DefaultSystemZoneConfigOverride, if set, overrides the default system zone config defined in `pkg/config/zone.go`
	DefaultSystemZoneConfigOverride *zonepb.ZoneConfig
	// PauseAfterGettingRPCAddress, if non-nil, instructs the server to wait until
	// the channel is closed after getting an RPC serving address.
	PauseAfterGettingRPCAddress chan struct{}
	// SignalAfterGettingRPCAddress, if non-nil, is closed after the server gets
	// an RPC server address.
	SignalAfterGettingRPCAddress chan struct{}
	// ContextTestingKnobs allows customization of the RPC context testing knobs.
	ContextTestingKnobs rpc.ContextTestingKnobs

	// If set, use this listener for RPC (and possibly SQL, depending on
	// the SplitListenSQL setting), instead of binding a new listener.
	// This is useful in tests that need an ephemeral listening port but
	// must know it before the server starts.
	//
	// When this is used, the advertise address should also be set to
	// match.
	//
	// The Server takes responsibility for closing this listener.
	// TODO(bdarnell): That doesn't give us a good way to clean up if the
	// server fails to start.
	RPCListener net.Listener

	// BootstrapVersionOverride, if not empty, will be used for bootstrapping
	// clusters instead of clusterversion.BinaryVersion (if this server is the
	// one bootstrapping the cluster).
	//
	// This can be used by tests to essentially pretend that a new cluster is
	// not starting from scratch, but instead is "created" by a node starting up
	// with engines that had already been bootstrapped, at this
	// BootstrapVersionOverride. For example, it allows convenient creation of a
	// cluster from a 2.1 binary, but that's running at version 2.0.
	//
	// NB: When setting this, you probably also want to set
	// DisableAutomaticVersionUpgrade.
	//
	// TODO(irfansharif): Update users of this testing knob to use the
	// appropriate clusterversion.Handle instead.
	BootstrapVersionOverride roachpb.Version
	// Clock Source used to an inject a custom clock for testing the server. It is
	// typically either an hlc.HybridManualClock or hlc.ManualClock.
	ClockSource func() int64
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
