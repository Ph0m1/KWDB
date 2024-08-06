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

package rpc

import (
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"google.golang.org/grpc"
)

// ContextTestingKnobs provides hooks to aid in testing the system. The testing
// knob functions are called at various points in the Context life cycle if they
// are non-nil.
type ContextTestingKnobs struct {

	// UnaryClientInterceptor if non-nil will be called at dial time to provide
	// the base unary interceptor for client connections.
	// This function may return a nil interceptor to avoid injecting behavior
	// for a given target and class.
	UnaryClientInterceptor func(target string, class ConnectionClass) grpc.UnaryClientInterceptor

	// StreamClient if non-nil will be called at dial time to provide
	// the base stream interceptor for client connections.
	// This function may return a nil interceptor to avoid injecting behavior
	// for a given target and class.
	StreamClientInterceptor func(target string, class ConnectionClass) grpc.StreamClientInterceptor

	// ArtificialLatencyMap if non-nil contains a map from target address
	// (server.RPCServingAddr() of a remote node) to artificial latency in
	// milliseconds to inject. Setting this will cause the server to pause for
	// the given amount of milliseconds on every network write.
	ArtificialLatencyMap map[string]int

	// ClusterID initializes the Context's ClusterID container to this value if
	// non-nil at construction time.
	ClusterID *uuid.UUID
}

// NewInsecureTestingContext creates an insecure rpc Context suitable for tests.
func NewInsecureTestingContext(clock *hlc.Clock, stopper *stop.Stopper) *Context {
	clusterID := uuid.MakeV4()
	return NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
}

// NewInsecureTestingContextWithClusterID creates an insecure rpc Context
// suitable for tests. The context is given the provided cluster ID.
func NewInsecureTestingContextWithClusterID(
	clock *hlc.Clock, stopper *stop.Stopper, clusterID uuid.UUID,
) *Context {
	return NewInsecureTestingContextWithKnobs(clock, stopper, ContextTestingKnobs{
		ClusterID: &clusterID,
	})
}

// NewInsecureTestingContextWithKnobs creates an insecure rpc Context
// suitable for tests configured with the provided knobs.
func NewInsecureTestingContextWithKnobs(
	clock *hlc.Clock, stopper *stop.Stopper, knobs ContextTestingKnobs,
) *Context {
	return NewContextWithTestingKnobs(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		clock,
		stopper,
		cluster.MakeTestingClusterSettings(),
		knobs,
	)
}
