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

package server_test

import (
	"context"
	"io"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/grpcutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// TestDrain tests the Drain RPC.
func TestDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	doTestDrain(t, true /* newInterface */)
}

// TestDrainLegacy tests the Drain RPC using the pre-20.1 probe signaling.
// TODO(knz): Remove this test when compatibility with pre-20.1 nodes
// is dropped.
func TestDrainLegacy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	doTestDrain(t, false /* newInterface */)
}

// doTestDrain runs the drain test.
// The parameter newInterface indicates whether to use the pre-20.1
// protocol based on "drain modes" or the post-20.1 protocol
// using discrete fields on the request object.
func doTestDrain(tt *testing.T, newInterface bool) {
	t := newTestDrainContext(tt, newInterface)
	defer t.Close()

	// Issue a probe. We're not draining yet, so the probe should
	// reflect that.
	resp := t.sendProbe()
	t.assertDraining(resp, false)
	t.assertRemaining(resp, false)

	// Issue a drain without shutdown, so we can probe more afterwards.
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, true)

	// Issue another probe. This checks that the server is still running
	// (i.e. Shutdown: false was effective) and also that the draining
	// status is still properly reported.
	resp = t.sendProbe()
	t.assertDraining(resp, true)
	// probe-only has no remaining.
	t.assertRemaining(resp, false)

	// Issue another drain. Verify that the remaining is zero (i.e. complete).
	resp = t.sendDrainNoShutdown()
	t.assertDraining(resp, true)
	t.assertRemaining(resp, false)

	// Now issue a drain request without drain but with shutdown.
	// We're expecting the node to be shut down after that.
	resp = t.sendShutdown()
	if resp != nil {
		t.assertDraining(resp, true)
		t.assertRemaining(resp, false)
	}

	// Now expect the server to be shut down.
	testutils.SucceedsSoon(t, func() error {
		_, err := t.c.Drain(context.TODO(), &serverpb.DrainRequest{Shutdown: false})
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return errors.Newf("server not yet refusing RPC, got %v", err)
	})
}

type testDrainContext struct {
	*testing.T
	tc           *testcluster.TestCluster
	newInterface bool
	c            serverpb.AdminClient
	connCloser   func()
}

func newTestDrainContext(t *testing.T, newInterface bool) *testDrainContext {
	tc := &testDrainContext{
		T:            t,
		newInterface: newInterface,
		tc: testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			// We need to start the cluster insecure in order to not
			// care about TLS settings for the RPC client connection.
			ServerArgs: base.TestServerArgs{
				Insecure: true,
			},
		}),
	}

	// We'll have the RPC talk to the first node.
	var err error
	tc.c, tc.connCloser, err = getAdminClientForServer(context.TODO(),
		tc.tc, 0 /* serverIdx */)
	if err != nil {
		tc.Close()
		t.Fatal(err)
	}

	return tc
}

func (t *testDrainContext) Close() {
	if t.connCloser != nil {
		t.connCloser()
	}
	t.tc.Stopper().Stop(context.TODO())
}

func (t *testDrainContext) sendProbe() *serverpb.DrainResponse {
	return t.drainRequest(false /* drain */, false /* shutdown */)
}

func (t *testDrainContext) sendDrainNoShutdown() *serverpb.DrainResponse {
	return t.drainRequest(true /* drain */, false /* shutdown */)
}

func (t *testDrainContext) drainRequest(drain, shutdown bool) *serverpb.DrainResponse {
	// Issue a simple drain probe.
	req := &serverpb.DrainRequest{Shutdown: shutdown}

	if drain {
		if t.newInterface {
			req.DoDrain = true
		} else {
			req.DeprecatedProbeIndicator = server.DeprecatedDrainParameter
		}
	}

	drainStream, err := t.c.Drain(context.TODO(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := t.getDrainResponse(drainStream)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (t *testDrainContext) sendShutdown() *serverpb.DrainResponse {
	req := &serverpb.DrainRequest{Shutdown: true}
	drainStream, err := t.c.Drain(context.TODO(), req)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := t.getDrainResponse(drainStream)
	if err != nil {
		// It's possible we're getting "connection reset by peer" or some
		// gRPC initialization failure because the server is shutting
		// down. Tolerate that.
		t.Logf("RPC error: %v", err)
	}
	return resp
}

func (t *testDrainContext) assertDraining(resp *serverpb.DrainResponse, drain bool) {
	if resp.IsDraining != drain {
		t.Fatalf("expected draining %v, got %v", drain, resp.IsDraining)
	}
	// Check that the deprecated status field is compatible with expectation.
	// TODO(knz): Remove this test when compatibility with pre-20.1 nodes
	// is dropped.
	if drain {
		if !reflect.DeepEqual(resp.DeprecatedDrainStatus, server.DeprecatedDrainParameter) {
			t.Fatalf("expected compat drain status, got %# v", pretty.Formatter(resp))
		}
	} else {
		if len(resp.DeprecatedDrainStatus) > 0 {
			t.Fatalf("expected no compat drain status, got %# v", pretty.Formatter(resp))
		}
	}
}

func (t *testDrainContext) assertRemaining(resp *serverpb.DrainResponse, remaining bool) {
	if actualRemaining := (resp.DrainRemainingIndicator > 0); remaining != actualRemaining {
		t.Fatalf("expected remaining %v, got %v", remaining, actualRemaining)
	}
}

func (t *testDrainContext) getDrainResponse(
	stream serverpb.Admin_DrainClient,
) (*serverpb.DrainResponse, error) {
	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	unexpected, err := stream.Recv()
	if err != io.EOF {
		if unexpected != nil {
			t.Fatalf("unexpected additional response: %# v // %v", pretty.Formatter(unexpected), err)
		}
		if err == nil {
			err = errors.New("unexpected response")
		}
		return nil, err
	}
	return resp, nil
}

func getAdminClientForServer(
	ctx context.Context, tc *testcluster.TestCluster, serverIdx int,
) (c serverpb.AdminClient, closer func(), err error) {
	stopper := stop.NewStopper() // stopper for the client.
	// Retrieve some parameters to initialize the client RPC context.
	cfg := tc.Server(0).RPCContext().Config
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	rpcContext := rpc.NewContext(
		log.AmbientContext{Tracer: execCfg.Settings.Tracer},
		cfg, execCfg.Clock, stopper, execCfg.Settings,
	)
	conn, err := rpcContext.GRPCUnvalidatedDial(tc.Server(serverIdx).ServingRPCAddr()).Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	return serverpb.NewAdminClient(conn), func() { stopper.Stop(ctx) }, nil
}
