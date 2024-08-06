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

package server

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// initServer manages the temporary init server used during
// bootstrapping.
type initServer struct {
	mu struct {
		syncutil.Mutex
		// If set, a Bootstrap() call is rejected with this error.
		rejectErr error
	}
	bootstrapReqCh chan struct{}
	connected      <-chan struct{}
	shouldStop     <-chan struct{}
	auditServer    *server.AuditServer
	clusterID      *base.ClusterIDContainer
}

func newInitServer(
	connected <-chan struct{},
	shouldStop <-chan struct{},
	auditServer *server.AuditServer,
	clusterID *base.ClusterIDContainer,
) *initServer {
	return &initServer{
		bootstrapReqCh: make(chan struct{}),
		connected:      connected,
		shouldStop:     shouldStop,
		auditServer:    auditServer,
		clusterID:      clusterID,
	}
}

// testOrSetRejectErr set the reject error unless a reject error was already
// set, in which case it returns the one that was already set. If no error had
// previously been set, returns nil.
func (s *initServer) testOrSetRejectErr(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.rejectErr != nil {
		return s.mu.rejectErr
	}
	s.mu.rejectErr = err
	return nil
}

type initServerResult int

const (
	invalidInitResult initServerResult = iota
	connectedToCluster
	needBootstrap
)

// awaitBootstrap blocks until the connected channel is closed or a Bootstrap()
// call is received. It returns true if a Bootstrap() call is received,
// instructing the caller to perform cluster bootstrap. It returns false if the
// connected channel is closed, telling the caller that someone else
// bootstrapped the cluster. Assuming that the connected channel comes from
// Gossip, this means that the cluster ID is now available in gossip.
func (s *initServer) awaitBootstrap() (initServerResult, error) {
	select {
	case <-s.connected:
		_ = s.testOrSetRejectErr(fmt.Errorf("already connected to cluster"))
		return connectedToCluster, nil
	case <-s.bootstrapReqCh:
		return needBootstrap, nil
	case <-s.shouldStop:
		err := fmt.Errorf("stop called while waiting to bootstrap")
		_ = s.testOrSetRejectErr(err)
		return invalidInitResult, err
	}
}

// Bootstrap unblocks an awaitBootstrap() call. If awaitBootstrap() hasn't been
// called yet, it will not block the next time it's called.
//
// TODO(andrei): There's a race between gossip connecting and this initServer
// getting a Bootstrap request that allows both to succeed: there's no
// synchronization between gossip and this server and so gossip can succeed in
// propagating one cluster ID while this call succeeds in telling the Server to
// bootstrap and created a new cluster ID. We should fix it somehow by tangling
// the gossip.Server with this initServer such that they serialize access to a
// clusterID and decide among themselves a single winner for the race.
func (s *initServer) Bootstrap(
	ctx context.Context, request *serverpb.BootstrapRequest,
) (response *serverpb.BootstrapResponse, err error) {
	auditInfo := server.MakeAuditInfo(timeutil.Now(), "", nil,
		target.Init, target.ObjectCluster, 0, nil, nil)

	if err := s.testOrSetRejectErr(ErrClusterInitialized); err != nil {
		auditInfo.SetResult(err, 0)
		if err := s.auditServer.LogAudit(ctx, nil, &auditInfo); err != nil {
			log.Warningf(ctx, "got error when audit cluster init, err:%s", err)
		}
		return nil, err
	}
	close(s.bootstrapReqCh)
	if err := s.auditServer.LogAudit(ctx, nil, &auditInfo); err != nil {
		log.Warningf(ctx, "got error when audit cluster init, err:%s", err)
	}
	return &serverpb.BootstrapResponse{}, nil
}

// ErrClusterInitialized is reported when the Boostrap RPC is ran on
// a node already part of an initialized cluster.
var ErrClusterInitialized = fmt.Errorf("cluster has already been initialized")
