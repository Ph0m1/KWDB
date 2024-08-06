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

package kvserver

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestReplicaChecksumVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	testutils.RunTrueAndFalse(t, "matchingVersion", func(t *testing.T, matchingVersion bool) {
		cc := storagepb.ComputeChecksum{
			ChecksumID: uuid.FastMakeV4(),
			Mode:       roachpb.ChecksumMode_CHECK_FULL,
		}
		if matchingVersion {
			cc.Version = batcheval.ReplicaChecksumVersion
		} else {
			cc.Version = 1
		}
		tc.repl.computeChecksumPostApply(ctx, cc)
		rc, err := tc.repl.getChecksum(ctx, cc.ChecksumID)
		if !matchingVersion {
			if !testutils.IsError(err, "no checksum found") {
				t.Fatal(err)
			}
			require.Nil(t, rc.Checksum)
		} else {
			require.NoError(t, err)
			require.NotNil(t, rc.Checksum)
		}
	})
}

func TestGetChecksumNotSuccessfulExitConditions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	defer cancel()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	id := uuid.FastMakeV4()
	notify := make(chan struct{})
	close(notify)

	// Simple condition, the checksum is notified, but not computed.
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: notify}
	tc.repl.mu.Unlock()
	rc, err := tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "no checksum found") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
	// Next condition, the initial wait expires and checksum is not started,
	// this will take 10ms.
	id = uuid.FastMakeV4()
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: make(chan struct{})}
	tc.repl.mu.Unlock()
	rc, err = tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "checksum computation did not start") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
	// Next condition, initial wait expired and we found the started flag,
	// so next step is for context deadline.
	id = uuid.FastMakeV4()
	tc.repl.mu.Lock()
	tc.repl.mu.checksums[id] = ReplicaChecksum{notify: make(chan struct{}), started: true}
	tc.repl.mu.Unlock()
	rc, err = tc.repl.getChecksum(ctx, id)
	if !testutils.IsError(err, "context deadline exceeded") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)

	// Need to reset the context, since we deadlined it above.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// Next condition, node should quiesce.
	tc.repl.store.Stopper().Quiesce(ctx)
	rc, err = tc.repl.getChecksum(ctx, uuid.FastMakeV4())
	if !testutils.IsError(err, "store quiescing") {
		t.Fatal(err)
	}
	require.Nil(t, rc.Checksum)
}
