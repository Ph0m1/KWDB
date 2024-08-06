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

package colrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOutboxCatchesPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input    = colexec.NewBatchBuffer()
		typs     = []coltypes.T{coltypes.Int64}
		rpcLayer = makeMockFlowStreamRPCLayer()
	)
	outbox, err := NewOutbox(testAllocator, input, typs, nil /* metadataSources */, nil /* toClose */)
	require.NoError(t, err)

	// This test relies on the fact that BatchBuffer panics when there are no
	// batches to return. Verify this assumption.
	require.Panics(t, func() { input.Next(ctx) })

	// The actual test verifies that the Outbox handles input execution tree
	// panics by not panicking and returning.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)
		wg.Done()
	}()

	inboxMemAccount := testMemMonitor.MakeBoundAccount()
	defer inboxMemAccount.Close(ctx)
	inbox, err := NewInbox(
		colexec.NewAllocator(ctx, &inboxMemAccount), typs, execinfrapb.StreamID(0),
	)
	require.NoError(t, err)

	streamHandlerErrCh := handleStream(ctx, inbox, rpcLayer.server, func() { close(rpcLayer.server.csChan) })

	// The outbox will be sending the panic as metadata eagerly. This Next call
	// is valid, but should return a zero-length batch, indicating that the caller
	// should call DrainMeta.
	require.True(t, inbox.Next(ctx).Length() == 0)

	// Expect the panic as an error in DrainMeta.
	meta := inbox.DrainMeta(ctx)

	require.True(t, len(meta) == 1)
	require.True(t, testutils.IsError(meta[0].Err, "runtime error: index out of range"), meta[0])

	require.NoError(t, <-streamHandlerErrCh)
	wg.Wait()
}

func TestOutboxDrainsMetadataSources(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input = colexec.NewBatchBuffer()
		typs  = []coltypes.T{coltypes.Int64}
	)

	// Define common function that returns both an Outbox and a pointer to a
	// uint32 that is set atomically when the outbox drains a metadata source.
	newOutboxWithMetaSources := func(allocator *colexec.Allocator) (*Outbox, *uint32, error) {
		var sourceDrained uint32
		outbox, err := NewOutbox(allocator, input, typs, []execinfrapb.MetadataSource{
			execinfrapb.CallbackMetadataSource{
				DrainMetaCb: func(context.Context) []execinfrapb.ProducerMetadata {
					atomic.StoreUint32(&sourceDrained, 1)
					return nil
				},
			},
		}, nil /* toClose */)
		if err != nil {
			return nil, nil, err
		}
		return outbox, &sourceDrained, nil
	}

	t.Run("AfterSuccessfulRun", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		outboxMemAccount := testMemMonitor.MakeBoundAccount()
		defer outboxMemAccount.Close(ctx)
		outbox, sourceDrained, err := newOutboxWithMetaSources(
			colexec.NewAllocator(ctx, &outboxMemAccount),
		)
		require.NoError(t, err)

		b := testAllocator.NewMemBatch(typs)
		b.SetLength(0)
		input.Add(b)

		// Close the csChan to unblock the Recv goroutine (we don't need it for this
		// test).
		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})

	// This is similar to TestOutboxCatchesPanics, but focuses on verifying that
	// the Outbox drains its metadata sources even after an error.
	t.Run("AfterOutboxError", func(t *testing.T) {
		// This test, similar to TestOutboxCatchesPanics, relies on the fact that
		// a BatchBuffer panics when there are no batches to return.
		require.Panics(t, func() { input.Next(ctx) })

		rpcLayer := makeMockFlowStreamRPCLayer()
		outbox, sourceDrained, err := newOutboxWithMetaSources(testAllocator)
		require.NoError(t, err)

		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})
}
