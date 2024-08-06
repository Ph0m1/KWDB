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

package ptreconcile_test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptreconcile"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReconciler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	s0 := tc.Server(0)
	ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider

	settings := cluster.MakeTestingClusterSettings()
	const testTaskType = "foo"
	var state = struct {
		mu       syncutil.Mutex
		toRemove map[string]struct{}
	}{}
	state.toRemove = map[string]struct{}{}
	cfg := ptreconcile.Config{
		Settings: settings,
		Stores:   s0.GetStores().(*kvserver.Stores),
		DB:       s0.DB(),
		Storage:  ptp,
		Cache:    ptp,
		StatusFuncs: ptreconcile.StatusFuncs{
			testTaskType: func(
				ctx context.Context, txn *kv.Txn, meta []byte,
			) (shouldRemove bool, err error) {
				state.mu.Lock()
				defer state.mu.Unlock()
				_, shouldRemove = state.toRemove[string(meta)]
				return shouldRemove, nil
			},
		},
	}
	r := ptreconcile.NewReconciler(cfg)
	require.NoError(t, r.Start(ctx, tc.Stopper()))
	recMeta := "a"
	rec1 := ptpb.Record{
		ID:        uuid.MakeV4(),
		Timestamp: s0.Clock().Now(),
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  testTaskType,
		Meta:      []byte(recMeta),
		Spans: []roachpb.Span{
			{Key: keys.MinKey, EndKey: keys.MaxKey},
		},
	}
	require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return ptp.Protect(ctx, txn, &rec1)
	}))

	t.Run("update settings", func(t *testing.T) {
		ptreconcile.ReconcileInterval.Override(&settings.SV, time.Millisecond)
		testutils.SucceedsSoon(t, func() error {
			require.Equal(t, int64(0), r.Metrics().RecordsRemoved.Count())
			require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
			if processed := r.Metrics().RecordsProcessed.Count(); processed < 1 {
				return errors.Errorf("expected processed to be at least 1, got %d", processed)
			}
			return nil
		})
	})
	t.Run("reconcile", func(t *testing.T) {
		state.mu.Lock()
		state.toRemove[recMeta] = struct{}{}
		state.mu.Unlock()

		ptreconcile.ReconcileInterval.Override(&settings.SV, time.Millisecond)
		testutils.SucceedsSoon(t, func() error {
			require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
			if removed := r.Metrics().RecordsRemoved.Count(); removed != 1 {
				return errors.Errorf("expected processed to be 1, got %d", removed)
			}
			return nil
		})
		require.Regexp(t, protectedts.ErrNotExists, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := ptp.GetRecord(ctx, txn, rec1.ID)
			return err
		}))
	})
}
