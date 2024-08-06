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

package ptverifier_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvclient/kvcoord"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptstorage"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptverifier"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestVerifier tests the business logic of verification by mocking out the
// actual verification requests but using a real implementation of
// protectedts.Storage.
func TestVerifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	var senderFunc atomic.Value
	senderFunc.Store(kv.SenderFunc(nil))
	ds := s.DistSenderI().(*kvcoord.DistSender)
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx:        s.DB().AmbientContext,
			HeartbeatInterval: time.Second,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		kv.SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if f := senderFunc.Load().(kv.SenderFunc); f != nil {
				return f(ctx, ba)
			}
			return ds.Send(ctx, ba)
		}),
	)

	pts := ptstorage.New(s.ClusterSettings(), s.InternalExecutor().(sqlutil.InternalExecutor))
	withDB := ptstorage.WithDatabase(pts, s.DB())
	db := kv.NewDB(s.DB().AmbientContext, tsf, s.Clock())
	ptv := ptverifier.New(db, pts)
	makeTableSpan := func(tableID uint32) roachpb.Span {
		k := roachpb.Key(keys.MakeTablePrefix(tableID))
		return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	}

	createRecord := func(t *testing.T, tables ...uint32) *ptpb.Record {
		spans := make([]roachpb.Span, len(tables))
		for i, tid := range tables {
			spans[i] = makeTableSpan(tid)
		}
		r := ptpb.Record{
			ID:        uuid.MakeV4(),
			Timestamp: s.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			Spans:     spans,
		}
		require.Nil(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return pts.Protect(ctx, txn, &r)
		}))
		return &r
	}
	ensureVerified := func(t *testing.T, id uuid.UUID, verified bool) {
		got, err := withDB.GetRecord(ctx, nil, id)
		require.NoError(t, err)
		require.Equal(t, verified, got.Verified)
	}
	release := func(t *testing.T, id uuid.UUID) {
		require.NoError(t, withDB.Release(ctx, nil, id))
	}
	for _, c := range []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "record doesn't exist",
			test: func(t *testing.T) {
				require.Regexp(t, protectedts.ErrNotExists.Error(),
					ptv.Verify(ctx, uuid.MakeV4()).Error())
			},
		},
		{
			name: "verification failed with injected error",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						return nil, roachpb.NewError(errors.New("boom"))
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "boom", ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification failed with injected response",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							FailedRanges: []roachpb.RangeDescriptor{{
								RangeID:  42,
								StartKey: roachpb.RKey(r.Spans[0].Key),
								EndKey:   roachpb.RKey(r.Spans[0].EndKey),
							}},
						})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "failed to verify protection.*r42", ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification failed with injected response over two spans",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42, 12)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							FailedRanges: []roachpb.RangeDescriptor{{
								RangeID:  42,
								StartKey: roachpb.RKey(r.Spans[0].Key),
								EndKey:   roachpb.RKey(r.Spans[0].EndKey),
							}},
						})
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							FailedRanges: []roachpb.RangeDescriptor{{
								RangeID:  12,
								StartKey: roachpb.RKey(r.Spans[1].Key),
								EndKey:   roachpb.RKey(r.Spans[1].EndKey),
							}},
						})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "failed to verify protection.*r42.*r12", ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification succeeded",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.NoError(t, ptv.Verify(ctx, r.ID))
				ensureVerified(t, r.ID, true)
				// Show that we don't send again once we've already verified.
				sawVerification := false
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						sawVerification = true
					}
					return ds.Send(ctx, ba)
				}))
				require.NoError(t, ptv.Verify(ctx, r.ID))
				require.False(t, sawVerification)
				release(t, r.ID)
			},
		},
	} {
		t.Run(c.name, c.test)
	}
}
