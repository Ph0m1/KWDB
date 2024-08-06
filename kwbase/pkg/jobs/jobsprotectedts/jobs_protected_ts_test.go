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

package jobsprotectedts_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobsprotectedts"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/protectedts/ptpb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestJobsProtectedTimestamp is an end-to-end test of protected timestamp
// reconciliation for jobs.
func TestJobsProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	s0 := tc.Server(0)
	ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")
	jr := s0.JobRegistry().(*jobs.Registry)
	mkJobRec := func() jobs.Record {
		return jobs.Record{
			Description: "testing",
			Statement:   "SELECT 1",
			Username:    "root",
			Details: jobspb.SchemaChangeGCDetails{
				Tables: []jobspb.SchemaChangeGCDetails_DroppedID{
					{
						ID:       42,
						DropTime: s0.Clock().PhysicalNow(),
					},
				},
			},
			Progress:      jobspb.SchemaChangeGCProgress{},
			DescriptorIDs: []sqlbase.ID{42},
		}
	}
	mkJobAndRecord := func() (j *jobs.Job, rec *ptpb.Record) {
		ts := s0.Clock().Now()
		require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			if j, err = jr.CreateJobWithTxn(ctx, mkJobRec(), txn); err != nil {
				return err
			}
			rec = jobsprotectedts.MakeRecord(uuid.MakeV4(), *j.ID(), ts, []roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}})
			return ptp.Protect(ctx, txn, rec)
		}))
		return j, rec
	}
	jMovedToFailed, recMovedToFailed := mkJobAndRecord()
	require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return jr.Failed(ctx, txn, *jMovedToFailed.ID(), io.ErrUnexpectedEOF)
	}))
	jFinished, recFinished := mkJobAndRecord()
	require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return jr.Succeeded(ctx, txn, *jFinished.ID())
	}))
	_, recRemains := mkJobAndRecord()
	ensureNotExists := func(ctx context.Context, txn *kv.Txn, ptsID uuid.UUID) (err error) {
		_, err = ptp.GetRecord(ctx, txn, ptsID)
		if err == protectedts.ErrNotExists {
			return nil
		}
		return fmt.Errorf("waiting for %v, got %v", protectedts.ErrNotExists, err)
	}
	testutils.SucceedsSoon(t, func() (err error) {
		return s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := ensureNotExists(ctx, txn, recMovedToFailed.ID); err != nil {
				return err
			}
			if err := ensureNotExists(ctx, txn, recFinished.ID); err != nil {
				return err
			}
			_, err := ptp.GetRecord(ctx, txn, recRemains.ID)
			require.NoError(t, err)
			return err
		})
	})

	// Verify that the two jobs we just observed as removed were recorded in the
	// metrics.
	var removed int
	runner.QueryRow(t, `
SELECT
    value
FROM
    kwdb_internal.node_metrics
WHERE
    name = 'kv.protectedts.reconciliation.records_removed';
`).Scan(&removed)
	require.Equal(t, 2, removed)
}
