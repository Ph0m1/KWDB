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

package jobs

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobstest"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

type execSchedulesFn func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error
type testHelper struct {
	env           *jobstest.JobSchedulerTestEnv
	server        serverutils.TestServerInterface
	execSchedules execSchedulesFn
	cfg           *scheduledjobs.JobExecutionConfig
	sqlDB         *sqlutils.SQLRunner
}

// newTestHelper creates and initializes appropriate state for a test,
// returning testHelper as well as a cleanup function.
// This test helper does not use system tables for jobs and scheduled jobs.
// It creates separate tables for the test, that are then dropped when cleanup
// function executes.  Because of this, the execution of job scheduler daemon
// is disabled by this test helper.
// If you want to run daemon, invoke it directly.
//
// The testHelper will accelerate the adoption and cancellation loops inside of
// the registry.
func newTestHelper(t *testing.T) (*testHelper, func()) {
	return newTestHelperForTables(t, jobstest.UseTestTables)
}

func newTestHelperForTables(
	t *testing.T, envTableType jobstest.EnvTablesType,
) (*testHelper, func()) {
	var execSchedules execSchedulesFn

	// Setup test scheduled jobs table.
	env := jobstest.NewJobSchedulerTestEnv(envTableType, timeutil.Now())
	knobs := &TestingKnobs{
		JobSchedulerEnv: env,
		TakeOverJobsScheduling: func(daemon func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error) {
			execSchedules = daemon
		},
	}
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{JobsTestingKnobs: knobs},
	})

	sqlDB := sqlutils.MakeSQLRunner(db)

	if envTableType == jobstest.UseTestTables {
		sqlDB.Exec(t, jobstest.GetScheduledJobsTableSchema(env))
		sqlDB.Exec(t, jobstest.GetJobsTableSchema(env))
	}

	restoreRegistry := settings.TestingSaveRegistry()
	return &testHelper{
			env:    env,
			server: s,
			cfg: &scheduledjobs.JobExecutionConfig{
				Settings:         s.ClusterSettings(),
				InternalExecutor: s.InternalExecutor().(sqlutil.InternalExecutor),
				DB:               kvDB,
				TestingKnobs:     knobs,
			},
			sqlDB:         sqlDB,
			execSchedules: execSchedules,
		}, func() {
			if envTableType == jobstest.UseTestTables {
				sqlDB.Exec(t, "DROP TABLE "+env.SystemJobsTableName())
				sqlDB.Exec(t, "DROP TABLE "+env.ScheduledJobsTableName())
			}
			s.Stopper().Stop(context.Background())
			restoreRegistry()
		}
}

// newScheduledJob is a helper to create scheduled job with helper environment.
func (h *testHelper) newScheduledJob(t *testing.T, scheduleLabel, sql string) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleLabel(scheduleLabel)
	j.SetOwner("test")
	any, err := types.MarshalAny(&jobspb.SqlStatementExecutionArg{Statement: sql})
	require.NoError(t, err)
	j.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	return j
}

// newScheduledJobForExecutor is a helper to create scheduled job for the specified
// executor and its args.
func (h *testHelper) newScheduledJobForExecutor(
	scheduleLabel, executorName string, executorArgs *types.Any,
) *ScheduledJob {
	j := NewScheduledJob(h.env)
	j.SetScheduleLabel(scheduleLabel)
	j.SetOwner("test")
	j.SetExecutionDetails(executorName, jobspb.ExecutionArguments{Args: executorArgs})
	return j
}

// loadSchedule loads  all columns for the specified scheduled job.
func (h *testHelper) loadSchedule(t *testing.T, id int64) *ScheduledJob {
	j := NewScheduledJob(h.env)
	rows, cols, err := h.cfg.InternalExecutor.QueryWithCols(
		context.Background(), "sched-load", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"SELECT * FROM %s WHERE schedule_id = %d",
			h.env.ScheduledJobsTableName(), id),
	)
	require.NoError(t, err)

	require.Equal(t, 1, len(rows))
	require.NoError(t, j.InitFromDatums(rows[0], cols))
	return j
}

// registerScopedScheduledJobExecutor registers executor under the name,
// and returns a function which, when invoked, de-registers this executor.
func registerScopedScheduledJobExecutor(name string, ex ScheduledJobExecutor) func() {
	RegisterScheduledJobExecutorFactory(
		name,
		func() (ScheduledJobExecutor, error) {
			return ex, nil
		})
	return func() {
		executorRegistry.Lock()
		defer executorRegistry.Unlock()
		delete(executorRegistry.factories, name)
		delete(executorRegistry.executors, name)
	}
}

// addFakeJob adds a fake job associated with the specified scheduleID.
// Returns the id of the newly created job.
func addFakeJob(t *testing.T, h *testHelper, scheduleID int64, status Status, txn *kv.Txn) int64 {
	payload := []byte("fake payload")
	datums, err := h.cfg.InternalExecutor.QueryRowEx(context.Background(), "fake-job", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(`
INSERT INTO %s (created_by_type, created_by_id, status, payload)
VALUES ($1, $2, $3, $4)
RETURNING id`,
			h.env.SystemJobsTableName(),
		),
		CreatedByScheduledJobs, scheduleID, status, payload,
	)
	require.NoError(t, err)
	require.NotNil(t, datums)
	return int64(tree.MustBeDInt(datums[0]))
}
