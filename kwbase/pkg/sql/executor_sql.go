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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"github.com/gogo/protobuf/types"
)

// SQLExecutorName is the name associated with scheduled job executor which
// create by user to execute sql statement.
const SQLExecutorName = "scheduled-sql-executor"

// ScheduledSQLExecutor implements ScheduledJobExecutor interface.
type ScheduledSQLExecutor struct{}

var _ jobs.ScheduledJobExecutor = &ScheduledSQLExecutor{}

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *ScheduledSQLExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	args := &jobspb.SqlStatementExecutionArg{}
	if err := types.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return err
	}
	stmt := args.Statement

	user := security.NodeUser
	phs, cleanup := cfg.PlanHookMaker(SQLExecutorName, txn, user)
	defer cleanup()
	innerPlaner := phs.(PlanHookState)
	// create a job and start it
	jobRegistry := innerPlaner.ExecCfg().JobRegistry
	sqlScheduleDetail := jobspb.SqlScheduleDetails{
		ScheduleType: sqlSchedule,
		Statement:    stmt,
	}
	jobRecord := jobs.Record{
		Description: stmt,
		Username:    user,
		CreatedBy: &jobs.CreatedByInfo{
			Name: schedule.ScheduleLabel(),
			ID:   schedule.ScheduleID(),
		},
		Details:  sqlScheduleDetail,
		Progress: jobspb.SqlScheduleProgress{},
	}
	job, err := jobRegistry.CreateJobWithTxn(ctx, jobRecord, txn)
	if err != nil {
		return err
	}
	log.Infof(ctx, "sql schedule creates new job %d", job.ID())

	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *ScheduledSQLExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID int64,
	jobStatus jobs.Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	// For now, only interested in failed status.
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(schedule, "job %d failed", jobID)
	}
	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *ScheduledSQLExecutor) Metrics() metric.Struct {
	return nil
}
