// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

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
)

// RetentionExecutorName is the name associated with scheduled job executor which
// runs jobs outstanding -- that is, it doesn't spawn external system.job to do its work.
const RetentionExecutorName = "scheduled-retention-executor"

// ScheduledRetentionExecutor implements ScheduledJobExecutor interface.
type ScheduledRetentionExecutor struct{}

var _ jobs.ScheduledJobExecutor = &ScheduledRetentionExecutor{}

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *ScheduledRetentionExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	user := security.NodeUser
	phs, cleanup := cfg.PlanHookMaker(RetentionExecutorName, txn, user)
	defer cleanup()
	innerPlaner := phs.(PlanHookState)
	// create a job and start it
	jobRegistry := innerPlaner.ExecCfg().JobRegistry
	syncDetail := jobspb.SyncMetaCacheDetails{
		Type: deleteExpiredData,
	}
	jobRecord := jobs.Record{
		Description: "retention ts tables",
		Username:    user,
		CreatedBy: &jobs.CreatedByInfo{
			Name: schedule.ScheduleLabel(),
			ID:   schedule.ScheduleID(),
		},
		Details:  syncDetail,
		Progress: jobspb.SyncMetaCacheProgress{},
	}
	var job *jobs.StartableJob
	var err1 error
	if err := cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err1 = jobRegistry.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil)
		if err1 != nil {
			cleanupErr := job.CleanupOnRollback(ctx)
			if cleanupErr != nil {
				return cleanupErr
			}
			return err1
		}
		return nil
	}); err != nil {
		return err
	}
	jobStatus := jobs.StatusRunning
	defer func() {
		err := jobs.NotifyJobTermination(ctx, env, *job.ID(), jobStatus, nil,
			schedule.ScheduleID(), cfg.InternalExecutor, txn)
		if err != nil {
			log.Warningf(ctx, "callback to update schedule [%s] failed. err:%s", schedule.ScheduleID(), err.Error())
		}
	}()
	if err := job.Run(ctx); err != nil {
		jobStatus = jobs.StatusFailed
		log.Error(ctx, "start retention job failed")
		return err
	}
	jobStatus = jobs.StatusSucceeded

	if jobUpdateErr := jobRegistry.Succeeded(ctx, txn, *job.ID()); jobUpdateErr != nil {
		log.Errorf(ctx, "update job status failed. err: %s", jobUpdateErr.Error())
	}

	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *ScheduledRetentionExecutor) NotifyJobTermination(
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
func (e *ScheduledRetentionExecutor) Metrics() metric.Struct {
	return nil
}
