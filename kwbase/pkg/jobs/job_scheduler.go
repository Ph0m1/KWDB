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
	"math/rand"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// start mode
const (
	startSingleReplica int = iota // 0
	startSingleNode
	startMultiReplica
)

// CreatedByScheduledJobs identifies the job that was created
// by scheduled jobs system.
const CreatedByScheduledJobs = "kwdb_schedule"

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	*scheduledjobs.JobExecutionConfig
	env      scheduledjobs.JobSchedulerEnv
	registry *metric.Registry
	metrics  SchedulerMetrics
}

func newJobScheduler(
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	registry *metric.Registry,
) *jobScheduler {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}

	stats := MakeSchedulerMetrics()
	registry.AddMetricStruct(stats)

	return &jobScheduler{
		JobExecutionConfig: cfg,
		env:                env,
		registry:           registry,
		metrics:            stats,
	}
}

const allSchedules = 0

// getFindSchedulesStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func getFindSchedulesStatement(env scheduledjobs.JobSchedulerEnv, maxSchedules int64) string {
	limitClause := ""
	if maxSchedules != allSchedules {
		limitClause = fmt.Sprintf("LIMIT %d", maxSchedules)
	}

	return fmt.Sprintf(
		`
SELECT
  (SELECT count(*)
   FROM %s J
   WHERE
      J.created_by_id = S.schedule_id AND
      J.status NOT IN ('%s', '%s', '%s')
  ) AS num_running, S.*
FROM %s S
WHERE next_run < %s
ORDER BY random()
%s
FOR UPDATE`, env.SystemJobsTableName(),
		StatusSucceeded, StatusCanceled, StatusFailed,
		env.ScheduledJobsTableName(), env.NowExpr(), limitClause)
}

// unmarshalScheduledJob is a helper to deserialize a row returned by
// getFindSchedulesStatement() into a ScheduledJob
func (s *jobScheduler) unmarshalScheduledJob(
	row []tree.Datum, cols []sqlbase.ResultColumn,
) (*ScheduledJob, int64, error) {
	j := NewScheduledJob(s.env)
	if err := j.InitFromDatums(row[1:], cols[1:]); err != nil {
		return nil, 0, err
	}

	if n, ok := row[0].(*tree.DInt); ok {
		return j, int64(*n), nil
	}

	return nil, 0, errors.Newf("expected int found %T instead", row[0])
}

const recheckRunningAfter = 1 * time.Minute

type loopStats struct {
	rescheduleWait, rescheduleSkip, started int64
	readyToRun, jobsRunning                 int64
}

func (s *loopStats) updateMetrics(m *SchedulerMetrics) {
	m.NumStarted.Update(s.started)
	m.ReadyToRun.Update(s.readyToRun)
	m.NumRunning.Update(s.jobsRunning)
	m.RescheduleSkip.Update(s.rescheduleSkip)
	m.RescheduleWait.Update(s.rescheduleWait)
}

func (s *jobScheduler) processSchedule(
	ctx context.Context, schedule *ScheduledJob, numRunning int64, stats *loopStats, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch schedule.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			schedule.SetScheduleStatus("delayed due to %d already running", numRunning)
			stats.rescheduleWait++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.SetScheduleStatus("rescheduled due to %d already running", numRunning)
			stats.rescheduleSkip++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		}
	}

	schedule.ClearScheduleStatus()

	// Schedule the next job run.
	// We do this step early, before the actual execution, to grab a lock on
	// the scheduledjobs table.
	if schedule.HasRecurringSchedule() {
		if err := schedule.ScheduleNextRun(); err != nil {
			return err
		}
	} else {
		// It's a one-off schedule.  Clear next run to indicate that this schedule executed.
		schedule.SetNextRun(time.Time{})
	}

	if err := schedule.Update(ctx, s.InternalExecutor, txn); err != nil {
		return err
	}

	executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	log.Infof(ctx,
		"Starting job for schedule %d (%s); next run scheduled for %s",
		schedule.ScheduleID(), schedule.ScheduleLabel(), schedule.NextRun())

	if err := executor.ExecuteJob(ctx, s.JobExecutionConfig, s.env, schedule, txn); err != nil {
		return errors.Wrapf(err, "executing schedule %d failed", schedule.ScheduleID())
	}

	stats.started++

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.InternalExecutor, txn)
}

// TODO(yevgeniy): Re-evaluate if we need to have per-loop execution statistics.
func newLoopStats(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, ex sqlutil.InternalExecutor, txn *kv.Txn,
) (*loopStats, error) {
	numRunningJobsStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE created_by_type IS NOT NULL AND status NOT IN ('%s', '%s', '%s')",
		env.SystemJobsTableName(), StatusSucceeded, StatusCanceled, StatusFailed)
	readyToRunStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE next_run < %s",
		env.ScheduledJobsTableName(), env.NowExpr())
	statsStmt := fmt.Sprintf(
		"SELECT (%s) numReadySchedules, (%s) numRunningJobs",
		readyToRunStmt, numRunningJobsStmt)

	datums, err := ex.QueryRowEx(ctx, "scheduler-stats", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		statsStmt)
	if err != nil {
		return nil, err
	}
	if datums == nil {
		return nil, errors.New("failed to read scheduler stats")
	}
	stats := &loopStats{}
	stats.readyToRun = int64(tree.MustBeDInt(datums[0]))
	stats.jobsRunning = int64(tree.MustBeDInt(datums[1]))
	return stats, nil
}

type savePointError struct {
	err error
}

func (e savePointError) Error() string {
	return e.err.Error()
}

// withSavePoint executes function fn() wrapped with savepoint.
// The savepoint is either released (upon successful completion of fn())
// or it is rolled back.
// If an error occurs while performing savepoint operations, an instance
// of savePointError is returned.  If fn() returns an error, then that
// error is returned.
func withSavePoint(ctx context.Context, txn *kv.Txn, fn func() error) error {
	sp, err := txn.CreateSavepoint(ctx)
	if err != nil {
		return &savePointError{err}
	}
	execErr := fn()

	if execErr == nil {
		if err := txn.ReleaseSavepoint(ctx, sp); err != nil {
			return &savePointError{err}
		}
		return nil
	}

	if errors.HasType(execErr, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) {
		// If function execution failed because transaction was restarted,
		// treat this error as a savePointError so that the execution code bails out
		// and retries scheduling loop.
		return &savePointError{execErr}
	}

	if err := txn.RollbackToSavepoint(ctx, sp); err != nil {
		return &savePointError{errors.WithDetail(err, execErr.Error())}
	}
	return execErr
}

func (s *jobScheduler) executeSchedules(
	ctx context.Context, maxSchedules int64, txn *kv.Txn,
) error {
	stats, err := newLoopStats(ctx, s.env, s.InternalExecutor, txn)
	if err != nil {
		return err
	}

	defer stats.updateMetrics(&s.metrics)

	findSchedulesStmt := getFindSchedulesStatement(s.env, maxSchedules)
	rows, cols, err := s.InternalExecutor.QueryWithCols(
		ctx, "find-scheduled-jobs",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		findSchedulesStmt)

	if err != nil {
		return err
	}

	for _, row := range rows {
		schedule, numRunning, err := s.unmarshalScheduledJob(row, cols)
		if err != nil {
			s.metrics.NumBadSchedules.Inc(1)
			log.Errorf(ctx, "error parsing schedule: %+v", row)
			continue
		}

		if processErr := withSavePoint(ctx, txn, func() error {
			return s.processSchedule(ctx, schedule, numRunning, stats, txn)
		}); processErr != nil {
			if errors.HasType(processErr, (*savePointError)(nil)) {
				return errors.Wrapf(processErr, "savepoint error for schedule %d", schedule.ScheduleID())
			}

			// Failed to process schedule.
			s.metrics.NumBadSchedules.Inc(1)
			log.Errorf(ctx,
				"error processing schedule %d: %+v", schedule.ScheduleID(), processErr)

			// Try updating schedule record to indicate schedule execution error.
			if err := withSavePoint(ctx, txn, func() error {
				// Discard changes already made to the schedule, and treat schedule
				// execution failure the same way we treat job failure.
				schedule.ClearDirty()
				DefaultHandleFailedRun(schedule,
					"failed to create job for schedule %d: err=%s",
					schedule.ScheduleID(), processErr)

				// DefaultHandleFailedRun assumes schedule already had its next run set.
				// So, if the policy is to reschedule based on regular recurring schedule,
				// we need to set next run again..
				if schedule.HasRecurringSchedule() &&
					schedule.ScheduleDetails().OnError == jobspb.ScheduleDetails_RETRY_SCHED {
					if err := schedule.ScheduleNextRun(); err != nil {
						return err
					}
				}
				return schedule.Update(ctx, s.InternalExecutor, txn)
			}); err != nil {
				if errors.HasType(err, (*savePointError)(nil)) {
					return errors.Wrapf(err,
						"savepoint error for schedule %d", schedule.ScheduleID())
				}
				log.Errorf(ctx, "error recording processing error for schedule %d: %+v",
					schedule.ScheduleID(), err)
			}
		}
	}

	return nil
}

// runDaemon start a background worker to scan available schedules periodically
func (s *jobScheduler) runDaemon(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		var initialDelay time.Duration
		var testPace time.Duration
		var isTest = false
		// TestRetentionsInterval is only used for regression test to set a pace period.
		// So that we can get a pace period of less than 60s.It will not be set in production.
		TestRetentionsInterval := envutil.EnvOrDefaultInt("KWDB_RETENTIONS_INTERVAL", -1)
		if TestRetentionsInterval != -1 {
			isTest = true
			testPace = time.Duration(TestRetentionsInterval) * time.Second
		}
		if !isTest {
			initialDelay = getInitialScanDelay(s.TestingKnobs)
		}
		log.Infof(ctx, "waiting %v before scheduled jobs daemon start", initialDelay)

		if err := RegisterExecutorsMetrics(s.registry); err != nil {
			log.Errorf(ctx, "error registering executor metrics: %+v", err)
		}
		pace := getWaitPeriod(&s.Settings.SV, s.TestingKnobs)
		for timer := time.NewTimer(initialDelay); ; {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if isTest {
					timer.Reset(testPace)
				} else {
					timer.Reset(pace)
				}
				pace = getWaitPeriod(&s.Settings.SV, s.TestingKnobs)
				if !schedulerEnabledSetting.Get(&s.Settings.SV) {
					log.Info(ctx, "scheduled job daemon disabled")
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return s.executeSchedules(ctx, maxSchedules, txn)
				})
				if err != nil {
					log.Errorf(ctx, "error executing schedules: %+v", err)
				}

			}
		}
	})
}

var schedulerEnabledSetting = settings.RegisterBoolSetting(
	"jobs.scheduler.enabled",
	"enable/disable job scheduler",
	true,
)

var schedulerPaceSetting = settings.RegisterDurationSetting(
	"jobs.scheduler.pace",
	"how often to scan system.scheduled_jobs table",
	time.Minute,
)

var schedulerMaxJobsPerIterationSetting = settings.RegisterIntSetting(
	"jobs.scheduler.max_jobs_per_iteration",
	"how many schedules to start per iteration; setting to 0 turns off this limit",
	10,
)

// Returns the amount of time to wait before starting initial scan.
func getInitialScanDelay(knobs base.ModuleTestingKnobs) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonInitialScanDelay != nil {
		return k.SchedulerDaemonInitialScanDelay()
	}

	// By default, we'll wait between 2 and 5 minutes before performing initial scan.
	return time.Minute * time.Duration(2+rand.Intn(3))
}

// Fastest pace for the scheduler.
const minPacePeriod = 60 * time.Second

// Frequency to recheck if the daemon is enabled.
const recheckEnabledAfterPeriod = 5 * time.Minute

var warnIfPaceTooLow = log.Every(time.Minute)

// Returns duration to wait before scanning system.scheduled_jobs.
func getWaitPeriod(sv *settings.Values, knobs base.ModuleTestingKnobs) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonScanDelay != nil {
		return k.SchedulerDaemonScanDelay()
	}

	if !schedulerEnabledSetting.Get(sv) {
		return recheckEnabledAfterPeriod
	}
	pace := schedulerPaceSetting.Get(sv)
	if pace < minPacePeriod {
		if warnIfPaceTooLow.ShouldLog() {
			log.Warningf(context.Background(),
				"job.scheduler.pace setting too low (%s < %s)", pace, minPacePeriod)
		}
		pace = minPacePeriod
	}

	return pace
}

// StartJobSchedulerDaemon starts a daemon responsible for periodically scanning
// system.scheduled_jobs table to find and executing eligible scheduled jobs.
func StartJobSchedulerDaemon(
	ctx context.Context,
	stopper *stop.Stopper,
	registry *metric.Registry,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
) {
	schedulerEnv := env
	var daemonKnobs *TestingKnobs
	if jobsKnobs, ok := cfg.TestingKnobs.(*TestingKnobs); ok {
		daemonKnobs = jobsKnobs
	}

	if daemonKnobs != nil && daemonKnobs.CaptureJobExecutionConfig != nil {
		daemonKnobs.CaptureJobExecutionConfig(cfg)
	}
	if daemonKnobs != nil && daemonKnobs.JobSchedulerEnv != nil {
		schedulerEnv = daemonKnobs.JobSchedulerEnv
	}

	scheduler := newJobScheduler(cfg, schedulerEnv, registry)

	if daemonKnobs != nil && daemonKnobs.TakeOverJobsScheduling != nil {
		daemonKnobs.TakeOverJobsScheduling(
			func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error {
				return scheduler.executeSchedules(ctx, maxSchedules, txn)
			})
		return
	}

	scheduler.runDaemon(ctx, stopper)
}
