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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

type statusTrackingExecutor struct {
	numExec int
	counts  map[Status]int
}

func (s *statusTrackingExecutor) ExecuteJob(
	_ context.Context,
	_ *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	_ *ScheduledJob,
	_ *kv.Txn,
) error {
	s.numExec++
	return nil
}

func (s *statusTrackingExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID int64,
	jobStatus Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	s.counts[jobStatus]++
	return nil
}

func (s *statusTrackingExecutor) Metrics() metric.Struct {
	return nil
}

var _ ScheduledJobExecutor = &statusTrackingExecutor{}

func newStatusTrackingExecutor() *statusTrackingExecutor {
	return &statusTrackingExecutor{counts: make(map[Status]int)}
}

func TestScheduledJobExecutorRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const executorName = "test-executor"
	instance := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, instance)()

	registered, err := GetScheduledJobExecutor(executorName)
	require.NoError(t, err)
	require.Equal(t, instance, registered)
}

func TestJobTerminationNotification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	const executorName = "test-executor"
	ex := newStatusTrackingExecutor()
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	// Create a single job.
	schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
	ctx := context.Background()
	require.NoError(t, schedule.Create(ctx, h.cfg.InternalExecutor, nil))

	// Pretend it completes multiple runs with terminal statuses.
	for _, s := range []Status{StatusCanceled, StatusFailed, StatusSucceeded} {
		require.NoError(t, NotifyJobTermination(
			ctx, h.env, 123, s, nil, schedule.ScheduleID(), h.cfg.InternalExecutor, nil))
	}

	// Verify counts.
	require.Equal(t, map[Status]int{StatusSucceeded: 1, StatusFailed: 1, StatusCanceled: 1}, ex.counts)
}
