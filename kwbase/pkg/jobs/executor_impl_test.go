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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func TestInlineExecutorFailedJobsHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer TestingSetAdoptAndCancelIntervals(time.Millisecond, time.Microsecond)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	var tests = []struct {
		onError         jobspb.ScheduleDetails_ErrorHandlingBehavior
		expectedNextRun time.Time
	}{
		{
			onError:         jobspb.ScheduleDetails_RETRY_SCHED,
			expectedNextRun: cronexpr.MustParse("@daily").Next(h.env.Now()).Round(time.Microsecond),
		},
		{
			onError:         jobspb.ScheduleDetails_RETRY_SOON,
			expectedNextRun: h.env.Now().Add(retryFailedJobAfter).Round(time.Microsecond),
		},
		{
			onError:         jobspb.ScheduleDetails_PAUSE_SCHED,
			expectedNextRun: time.Time{}.UTC(),
		},
	}

	for _, test := range tests {
		t.Run(test.onError.String(), func(t *testing.T) {
			j := h.newScheduledJob(t, "test_job", "test sql")
			j.rec.ExecutorType = InlineExecutorName

			require.NoError(t, j.SetSchedule("@daily"))
			j.SetScheduleDetails(jobspb.ScheduleDetails{OnError: test.onError})

			ctx := context.Background()
			require.NoError(t, j.Create(ctx, h.cfg.InternalExecutor, nil))

			// Pretend we failed running; we expect job to be rescheduled.
			require.NoError(t, NotifyJobTermination(
				ctx, h.env, 123, StatusFailed, nil, j.ScheduleID(), h.cfg.InternalExecutor, nil))

			// Verify nextRun updated
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, test.expectedNextRun, loaded.NextRun())
		})
	}
}
