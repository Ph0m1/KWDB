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

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestScheduleControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	t.Run("non-existent", func(t *testing.T) {
		for _, command := range []string{
			"PAUSE SCHEDULE IF EXISTS schedule1",
			"RESUME SCHEDULE IF EXISTS schedule1",
		} {
			t.Run(command, func(t *testing.T) {
				th.sqlDB.ExecRowsAffected(t, 0, command)
			})
		}
	})

	ctx := context.Background()

	makeSchedule := func(name string, cron string) int64 {
		schedule := th.newScheduledJob(t, name, "sql")
		if cron != "" {
			require.NoError(t, schedule.SetSchedule(cron))
		}
		require.NoError(t, schedule.Create(ctx, th.cfg.InternalExecutor, nil))
		return schedule.ScheduleID()
	}

	t.Run("pause-one-schedule", func(t *testing.T) {
		scheduleID := makeSchedule("schedule1", "@daily")
		th.sqlDB.Exec(t, "PAUSE SCHEDULE schedule1")
		require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
	})

	t.Run("pause-and-resume-one-schedule", func(t *testing.T) {
		scheduleID := makeSchedule("schedule2", "@daily")
		th.sqlDB.Exec(t, "PAUSE SCHEDULE schedule2")
		require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
		th.sqlDB.Exec(t, "RESUME SCHEDULE schedule2")

		schedule := th.loadSchedule(t, scheduleID)
		require.False(t, schedule.IsPaused())
	})
}
