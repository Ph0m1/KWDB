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

package sql

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// jobSchedulerEnv returns JobSchedulerEnv.
func jobSchedulerEnv(params runParams) scheduledjobs.JobSchedulerEnv {
	if knobs, ok := params.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			return knobs.JobSchedulerEnv
		}
	}
	return scheduledjobs.ProdJobSchedulerEnv
}

// loadSchedule loads schedule information.
func loadSchedule(params runParams, scheduleName tree.Name) (*jobs.ScheduledJob, error) {
	env := jobSchedulerEnv(params)
	schedule := jobs.NewScheduledJob(env)

	// Load schedule expression.  This is needed for resume command, but we
	// also use this query to check for the schedule existence.
	datums, cols, err := params.ExecCfg().InternalExecutor.QueryWithCols(
		params.ctx,
		"load-schedule",
		params.EvalContext().Txn, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"SELECT schedule_id, schedule_expr FROM %s WHERE schedule_name = $1",
			env.ScheduledJobsTableName(),
		),
		string(scheduleName))
	if err != nil {
		return nil, err
	}

	// Not an error if schedule does not exist.
	if len(datums) != 1 {
		return nil, nil
	}

	if err := schedule.InitFromDatums(datums[0], cols); err != nil {
		return nil, err
	}
	return schedule, nil
}

// updateSchedule executes update for the schedule.
func updateSchedule(params runParams, schedule *jobs.ScheduledJob) error {
	return schedule.Update(
		params.ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
	)
}

// deleteSchedule deletes specified schedule.
func deleteSchedule(params runParams, scheduleID int64) error {
	env := jobSchedulerEnv(params)
	_, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.ctx,
		"delete-schedule",
		params.EvalContext().Txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"DELETE FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID,
	)
	return err
}
