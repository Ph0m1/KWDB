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

package jobstest

import (
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// EnvTablesType tells JobSchedulerTestEnv whether to use the system tables,
// or to use test tables.
type EnvTablesType bool

// UseTestTables instructs JobSchedulerTestEnv to use test tables.
const UseTestTables EnvTablesType = false

// UseSystemTables instructs JobSchedulerTestEnv to use system tables.
const UseSystemTables EnvTablesType = true

// NewJobSchedulerTestEnv creates JobSchedulerTestEnv and initializes environments
// current time to initial time.
func NewJobSchedulerTestEnv(whichTables EnvTablesType, t time.Time) *JobSchedulerTestEnv {
	var env *JobSchedulerTestEnv
	if whichTables == UseTestTables {
		env = &JobSchedulerTestEnv{
			scheduledJobsTableName: "defaultdb.scheduled_jobs",
			jobsTableName:          "defaultdb.system_jobs",
		}
	} else {
		env = &JobSchedulerTestEnv{
			scheduledJobsTableName: "system.scheduled_jobs",
			jobsTableName:          "system.jobs",
		}
	}
	env.mu.now = t
	return env
}

// JobSchedulerTestEnv is a job scheduler environment with an added ability to
// manipulate time.
type JobSchedulerTestEnv struct {
	scheduledJobsTableName string
	jobsTableName          string
	mu                     struct {
		syncutil.Mutex
		now time.Time
	}
}

var _ scheduledjobs.JobSchedulerEnv = &JobSchedulerTestEnv{}

// ScheduledJobsTableName implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) ScheduledJobsTableName() string {
	return e.scheduledJobsTableName
}

// SystemJobsTableName implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) SystemJobsTableName() string {
	return e.jobsTableName
}

// Now implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) Now() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.now
}

// AdvanceTime implements JobSchedulerTestEnv
func (e *JobSchedulerTestEnv) AdvanceTime(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = e.mu.now.Add(d)
}

// SetTime implements JobSchedulerTestEnv
func (e *JobSchedulerTestEnv) SetTime(t time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = t
}

const timestampTZLayout = "2006-01-02 15:04:05.000000"

// NowExpr implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) NowExpr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return fmt.Sprintf("TIMESTAMPTZ '%s'", e.mu.now.Format(timestampTZLayout))
}

// GetScheduledJobsTableSchema returns schema for the scheduled jobs table.
func GetScheduledJobsTableSchema(env scheduledjobs.JobSchedulerEnv) string {
	if env.ScheduledJobsTableName() == "system.jobs" {
		return sqlbase.ScheduledJobsTableSchema
	}
	return strings.Replace(sqlbase.ScheduledJobsTableSchema,
		"system.scheduled_jobs", env.ScheduledJobsTableName(), 1)
}

// GetJobsTableSchema returns schema for the jobs table.
func GetJobsTableSchema(env scheduledjobs.JobSchedulerEnv) string {
	if env.SystemJobsTableName() == "system.jobs" {
		return sqlbase.JobsTableSchema
	}
	return strings.Replace(sqlbase.JobsTableSchema,
		"system.jobs", env.SystemJobsTableName(), 1)
}
