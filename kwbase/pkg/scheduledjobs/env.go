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

package scheduledjobs

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// JobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type JobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the scheduled_jobs table.
	ScheduledJobsTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
}

// JobExecutionConfig encapsulates external components needed for scheduled job execution.
type JobExecutionConfig struct {
	Settings         *cluster.Settings
	InternalExecutor sqlutil.InternalExecutor
	DB               *kv.DB
	// TestingKnobs is *jobs.TestingKnobs; however we cannot depend
	// on jobs package due to circular dependencies.
	TestingKnobs base.ModuleTestingKnobs
	// PlanHookMaker is responsible for creating sql.NewInternalPlanner. It returns an
	// *sql.planner as an interface{} due to package dependency cycles. It should
	// be cast to that type in the sql package when it is used. Returns a cleanup
	// function that must be called once the caller is done with the planner.
	// This is the same mechanism used in jobs.Registry.
	PlanHookMaker func(opName string, tnx *kv.Txn, user string) (interface{}, func())
	// StartMode means kwbase start mode
	StartMode int
}

// production JobSchedulerEnv implementation.
type prodJobSchedulerEnvImpl struct{}

// ProdJobSchedulerEnv is a JobSchedulerEnv implementation suitable for production.
var ProdJobSchedulerEnv JobSchedulerEnv = &prodJobSchedulerEnvImpl{}

func (e *prodJobSchedulerEnvImpl) ScheduledJobsTableName() string {
	return "system.scheduled_jobs"
}

func (e *prodJobSchedulerEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnvImpl) NowExpr() string {
	return "current_timestamp()"
}
