// Copyright 2017 The Cockroach Authors.
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
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
)

type createScheduleNode struct {
	n *tree.CreateSchedule
	// schedule specific properties that get evaluated.
	scheduleName func() (string, error)
	recurrence   func() (string, error)
	scheduleOpts func() (map[string]string, error)
}

const scheduleExecSQLOp = "CREATE SCHEDULE FOR SQL"

// CreateSchedule creates a Schedule.
func (p *planner) CreateSchedule(ctx context.Context, n *tree.CreateSchedule) (planNode, error) {
	node := &createScheduleNode{n: n}
	var err error
	if n.ScheduleName != nil {
		node.scheduleName, err = p.TypeAsString(n.ScheduleName, scheduleExecSQLOp)
		if err != nil {
			return nil, err
		}
	}

	if n.Recurrence == nil {
		// sanity check: recurrence must be specified.
		return nil, pgerror.New(pgcode.InvalidParameterValue, "RECURRING clause required")
	}
	node.recurrence, err = p.TypeAsString(n.Recurrence, scheduleExecSQLOp)
	if err != nil {
		return nil, err
	}

	node.scheduleOpts, err = p.TypeAsStringOpts(n.ScheduleOptions, scheduledBackupOptionExpectValues)
	if err != nil {
		return nil, err
	}
	if isAdmin, err := p.HasAdminRole(ctx); !isAdmin {
		if err != nil {
			return nil, err
		}
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is not superuser or membership of admin, has no privilege to CREATE SCHEDULE",
			p.User())
	}

	return node, nil
}

func (n *createScheduleNode) startExec(params runParams) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := params.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	var fullScheduleName string
	if n.scheduleName != nil {
		scheduleName, err := n.scheduleName()
		if err != nil {
			return err
		}
		fullScheduleName = scheduleName
	} else {
		fullScheduleName = fmt.Sprintf("EXEC SQL %d", env.Now().Unix())
	}
	// check if the schedule already exists.
	schedule, err := loadSchedule(params, tree.Name(fullScheduleName))
	if err != nil {
		return err
	}
	if schedule != nil {
		if n.n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject, "schedule %q already exists", fullScheduleName)
	}
	// check if the target is correct
	err = params.p.checkScheduledSQL(params.ctx, n.n.SQL)
	if err != nil {
		return err
	}

	// parse cron expr
	cron, err := n.recurrence()
	if err != nil {
		return err
	}
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return err
	}
	scheduleOptions, err := n.scheduleOpts()
	if err != nil {
		return err
	}

	scheduleDetails := jobspb.ScheduleDetails{Wait: jobspb.ScheduleDetails_SKIP, OnError: jobspb.ScheduleDetails_RETRY_SCHED}
	if value, ok := scheduleOptions[optOnExecFailure]; ok {
		switch value {
		case "retry":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SOON
		case "reschedule":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SCHED
		case "pause":
			scheduleDetails.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "%s is not valid on_execution_failure parameter", value)
		}
	}

	if value, ok := scheduleOptions[optOnPreviousRunning]; ok {
		switch value {
		case "start":
			scheduleDetails.Wait = jobspb.ScheduleDetails_NO_WAIT
		case "skip":
			scheduleDetails.Wait = jobspb.ScheduleDetails_SKIP
		case "wait":
			scheduleDetails.Wait = jobspb.ScheduleDetails_WAIT
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "%s is not valid on_previous_running parameter", value)
		}
	}
	// make new schedule job
	schedule = jobs.NewScheduledJob(env)
	schedule.SetScheduleLabel(fullScheduleName)
	schedule.SetOwner(params.p.User())
	schedule.SetNextRun(expr.Next(env.Now()))
	schedule.SetScheduleDetails(scheduleDetails)
	if err := schedule.SetSchedule(cron); err != nil {
		return err
	}

	any, err := types.MarshalAny(
		&jobspb.SqlStatementExecutionArg{Statement: n.n.SQL})
	if err != nil {
		return err
	}
	schedule.SetExecutionDetails(tree.ScheduledExecSQLExecutor.InternalName(), jobspb.ExecutionArguments{Args: any})
	if value, ok := scheduleOptions[optFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(&params.p.ExtendedEvalContext().EvalContext, value, time.Microsecond)
		if err != nil {
			return err
		}
		schedule.SetNextRun(firstRun.Time)
	}

	if err := schedule.Create(params.ctx, params.p.ExecCfg().InternalExecutor, nil); err != nil {
		return err
	}
	return nil
}

// checkScheduledSQL checks whether the sql statement supports to be scheduled.
func (p *planner) checkScheduledSQL(ctx context.Context, sql string) error {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return err
	}
	// check if the sql statement is supported by schedule
	switch stmt.AST.(type) {
	// only support some dml statement for now
	case *tree.Insert, *tree.Update, *tree.Delete:
	default:
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s does not support to be scheduled", stmt.AST.StatementTag())
	}
	if stmt.NumPlaceholders != 0 {
		return pgerror.New(pgcode.FeatureNotSupported, "scheduled sql does not support placeholder")
	}
	// make a new local planner to check the sql if correct
	plan, cleanup := newInternalPlanner("sqlSchedule", p.txn, p.User(), &MemoryMetrics{}, p.execCfg)
	defer cleanup()
	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	//localPlanner.SessionData().Database = p.CurrentDatabase()
	//localPlanner.SessionData().SearchPath = p.CurrentSearchPath()

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(ctx)
	return nil
}
func (*createScheduleNode) Next(runParams) (bool, error) { return false, nil }
func (*createScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*createScheduleNode) Close(context.Context)        {}
