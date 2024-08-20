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
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// sqlScheduleResumer implements the jobs.Resumer interface for sql schedule
// jobs. A new instance is created for each job.
type sqlScheduleResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &sqlScheduleResumer{}

const (
	_ = iota
	sqlSchedule
)

// Resume is part of the jobs.Resumer interface.
func (r *sqlScheduleResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(PlanHookState)
	d := r.job.Details().(jobspb.SqlScheduleDetails)
	switch d.ScheduleType {
	case sqlSchedule:
		p.ExecCfg().InternalExecutor.SetSessionData(&sessiondata.SessionData{
			DistSQLMode: sessiondata.DistSQLAuto,
		})
		_, err := p.ExecCfg().InternalExecutor.ExecEx(ctx, "exec-schedule", p.Txn(),
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			d.Statement,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *sqlScheduleResumer) OnFailOrCancel(context.Context, interface{}) error { return nil }

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &sqlScheduleResumer{job: job}
	}

	jobs.RegisterConstructor(jobspb.TypeSqlSchedule, createResumerFn)
}
