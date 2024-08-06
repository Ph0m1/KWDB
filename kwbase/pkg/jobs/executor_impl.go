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

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

// InlineExecutorName is the name associated with scheduled job executor which
// runs jobs "inline" -- that is, it doesn't spawn external system.job to do its work.
const InlineExecutorName = "inline"

// inlineScheduledJobExecutor implements ScheduledJobExecutor interface.
// This executor runs SQL statement "inline" -- that is, it executes statement
// directly under transaction.
type inlineScheduledJobExecutor struct{}

var _ ScheduledJobExecutor = &inlineScheduledJobExecutor{}

// ExecuteJob implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	txn *kv.Txn,
) error {
	sqlArgs := &jobspb.SqlStatementExecutionArg{}

	if err := types.UnmarshalAny(schedule.ExecutionArgs().Args, sqlArgs); err != nil {
		return errors.Wrapf(err, "expected SqlStatementExecutionArg")
	}

	// TODO(yevgeniy): this is too simplistic.  It would be nice
	// to capture execution traces, or some similar debug information and save that.
	// Also, performing this under the same transaction as the scan loop is not ideal
	// since a single failure would result in rollback for all of the changes.
	_, err := cfg.InternalExecutor.ExecEx(ctx, "inline-exec", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		sqlArgs.Statement,
	)

	if err != nil {
		return err
	}

	// TODO(yevgeniy): Log execution result
	return nil
}

// NotifyJobTermination implements ScheduledJobExecutor interface.
func (e *inlineScheduledJobExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID int64,
	jobStatus Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	// For now, only interested in failed status.
	if jobStatus == StatusFailed {
		DefaultHandleFailedRun(schedule, "job %d failed", jobID)
	}
	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *inlineScheduledJobExecutor) Metrics() metric.Struct {
	return nil
}

func init() {
	RegisterScheduledJobExecutorFactory(
		InlineExecutorName,
		func() (ScheduledJobExecutor, error) {
			return &inlineScheduledJobExecutor{}, nil
		})
}
