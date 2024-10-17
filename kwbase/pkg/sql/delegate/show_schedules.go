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

package delegate

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

// commandColumn converts executor execution arguments into jsonb representation.
const commandColumn = `kwdbdb_internal.pb_to_json('kwbase.jobs.jobspb.ExecutionArguments', execution_args)->'args'`

func (d *delegator) delegateShowSchedules(n *tree.ShowSchedule) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Schedules)

	columnExprs := []string{
		"schedule_id as id",
		"schedule_name as name",
		"(CASE WHEN next_run IS NULL THEN 'PAUSED' ELSE 'ACTIVE' END) AS schedule_status",
		"next_run",
		"kwdbdb_internal.pb_to_json('kwbase.jobs.jobspb.ScheduleState', schedule_state)->>'status' as state",
		"(CASE WHEN schedule_expr IS NULL THEN 'NEVER' ELSE schedule_expr END) as recurrence",
		fmt.Sprintf(`(
SELECT count(*) FROM system.jobs
WHERE status='%s' AND created_by_id=schedule_id
) AS jobsRunning`, jobs.StatusRunning),
		"owner",
		"created",
	}

	var whereExprs []string

	//switch n.WhichSchedules {
	//case tree.PausedSchedules:
	//	whereExprs = append(whereExprs, "next_run IS NULL")
	//case tree.ActiveSchedules:
	//	whereExprs = append(whereExprs, "next_run IS NOT NULL")
	//}

	//switch n.ExecutorType {
	//case tree.ScheduledBackupExecutor:
	//	whereExprs = append(whereExprs, fmt.Sprintf(
	//		"executor_type = '%s'", tree.ScheduledBackupExecutor.InternalName()))
	//	columnExprs = append(columnExprs, fmt.Sprintf(
	//		"%s->>'backup_statement' AS command", commandColumn))
	//default:
	//	// Strip out '@type' tag from the ExecutionArgs.args, and display what's left.
	//	columnExprs = append(columnExprs, fmt.Sprintf("%s #-'{@type}' AS command", commandColumn))
	//}

	if n.ScheduleName != "" {
		whereExprs = append(whereExprs,
			fmt.Sprintf("schedule_name='%s'", string(n.ScheduleName)))
	}

	var whereClause string
	if len(whereExprs) > 0 {
		whereClause = fmt.Sprintf("WHERE (%s)", strings.Join(whereExprs, " AND "))
	}

	return parse(fmt.Sprintf(
		"SELECT %s FROM system.scheduled_jobs %s",
		strings.Join(columnExprs, ","),
		whereClause,
	))
}
