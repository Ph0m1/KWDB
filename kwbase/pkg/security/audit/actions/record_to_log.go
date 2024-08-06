// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package actions

import (
	"context"
	"encoding/json"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// RecordToLog represents record audit log to log.
type RecordToLog struct {
	*ActionName
	auditLogger *log.SecondaryLogger // used to log data to audit log file
}

// NewRecordToLog return a new RecodeToLog.
func NewRecordToLog(Logger *log.SecondaryLogger) *RecordToLog {
	return &RecordToLog{
		ActionName:  &ActionName{"log "},
		auditLogger: Logger,
	}
}

// Action implements the Action interface.
func (r *RecordToLog) Action(ctx context.Context, auditInfo *server.AuditInfo) error {
	auditInfoJSON, err := json.Marshal(auditInfo)
	if err != nil {
		log.Error(ctx, "marshaling AuditInfo failed")
		return err
	}
	r.auditLogger.Logf(ctx, "%s", string(auditInfoJSON))
	return nil
}
