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

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/server"
)

var _ AuditAction = &RecordToLog{}

// AuditAction interface.
type AuditAction interface {
	Action(ctx context.Context, auditInfo *server.AuditInfo) error
	GetActionName() string
}

// ActionName is the name of audit action.
type ActionName struct {
	Name string
}

// GetActionName returns audit action name.
func (an *ActionName) GetActionName() string {
	return an.Name
}

// ActionID is kind of audit action.
type ActionID uint32

// List of audit action.
const (
	AllAction      ActionID = 0
	MailSend       ActionID = 1
	LogRecord      ActionID = 2
	WriteIntoTable ActionID = 3
)
