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

package setting

import "gitee.com/kwbasedb/kwbase/pkg/settings"

// AuditSetting define
type AuditSetting string

const (
	// SettingAuditEnable : the switch of audit server
	SettingAuditEnable AuditSetting = "auditEnable"

	// SettingAuditLogEnable :the mode to save
	SettingAuditLogEnable AuditSetting = "auditLogEnable"
)

var (
	// AuditEnabled audit switch
	AuditEnabled = settings.RegisterBoolSetting(
		"audit.enabled",
		"the switch of audit",
		false,
	)

	// AuditLogEnabled audit log file record switch
	AuditLogEnabled = settings.RegisterBoolSetting(
		"audit.log.enabled",
		"the switch of audit log file record",
		true,
	)
)
