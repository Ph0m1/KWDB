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

package target

// OperationType represents an operation type that can be recorded in the audit log.
type OperationType string

// List of OperationType.
const (
	Login        OperationType = "LOGIN"
	Logout       OperationType = "LOGOUT"
	Create       OperationType = "CREATE"
	Alter        OperationType = "ALTER"
	Drop         OperationType = "DROP"
	Execute      OperationType = "EXECUTE"
	Grant        OperationType = "GRANT"
	Revoke       OperationType = "REVOKE"
	Explain      OperationType = "EXPLAIN"
	Set          OperationType = "SET"
	Reset        OperationType = "RESET"
	Pause        OperationType = "PAUSE"
	Resume       OperationType = "RESUME"
	Cancel       OperationType = "CANCEL"
	Insert       OperationType = "INSERT"
	Update       OperationType = "UPDATE"
	Upsert       OperationType = "UPSERT"
	Delete       OperationType = "DELETE"
	Select       OperationType = "SELECT"
	Truncate     OperationType = "TRUNCATE"
	MergeInto    OperationType = "MERGEINTO"
	Refresh      OperationType = "REFRESH"
	Flashback    OperationType = "FLASHBACK"
	Init         OperationType = "INIT"
	Restart      OperationType = "RESTART"
	Join         OperationType = "JOIN"
	Decommission OperationType = "DECOMMISSION"
	Recommission OperationType = "RECOMMISSION"
	Quit         OperationType = "QUIT"
	Enable       OperationType = "ENABLE"
	Disable      OperationType = "DISABLE"
	Replace      OperationType = "REPLACE"
	OverFlowLen  OperationType = "OVERFLOW"
	Inject       OperationType = "INJECT"
	Change       OperationType = "CHANGE"
	Rollback     OperationType = "ROLLBACK"
	Import       OperationType = "IMPORT"
	Export       OperationType = "EXPORT"
	Restore      OperationType = "RESTORE"

	AllOper OperationType = "ALL"
)
