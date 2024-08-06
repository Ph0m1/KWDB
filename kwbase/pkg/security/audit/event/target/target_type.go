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

// AuditObjectType represents the type of audit object.
type AuditObjectType string

// AuditLevelType represents the level of audit object.
type AuditLevelType int32

// audit level
const (
	UnknownLevel AuditLevelType = -1
	SystemLevel  AuditLevelType = 0
	StmtLevel    AuditLevelType = 1
	ObjectLevel  AuditLevelType = 2
)

// audit object
const (
	// SystemLevel object
	ObjectNode           AuditObjectType = "NODE"
	ObjectCluster        AuditObjectType = "CLUSTER"
	ObjectClusterSetting AuditObjectType = "CLUSTERSETTINGS"
	ObjectStore          AuditObjectType = "STORE"
	ObjectConn           AuditObjectType = "CONN"

	// StmtLevel + ObjectLevel object
	ObjectUser       AuditObjectType = "USER"
	ObjectRole       AuditObjectType = "ROLE"
	ObjectDatabase   AuditObjectType = "DATABASE"
	ObjectSchema     AuditObjectType = "SCHEMA"
	ObjectTable      AuditObjectType = "TABLE"
	ObjectView       AuditObjectType = "VIEW"
	ObjectIndex      AuditObjectType = "INDEX"
	ObjectFunction   AuditObjectType = "FUNCTION"
	ObjectProcedure  AuditObjectType = "PROCEDURE"
	ObjectTrigger    AuditObjectType = "TRIGGER"
	ObjectSequence   AuditObjectType = "SEQUENCE"
	ObjectPrivilege  AuditObjectType = "PRIVILEGE"
	ObjectAudit      AuditObjectType = "AUDIT"
	ObjectRange      AuditObjectType = "RANGE"
	ObjectCDC        AuditObjectType = "CHANGEFEED"
	ObjectQuery      AuditObjectType = "QUERY"
	ObjectJob        AuditObjectType = "JOB"
	ObjectSchedule   AuditObjectType = "SCHEDULE"
	ObjectSession    AuditObjectType = "SESSION"
	ObjectStatistics AuditObjectType = "STATISTICS"
	ObjectSQL        AuditObjectType = "SQL"
	ObjectDevice     AuditObjectType = "DEVICE"
)

var auditObjectOperation = map[AuditObjectType][]OperationType{
	ObjectNode:           {Join, Quit, Restart, Decommission, Recommission},
	ObjectCluster:        {Init},
	ObjectClusterSetting: {Set, Reset},
	ObjectStore:          {Enable, Disable},
	ObjectConn:           {Login, Logout},
	ObjectUser:           {Create, Alter, Drop},
	ObjectRole:           {Create, Drop, Grant, Revoke, Alter},
	ObjectDatabase:       {Create, Drop, Alter, Flashback, Import, Export},
	ObjectSchema:         {Create, Drop, Alter, Change, Rollback},
	ObjectTable:          {Create, Drop, Alter, Flashback, Import, Export, Truncate, Select, Insert, Delete, Update},
	ObjectView:           {Create, Drop, Alter, Select, Insert, Delete, Update, Refresh},
	ObjectIndex:          {Create, Drop, Alter},
	ObjectTrigger:        {Create, Drop, Alter},
	ObjectSequence:       {Create, Drop, Alter},
	ObjectPrivilege:      {Grant, Revoke},
	ObjectAudit:          {Create, Drop, Alter},
	ObjectRange:          {Alter},
	ObjectCDC:            {Create},
	ObjectQuery:          {Cancel, Explain},
	ObjectJob:            {Cancel, Pause, Resume},
	ObjectSchedule:       {Alter, Pause, Resume},
	ObjectSession:        {Cancel, Set, Reset},
	ObjectDevice:         {Create, Alter, Drop, Select},
}
