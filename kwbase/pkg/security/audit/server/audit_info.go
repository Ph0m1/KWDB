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

package server

import (
	"net"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// List of audit result
const (
	ExecSuccess string = "OK"
	ExecFail    string = "FAIL"
)

// BaseInfo hold all base info for audit server
type BaseInfo struct {
	status bool // server already started or not
}

// AuditInfo is the audit information
type AuditInfo struct {
	EventTime time.Time             // the time when the event happened
	Elapsed   time.Duration         // the time elapsed during the event execution
	User      *User                 // the user information
	Event     target.OperationType  // the event type
	Target    *TargetInfo           // the operation target
	Level     target.AuditLevelType // the audit level includes SystemLevel, StmtLevel, ObjectLevel
	Client    *ClientInfo           // the client information
	Result    *ResultInfo           // the result of event
	Command   *Command              // the command of the event
	Reporter  *Reporter             // the reporter of the audit event
}

// User includes the information of the user who executed the event.
type User struct {
	UserID   int    // user ID
	Username string // user name
	Roles    []Role // the role(s) that the user belongs to
}

// Role is the role information.
type Role struct {
	ID   int    // role ID
	Name string // role name
}

// TargetInfo includes the target information.
type TargetInfo struct {
	Typ     target.AuditObjectType // target type
	Targets map[uint32]Target      // target information
}

// Target is the operation target.
type Target struct {
	ID       uint32   // target ID
	Name     string   // target name
	Cascades []Target // the cascading dropped objects
}

// ClientInfo is the client information including the client address and application name.
type ClientInfo struct {
	AppName string // application name
	Address string // IP address and port(format: IP:port)
}

// ResultInfo is the result of execution result.
type ResultInfo struct {
	Status       string // the result, i.e. OK/FAIL
	ErrMsg       string // the error message
	RowsAffected int    // the number of rows affected
}

// Command  is the command of the execution event.
type Command struct {
	Cmd    string // the command
	Params string // the parameters of the command
}

// Reporter is the reporter of the audit event.
type Reporter struct {
	ClusterID uuid.UUID      // cluster ID
	NodeID    roachpb.NodeID // node ID
	HostIP    string         // host IP address
	HostPort  string         // host port
	HostMac   string         // host MAC address
	LastUp    int64          // The last time when the node started
}

// MakeAuditInfo returns audit information. It is used to record system level audit information.
func MakeAuditInfo(
	startTime time.Time,
	userName string,
	roles []string,
	oprType target.OperationType,
	objTyp target.AuditObjectType,
	level target.AuditLevelType,
	addr net.Addr,
	err error,
) AuditInfo {
	auditInfo := AuditInfo{}
	auditInfo.SetTimeAndElapsed(startTime)
	auditInfo.SetUser(userName, roles)
	auditInfo.SetEventType(oprType)
	auditInfo.SetTargetType(objTyp)
	auditInfo.SetAuditLevel(level)
	auditInfo.SetClientRemoteAddr(addr)
	auditInfo.SetResult(err, 0)
	return auditInfo
}

// GetAuditLevel returns Audit Level.
func (ai *AuditInfo) GetAuditLevel() target.AuditLevelType {
	return ai.Level
}

// SetAuditLevel sets Audit Level.
func (ai *AuditInfo) SetAuditLevel(level target.AuditLevelType) {
	ai.Level = level
}

// SetUser sets user information.
func (ai *AuditInfo) SetUser(userName string, roles []string) {
	if ai.User == nil {
		ai.User = &User{}
	}
	ai.User.Username = userName
	userRoles := make([]Role, len(roles))
	for i, roleName := range roles {
		userRoles[i] = Role{
			Name: roleName,
		}
	}
	ai.User.Roles = userRoles
}

// SetClient sets the client information.
func (ai *AuditInfo) SetClient(data *sessiondata.SessionData) {
	if data != nil {
		if ai.Client == nil {
			ai.Client = &ClientInfo{}
		}
		ai.Client.AppName = data.ApplicationName
		ai.SetClientRemoteAddr(data.RemoteAddr)
	}
}

// SetClientRemoteAddr sets the remote address of client.
func (ai *AuditInfo) SetClientRemoteAddr(addr net.Addr) {
	if ai.Client == nil {
		ai.Client = &ClientInfo{}
	}
	if addr != nil {
		ai.Client.Address = addr.String()
	}
}

// GetTargetType returns the target type.
func (ai *AuditInfo) GetTargetType() target.AuditObjectType {
	if ai.GetTargetInfo() != nil {
		return ai.GetTargetInfo().GetTargetType()
	}
	return ""
}

// SetTimeAndElapsed sets the time when the event happened and the time elapsed during the execution.
func (ai *AuditInfo) SetTimeAndElapsed(startTime time.Time) {
	ai.SetTime()
	ai.SetTimeElapsed(startTime)
}

// SetTime sets the time when the event happened.
func (ai *AuditInfo) SetTime() {
	ai.EventTime = timeutil.Now()
}

// SetTimeElapsed sets the time consumed when the event occurs.
func (ai *AuditInfo) SetTimeElapsed(startTime time.Time) {
	ai.Elapsed = timeutil.Now().Sub(startTime)
}

// SetResult sets the result of the event execution.
func (ai *AuditInfo) SetResult(err error, rowCount int) {
	if ai.Result == nil {
		ai.Result = &ResultInfo{}
	}
	if err != nil {
		ai.Result.Status = ExecFail
		ai.Result.ErrMsg = err.Error()
	} else {
		ai.Result.Status = ExecSuccess
	}
	ai.Result.RowsAffected = rowCount

}

// SetCommand sets the command of the event.
func (ai *AuditInfo) SetCommand(cmd string) {
	if ai.Command == nil {
		ai.Command = &Command{}
	}
	ai.Command.Cmd = cmd
}

// SetCommandOptions sets the command options.
func (ai *AuditInfo) SetCommandOptions(info *tree.PlaceholderInfo) {
	if info != nil {
		if ai.Command == nil {
			ai.Command = &Command{}
		}
		ai.Command.Params = info.Values.String()
	}
}

// SetReporter sets the reporter information.
func (ai *AuditInfo) SetReporter(
	clusterID uuid.UUID, nodeID roachpb.NodeID, host, port, hostMac string, lastUp int64,
) {
	if ai.Reporter == nil {
		ai.Reporter = &Reporter{}
	}
	ai.Reporter.ClusterID = clusterID
	ai.Reporter.NodeID = nodeID
	ai.Reporter.HostIP = host
	ai.Reporter.HostPort = port
	ai.Reporter.HostMac = hostMac
	ai.Reporter.LastUp = lastUp
}

// GetAuditUser returns user information.
func (ai *AuditInfo) GetAuditUser() *User {
	return ai.User
}

// GetTargetInfo returns target information.
func (ai *AuditInfo) GetTargetInfo() *TargetInfo {
	return ai.Target
}

// SetTargetType sets the target type.
func (ai *AuditInfo) SetTargetType(typ target.AuditObjectType) {
	if ai.Target == nil {
		ai.Target = &TargetInfo{}
	}
	if len(typ) != 0 {
		ai.Target.Typ = typ
	}
}

// SetTarget sets the target information.
func (ai *AuditInfo) SetTarget(id uint32, name string, cascade []string) {
	if ai.Target == nil {
		ai.Target = &TargetInfo{}
	}
	ai.Target.SetTargetIDAndName(id, name, cascade)
}

// SetTargetIDAndName sets the target information.
func (ti *TargetInfo) SetTargetIDAndName(id uint32, name string, cascade []string) {
	if ti.Targets == nil {
		ti.Targets = make(map[uint32]Target)
	}
	var cascadeTarget []Target
	if cascade != nil {
		for _, name := range cascade {
			temTarget := Target{Name: name}
			cascadeTarget = append(cascadeTarget, temTarget)
		}
	}
	newTarget := Target{ID: id, Name: name, Cascades: cascadeTarget}
	ti.Targets[id] = newTarget
}

// SetEventType sets the event type.
func (ai *AuditInfo) SetEventType(oprType target.OperationType) {
	ai.Event = oprType
}

// GetAuditResult returns result information.
func (ai *AuditInfo) GetAuditResult() *ResultInfo {
	return ai.Result
}

// GetTargetIDs returns the target IDs.
func (ti *TargetInfo) GetTargetIDs() []uint32 {
	IDs := make([]uint32, 0, len(ti.Targets))
	for ID := range ti.Targets {
		IDs = append(IDs, ID)
	}
	return IDs
}

// GetTargetType returns the target type.
func (ti *TargetInfo) GetTargetType() target.AuditObjectType {
	return ti.Typ
}

// IsTargetEmpty returns if the target is empty.
func (ai *AuditInfo) IsTargetEmpty() bool {
	if ai.GetTargetInfo() != nil {
		return ai.GetTargetInfo().IsTargetEmpty()
	}
	return true
}

// IsTargetEmpty returns if target is empty.
func (ti *TargetInfo) IsTargetEmpty() bool {
	return len(ti.Typ) == 0 || ti.Typ == ""
}

// GetEvent returns the operation type.
func (ai *AuditInfo) GetEvent() target.OperationType {
	return ai.Event
}

// IsOperationEmpty returns if the operation of the event is empty.
func (ai *AuditInfo) IsOperationEmpty() bool {
	return len(ai.Event) == 0 || ai.Event == ""
}

// GetUserName returns the user name.
func (u *User) GetUserName() string {
	return u.Username
}

// GetResult returns the result.
func (r *ResultInfo) GetResult() string {
	return r.Status
}
