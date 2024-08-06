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

package infos

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

// NodeInfo info struct
type NodeInfo struct {
	NodeID    int32 `protobuf:"varint,1,opt,name=node_id,json=nodeId,casttype=NodeID" json:"node_id"`
	LastUp    int64
	ClusterID uuid.UUID
}

// String print info in mail
func (info *NodeInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "nodeID:%d, lastUp:%d, ClusterID:%v", info.NodeID, info.LastUp, info.ClusterID)
	return sb.String()
}

// DatabaseInfo info struct
type DatabaseInfo struct {
	DatabaseName        string
	NewName             string
	Statement           string
	User                string
	DroppedTableObjects []string
}

// String print info in mail
func (info *DatabaseInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "DatabaseName:%s", info.DatabaseName)
	if info.NewName != "" {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	if len(info.DroppedTableObjects) != 0 {
		var temp string
		var value string
		for _, value = range info.DroppedTableObjects {
			temp = temp + " " + value
		}
		fmt.Fprintf(&sb, ", DroppedTableObjects:%s", temp)
	}
	return sb.String()
}

// SchemaInfo info struct
type SchemaInfo struct {
	SchemaName          string
	NewName             string
	Statement           string
	User                string
	DroppedTableObjects []string
}

// String print info in mail
func (info *SchemaInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "SchemaName:%s", info.SchemaName)
	if info.NewName != "" {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	if info.DroppedTableObjects != nil {
		var temp string
		var value string
		for _, value = range info.DroppedTableObjects {
			temp = temp + " " + value
		}
		fmt.Fprintf(&sb, ", DroppedTableObjects:%s", temp)
	}
	return sb.String()
}

// TableInfo info struct
type TableInfo struct {
	TableName           string
	NewName             string
	Statement           string
	User                string
	MutationID          uint32
	CascadeDroppedViews []string
}

// String print info in mail
func (info *TableInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TableName:%s", info.TableName)
	if len(info.NewName) != 0 {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	if info.MutationID != 0 {
		fmt.Fprintf(&sb, ", MutationID:%d", info.MutationID)
	}
	if len(info.CascadeDroppedViews) != 0 {
		var temp string
		var value string
		for _, value = range info.CascadeDroppedViews {
			temp = temp + " " + value
		}
		fmt.Fprintf(&sb, ", CascadeDroppedViews:%s", temp)
	}
	return sb.String()
}

// CommentInfo info struct
type CommentInfo struct {
	Name      string
	Statement string
	User      string
	Comment   *string
}

// String print info in mail
func (info *CommentInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Name:%s", info.Name)
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	fmt.Fprintf(&sb, ", Comment:%s", *info.Comment)
	return sb.String()
}

// IndexInfo info struct
type IndexInfo struct {
	TableName           string
	IndexName           string
	NewName             string
	Statement           string
	User                string
	MutationID          uint32
	CascadeDroppedViews []string
}

// String print info in mail
func (info *IndexInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TableName:%s", info.TableName)
	fmt.Fprintf(&sb, ", IndexName:%s", info.IndexName)
	if len(info.NewName) != 0 {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	if info.MutationID != 0 {
		fmt.Fprintf(&sb, ", MutationID:%d", info.MutationID)
	}
	if info.CascadeDroppedViews != nil {
		var temp string
		var value string
		for _, value = range info.CascadeDroppedViews {
			temp = temp + " " + value
		}
		fmt.Fprintf(&sb, ", CascadeDroppedViews:%s", temp)
	}
	return sb.String()
}

// ViewInfo info struct
type ViewInfo struct {
	ViewName            string
	NewName             string
	Statement           string
	User                string
	CascadeDroppedViews []string
}

// String print info in mail
func (info *ViewInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "ViewName:%s", info.ViewName)
	if len(info.NewName) != 0 {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	if len(info.CascadeDroppedViews) != 0 {
		var temp string
		var value string
		for _, value = range info.CascadeDroppedViews {
			temp = temp + " " + value
		}
		fmt.Fprintf(&sb, ", CascadeDroppedViews:%s", temp)
	}
	return sb.String()
}

// SequenceInfo info struct
type SequenceInfo struct {
	SequenceName string
	NewName      string
	Statement    string
	User         string
}

// String print info in mail
func (info *SequenceInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "SequenceName:%s", info.SequenceName)
	if len(info.NewName) != 0 {
		fmt.Fprintf(&sb, ", NewName:%s", info.NewName)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// FunctionInfo info struct
type FunctionInfo struct {
	FunctionName string
	Statement    string
	User         string
}

// String print info in mail
func (info *FunctionInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "FunctionName:%s", info.FunctionName)
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// TriggerInfo info struct
type TriggerInfo struct {
	TriggerName string
	TableName   string
	Statement   string
	User        string
}

// String print info in mail
func (info *TriggerInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TriggerName:%s", info.TriggerName)
	fmt.Fprintf(&sb, "TableName:%s", info.TableName)
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// JobInfo info struct
type JobInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *JobInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// UserInfo info struct
type UserInfo struct {
	Username  string
	RoleName  string
	Statement string
	User      string
}

// String print info in mail
func (info *UserInfo) String() string {
	var sb strings.Builder
	if info.RoleName != "" {
		fmt.Fprintf(&sb, "RoleName:%s", info.RoleName)
	} else {
		fmt.Fprintf(&sb, "Username:%s", info.Username)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// PrivilegeInfo info struct
type PrivilegeInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *PrivilegeInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// QueryInfo info struct
type QueryInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *QueryInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// DumpLoadInfo info struct
type DumpLoadInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *DumpLoadInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// FlashBackInfo info struct
type FlashBackInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *FlashBackInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// CDCInfo info struct
type CDCInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *CDCInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// TransactionInfo info struct
type TransactionInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *TransactionInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// StatisticsInfo info struct
type StatisticsInfo struct {
	TableName string
	Statement string
	User      string
}

// String print info in mail
func (info *StatisticsInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "TableName:%s", info.TableName)
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// ScheduledInfo info struct
type ScheduledInfo struct {
	ScheduledName string
	ScheduledID   int64
	Statement     string
	User          string
}

// String print info in mail
func (info *ScheduledInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "ScheduledName:%s", info.ScheduledName)
	fmt.Fprintf(&sb, ", ScheduledID:%d", info.ScheduledID)
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// SessionInfo info struct
type SessionInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *SessionInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// AuditInfo info struct
type AuditInfo struct {
	Statement string
	User      string
}

// String print info in mail
func (info *AuditInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}

// SetClusterSettingInfo info struct
type SetClusterSettingInfo struct {
	SettingName string
	Value       string
	Statement   string
	User        string
}

// String print info in mail
func (info *SetClusterSettingInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "SettingName:%s,Value:%s,Statement:%s,User:%v,", info.SettingName, info.Value, info.Statement, info.User)
	return sb.String()
}

// SetZoneConfigInfo info struct
type SetZoneConfigInfo struct {
	Target    string
	Config    string `json:",omitempty"`
	Options   string `json:",omitempty"`
	Statement string
	User      string
}

// String print info in mail
func (info *SetZoneConfigInfo) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Target:%s", info.Target)
	if info.Config != "" {
		fmt.Fprintf(&sb, ", Config:%s", info.Config)
	}
	if info.Options != "" {
		fmt.Fprintf(&sb, ", Options:%s", info.Options)
	}
	fmt.Fprintf(&sb, ", Statement:%s", info.Statement)
	fmt.Fprintf(&sb, ", User:%v", info.User)
	return sb.String()
}
