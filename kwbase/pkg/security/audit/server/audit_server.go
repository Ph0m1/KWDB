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
	"context"
	"errors"
	"sync"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/setting"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
)

// AuditServer used to log events to table and log file, metric event and do actions when trigger rules
type AuditServer struct {
	ctx        context.Context   // context from server
	cs         *cluster.Settings // cluster settings
	nodeID     int32             // current nodeID used to log events as default reporting id
	logHandler LogHandler
	clock      *hlc.Clock
	settings   sync.Map         // settings for audit server
	base       *BaseInfo        // server base info
	registry   *metric.Registry //add registry to collect runner worker metrics
}

// LogHandler as interface to log audit data
type LogHandler interface {
	LogAudit(txn *kv.Txn, auditLog *AuditInfo) error // log audit data to db and file system
	InitAuditStrategy()
	IsAudit(txn *kv.Txn, info *AuditInfo) bool
	SetTableVersion(version uint32)
	GetTableVersion() uint32
}

// NewAuditServer return a new initialed audit server
func NewAuditServer(
	ctx context.Context, cs *cluster.Settings, mr *metric.Registry, cl *hlc.Clock,
) *AuditServer {
	// define new audit server
	auditServer := &AuditServer{
		ctx:      ctx,
		cs:       cs,
		settings: sync.Map{},
		clock:    cl,
		base:     &BaseInfo{},
		registry: mr,
	}

	auditServer.settings.Store(setting.SettingAuditEnable, setting.AuditEnabled.Get(&cs.SV))
	auditServer.settings.Store(setting.SettingAuditLogEnable, setting.AuditLogEnabled.Get(&cs.SV))
	return auditServer
}

// InitLogHandler init log handler
func (as *AuditServer) InitLogHandler(handler LogHandler) {
	as.logHandler = handler
}

// InitNodeID using node id after node started
func (as *AuditServer) InitNodeID(nodeID int32) {
	as.nodeID = nodeID
}

// Start to handle event async and refresh audit settings by specified interval
func (as *AuditServer) Start(ctx context.Context, s *stop.Stopper) {
	//	Start a goroutine to refresh settings every 'audit.refresh.interval' interval
	as.InitAuditServer(ctx)
	sv := &as.cs.SV
	as.settings.Store(setting.SettingAuditEnable, setting.AuditEnabled.Get(sv))
	as.settings.Store(setting.SettingAuditLogEnable, setting.AuditLogEnabled.Get(sv))
	setting.AuditEnabled.SetOnChange(sv, func() {
		as.settings.Store(setting.SettingAuditEnable, setting.AuditEnabled.Get(&as.cs.SV))
	})
	setting.AuditLogEnabled.SetOnChange(sv, func() {
		as.settings.Store(setting.SettingAuditLogEnable, setting.AuditLogEnabled.Get(&as.cs.SV))
	})

	if !as.base.status {
		as.base.status = true
		//	as.chanState <- true
	}
	as.logHandler.InitAuditStrategy()

	s.RunWorker(ctx, func(workersCtx context.Context) {
		<-s.ShouldStop()
	})
}

// LogAudit log audit info to table system.eventlog and audit file with sync or async
func (as *AuditServer) LogAudit(ctx context.Context, txn *kv.Txn, auditLog *AuditInfo) error {
	if auditLog == nil {
		return errors.New("auditInfo is a nil pointer")
	}
	if !as.GetSetting(setting.SettingAuditEnable).(bool) {
		return nil
	}
	if !as.GetSetting(setting.SettingAuditLogEnable).(bool) {
		return nil
	}

	// handle event synchronously
	return as.logHandler.LogAudit(txn, auditLog)
}

// GetSetting return audit settings for key
// TODO better to use default value instead of nil
func (as *AuditServer) GetSetting(key setting.AuditSetting) interface{} {
	if v, ok := as.settings.Load(key); ok {
		return v
	}
	return nil
}

// IsAudit check if event need to be audit
func (as *AuditServer) IsAudit(audinfo *AuditInfo) bool {

	// TODO remove this check after refactor audit server to support test that not using audit
	if as == nil || as.logHandler == nil {
		return false
	}
	return as.logHandler.IsAudit(nil, audinfo)
}

// InitAuditServer manges audit server.
func (as *AuditServer) InitAuditServer(ctx context.Context) {
	// TODO remove this check after refactor audit server to support test that not using audit
	if as == nil || as.logHandler == nil {
		return
	}
}

// GetHandler returns LogHandler
func (as *AuditServer) GetHandler() LogHandler {
	return as.logHandler
}

// TestSyncConfig sync
func (as *AuditServer) TestSyncConfig() {
	as.SetTestAuditEnable()
}

// SetTestAuditEnable set test enable
func (as *AuditServer) SetTestAuditEnable() {
	as.settings.Store(setting.SettingAuditEnable, true)
	as.settings.Store(setting.SettingAuditEnable, true)
}
