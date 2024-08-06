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

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// AuditMetrics define the metric interface
type AuditMetrics interface {
	Metric(opt OperationType) bool
	RegisterMetric() map[OperationType]*ObjectMetrics
	GetAuditLevel(opt OperationType) AuditLevelType
	Count(opt OperationType) int64
}

// ObjectMetrics contain CatchCount
type ObjectMetrics struct {
	CatchCount *metric.Counter // metric define
}

// Object represents ObjectMetrics
type Object struct {
	Ctx        context.Context                  // accept context
	Name       string                           // metric name
	Level      AuditLevelType                   // audit level
	ObjMetrics map[OperationType]*ObjectMetrics // map for ObjectMetrics
}

// GetAuditLevel Get object and operation level
func (obj *Object) GetAuditLevel(opt OperationType) AuditLevelType {
	if obj != nil {
		return obj.Level
	}
	return UnknownLevel
}

// RegisterMetric interface return metrics
func (obj *Object) RegisterMetric() map[OperationType]*ObjectMetrics {
	if obj != nil {
		return obj.ObjMetrics
	}
	return nil
}

// Metric returns false if opt does not exist in metric.
func (obj *Object) Metric(opt OperationType) bool {
	// 	increment by 1
	if mr, exist := obj.ObjMetrics[opt]; exist {
		mr.CatchCount.Inc(1)
		return true
	}
	return false
}

// Count increment by 1 if opt exists in metric.
func (obj *Object) Count(opt OperationType) int64 {
	// 	increment by 1
	if mr, exist := obj.ObjMetrics[opt]; exist {
		return mr.CatchCount.Count()
	}
	return 0
}

// CreateMetric a ObjectMetrics
func CreateMetric(obj AuditObjectType, opt OperationType) *ObjectMetrics {
	n := "audit." + string(obj) + "." + string(opt)
	h := string(opt) + "a" + string(obj)
	return &ObjectMetrics{
		CatchCount: metric.NewCounter(
			metric.Metadata{
				Name:        n,
				Help:        h,
				Measurement: "audit",
				Unit:        metric.Unit_COUNT,
			}),
	}
}

// CreateMetricMap create a ObjectMetrics  map
func CreateMetricMap(objType AuditObjectType) map[OperationType]*ObjectMetrics {
	// 	increment by 1
	metricsMap := make(map[OperationType]*ObjectMetrics)
	ops := auditObjectOperation[objType]
	for _, op := range ops {
		metricsMap[op] = CreateMetric(objType, op)
	}
	return metricsMap
}
