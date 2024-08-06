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

package object

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
)

// TableOperationMetric is the metric of operation on table
type TableOperationMetric struct {
	*target.Object
}

// NewTableMetric create a table metric
func NewTableMetric(ctx context.Context) *TableOperationMetric {
	return &TableOperationMetric{
		Object: &target.Object{
			Ctx:        ctx,
			Name:       "opera_tb_count",
			Level:      target.StmtLevel,
			ObjMetrics: target.CreateMetricMap(target.ObjectTable),
		},
	}
}

// GetAuditLevel Get object and operation level
func (t *TableOperationMetric) GetAuditLevel(opt target.OperationType) target.AuditLevelType {
	if opt == target.Select || opt == target.Insert || opt == target.Update || opt == target.Upsert ||
		opt == target.Delete || opt == target.MergeInto {
		return target.ObjectLevel
	}
	return t.Level
}
