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

// ProcedureOperationMetric is the metric of operation on procedure
type ProcedureOperationMetric struct {
	*target.Object
}

// NewProcedureMetric create a procedure metric
func NewProcedureMetric(ctx context.Context) *ProcedureOperationMetric {
	return &ProcedureOperationMetric{
		Object: &target.Object{
			Ctx:        ctx,
			Name:       "opera_procedure_count",
			Level:      target.StmtLevel,
			ObjMetrics: target.CreateMetricMap(target.ObjectProcedure),
		},
	}
}

// GetAuditLevel Get object and operation level
func (t *ProcedureOperationMetric) GetAuditLevel(opt target.OperationType) target.AuditLevelType {
	if opt == target.Execute {
		return target.ObjectLevel
	}
	return t.Level
}
