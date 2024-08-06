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

// ClusterSettingOperationMetric is the metric of operation on clusterSetting
type ClusterSettingOperationMetric struct {
	*target.Object
}

// NewClusterSettingMetric create a cluster setting metric
func NewClusterSettingMetric(ctx context.Context) *ClusterSettingOperationMetric {
	return &ClusterSettingOperationMetric{
		Object: &target.Object{
			Ctx:        ctx,
			Name:       "opera_clustersetting_count",
			Level:      target.SystemLevel,
			ObjMetrics: target.CreateMetricMap(target.ObjectClusterSetting),
		},
	}
}
