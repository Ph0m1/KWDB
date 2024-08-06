// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package jobs

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// Metrics are for production monitoring of each job type.
type Metrics struct {
	Changefeed        metric.Struct
	ReplicationIngest metric.Struct
	ReplicationRecv   metric.Struct
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// InitHooks initializes the metrics for job monitoring.
func (m *Metrics) InitHooks(histogramWindowInterval time.Duration) {
	if MakeChangefeedMetricsHook != nil {
		m.Changefeed = MakeChangefeedMetricsHook(histogramWindowInterval)
	}
	if MakeReplicationIngestMetricsHook != nil {
		m.ReplicationIngest = MakeReplicationIngestMetricsHook(histogramWindowInterval)
	}
	if MakeReplicationRecvMetricsHook != nil {
		m.ReplicationRecv = MakeReplicationRecvMetricsHook(histogramWindowInterval)
	}
}

// MakeChangefeedMetricsHook allows for registration of changefeed metrics
var MakeChangefeedMetricsHook func(time.Duration) metric.Struct

// MakeReplicationIngestMetricsHook allows for registration of streaming metrics
var MakeReplicationIngestMetricsHook func(duration time.Duration) metric.Struct

// MakeReplicationRecvMetricsHook allows for registration of streaming metrics
var MakeReplicationRecvMetricsHook func(duration time.Duration) metric.Struct
