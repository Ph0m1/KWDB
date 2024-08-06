// Copyright 2017 The Cockroach Authors.
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

package execinfra

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// DistSQLMetrics contains pointers to the metrics for monitoring DistSQL
// processing.
type DistSQLMetrics struct {
	QueriesActive     *metric.Gauge
	QueriesTotal      *metric.Counter
	FlowsActive       *metric.Gauge
	FlowsTotal        *metric.Counter
	FlowsQueued       *metric.Gauge
	QueueWaitHist     *metric.Histogram
	MaxBytesHist      *metric.Histogram
	CurBytesCount     *metric.Gauge
	VecOpenFDs        *metric.Gauge
	CurDiskBytesCount *metric.Gauge
	MaxDiskBytesHist  *metric.Histogram
}

// MetricStruct implements the metrics.Struct interface.
func (DistSQLMetrics) MetricStruct() {}

var _ metric.Struct = DistSQLMetrics{}

var (
	metaQueriesActive = metric.Metadata{
		Name:        "sql.distsql.queries.active",
		Help:        "Number of distributed SQL queries currently active",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaQueriesTotal = metric.Metadata{
		Name:        "sql.distsql.queries.total",
		Help:        "Number of distributed SQL queries executed",
		Measurement: "Queries",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsActive = metric.Metadata{
		Name:        "sql.distsql.flows.active",
		Help:        "Number of distributed SQL flows currently active",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsTotal = metric.Metadata{
		Name:        "sql.distsql.flows.total",
		Help:        "Number of distributed SQL flows executed",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaFlowsQueued = metric.Metadata{
		Name:        "sql.distsql.flows.queued",
		Help:        "Number of distributed SQL flows currently queued",
		Measurement: "Flows",
		Unit:        metric.Unit_COUNT,
	}
	metaQueueWaitHist = metric.Metadata{
		Name:        "sql.distsql.flows.queue_wait",
		Help:        "Duration of time flows spend waiting in the queue",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaMemMaxBytes = metric.Metadata{
		Name:        "sql.mem.distsql.max",
		Help:        "Memory usage per sql statement for distsql",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemCurBytes = metric.Metadata{
		Name:        "sql.mem.distsql.current",
		Help:        "Current sql statement memory usage for distsql",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaVecOpenFDs = metric.Metadata{
		Name:        "sql.distsql.vec.openfds",
		Help:        "Current number of open file descriptors used by vectorized external storage",
		Measurement: "Files",
		Unit:        metric.Unit_COUNT,
	}
	metaDiskCurBytes = metric.Metadata{
		Name:        "sql.disk.distsql.current",
		Help:        "Current sql statement disk usage for distsql",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
	metaDiskMaxBytes = metric.Metadata{
		Name:        "sql.disk.distsql.max",
		Help:        "Disk usage per sql statement for distsql",
		Measurement: "Disk",
		Unit:        metric.Unit_BYTES,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeDistSQLMetrics instantiates the metrics holder for DistSQL monitoring.
func MakeDistSQLMetrics(histogramWindow time.Duration) DistSQLMetrics {
	return DistSQLMetrics{
		QueriesActive:     metric.NewGauge(metaQueriesActive),
		QueriesTotal:      metric.NewCounter(metaQueriesTotal),
		FlowsActive:       metric.NewGauge(metaFlowsActive),
		FlowsTotal:        metric.NewCounter(metaFlowsTotal),
		FlowsQueued:       metric.NewGauge(metaFlowsQueued),
		QueueWaitHist:     metric.NewLatency(metaQueueWaitHist, histogramWindow),
		MaxBytesHist:      metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount:     metric.NewGauge(metaMemCurBytes),
		VecOpenFDs:        metric.NewGauge(metaVecOpenFDs),
		CurDiskBytesCount: metric.NewGauge(metaDiskCurBytes),
		MaxDiskBytesHist:  metric.NewHistogram(metaDiskMaxBytes, histogramWindow, log10int64times1000, 3),
	}
}

// QueryStart registers the start of a new DistSQL query.
func (m *DistSQLMetrics) QueryStart() {
	m.QueriesActive.Inc(1)
	m.QueriesTotal.Inc(1)
}

// QueryStop registers the end of a DistSQL query.
func (m *DistSQLMetrics) QueryStop() {
	m.QueriesActive.Dec(1)
}

// FlowStart registers the start of a new DistSQL flow.
func (m *DistSQLMetrics) FlowStart() {
	m.FlowsActive.Inc(1)
	m.FlowsTotal.Inc(1)
}

// FlowStop registers the end of a DistSQL flow.
func (m *DistSQLMetrics) FlowStop() {
	m.FlowsActive.Dec(1)
}
