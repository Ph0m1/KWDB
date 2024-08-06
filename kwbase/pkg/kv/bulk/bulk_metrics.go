// Copyright 2019 The Cockroach Authors.
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

package bulk

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// Metrics contains pointers to the metrics for
// monitoring bulk operations.
type Metrics struct {
	MaxBytesHist  *metric.Histogram
	CurBytesCount *metric.Gauge
}

// MetricStruct implements the metrics.Struct interface.
func (Metrics) MetricStruct() {}

var _ metric.Struct = Metrics{}

var (
	metaMemMaxBytes = metric.Metadata{
		Name:        "sql.mem.bulk.max",
		Help:        "Memory usage per sql statement for bulk operations",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
	metaMemCurBytes = metric.Metadata{
		Name:        "sql.mem.bulk.current",
		Help:        "Current sql statement memory usage for bulk operations",
		Measurement: "Memory",
		Unit:        metric.Unit_BYTES,
	}
)

// See pkg/sql/mem_metrics.go
// log10int64times1000 = log10(math.MaxInt64) * 1000, rounded up somewhat
const log10int64times1000 = 19 * 1000

// MakeBulkMetrics instantiates the metrics holder for bulk operation monitoring.
func MakeBulkMetrics(histogramWindow time.Duration) Metrics {
	return Metrics{
		MaxBytesHist:  metric.NewHistogram(metaMemMaxBytes, histogramWindow, log10int64times1000, 3),
		CurBytesCount: metric.NewGauge(metaMemCurBytes),
	}
}
