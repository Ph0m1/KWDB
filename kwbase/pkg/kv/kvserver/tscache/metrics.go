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

package tscache

import "gitee.com/kwbasedb/kwbase/pkg/util/metric"

// Metrics holds all metrics relating to a Cache.
type Metrics struct {
	Skl sklMetrics
}

// sklMetrics holds all metrics relating to an intervalSkl.
type sklMetrics struct {
	Pages         *metric.Gauge
	PageRotations *metric.Counter
}

// MetricStruct implements the metrics.Struct interface.
func (sklMetrics) MetricStruct() {}

var _ metric.Struct = sklMetrics{}

var (
	metaSklPages = metric.Metadata{
		Name:        "tscache.skl.pages",
		Help:        "Number of pages in the timestamp cache",
		Measurement: "Pages",
		Unit:        metric.Unit_COUNT,
	}
	metaSklRotations = metric.Metadata{
		Name:        "tscache.skl.rotations",
		Help:        "Number of page rotations in the timestamp cache",
		Measurement: "Page Rotations",
		Unit:        metric.Unit_COUNT,
	}
)

func makeMetrics() Metrics {
	return Metrics{
		Skl: sklMetrics{
			Pages:         metric.NewGauge(metaSklPages),
			PageRotations: metric.NewCounter(metaSklRotations),
		},
	}
}
