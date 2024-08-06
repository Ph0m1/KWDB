// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// ExecutorMetrics describes metrics related to scheduled
// job executor operations.
type ExecutorMetrics struct {
	NumStarted   *metric.Counter
	NumSucceeded *metric.Counter
	NumFailed    *metric.Counter
}

var _ metric.Struct = &ExecutorMetrics{}

// MetricStruct implements metric.Struct interface
func (m *ExecutorMetrics) MetricStruct() {}

// SchedulerMetrics are metrics specific to job scheduler daemon.
type SchedulerMetrics struct {
	// Number of schedules that were ready to execute.
	ReadyToRun *metric.Gauge
	// Number of scheduled jobs started.
	NumStarted *metric.Gauge
	// Number of jobs started by schedules that are currently running.
	NumRunning *metric.Gauge
	// Number of schedules rescheduled due to SKIP policy.
	RescheduleSkip *metric.Gauge
	// Number of schedules rescheduled due to WAIT policy.
	RescheduleWait *metric.Gauge
	// Number of schedules that could not be processed due to an error.
	NumBadSchedules *metric.Counter
}

// MakeSchedulerMetrics returns metrics for scheduled job daemon.
func MakeSchedulerMetrics() SchedulerMetrics {
	return SchedulerMetrics{
		ReadyToRun: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.schedules-ready-to-run",
			Help:        "The number of jobs ready to execute",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		NumRunning: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.num-jobs-running",
			Help:        "The number of jobs started by schedules that are currently running",
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumStarted: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.jobs-started",
			Help:        "The number of jobs started",
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		RescheduleSkip: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.reschedule-skip",
			Help:        "The number of schedules rescheduled due to SKIP policy",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		RescheduleWait: metric.NewGauge(metric.Metadata{
			Name:        "schedules.round.reschedule-wait",
			Help:        "The number of schedules rescheduled due to WAIT policy",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),

		NumBadSchedules: metric.NewCounter(metric.Metadata{
			Name:        "schedules.corrupt",
			Help:        "Number of corrupt/bad schedules",
			Measurement: "Schedules",
			Unit:        metric.Unit_COUNT,
		}),
	}
}

// MetricStruct implements metric.Struct interface
func (m *SchedulerMetrics) MetricStruct() {}

var _ metric.Struct = &SchedulerMetrics{}

// MakeExecutorMetrics creates metrics for scheduled job executor.
func MakeExecutorMetrics(name string) ExecutorMetrics {
	return ExecutorMetrics{
		NumStarted: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.started", name),
			Help:        fmt.Sprintf("Number of %s jobs started", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumSucceeded: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.succeeded", name),
			Help:        fmt.Sprintf("Number of %s jobs succeeded", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),

		NumFailed: metric.NewCounter(metric.Metadata{
			Name:        fmt.Sprintf("schedules.%s.failed", name),
			Help:        fmt.Sprintf("Number of %s jobs failed", name),
			Measurement: "Jobs",
			Unit:        metric.Unit_COUNT,
		}),
	}
}
