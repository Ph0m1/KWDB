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

package sqltelemetry

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/server/telemetry"
)

// ShowTelemetryType is an enum used to represent the different show commands
// that we are recording telemetry for.
type ShowTelemetryType int

const (
	_ ShowTelemetryType = iota
	// Ranges represents the SHOW RANGES command.
	Ranges
	// Partitions represents the SHOW PARTITIONS command.
	Partitions
	// Locality represents the SHOW LOCALITY command.
	Locality
	// Create represents the SHOW CREATE command.
	Create
	// RangeForRow represents the SHOW RANGE FOR ROW command.
	RangeForRow
	// Queries represents the SHOW QUERIES command.
	Queries
	// Indexes represents the SHOW INDEXES command.
	Indexes
	// Constraints represents the SHOW CONSTRAINTS command.
	Constraints
	// Jobs represents the SHOW JOBS command.
	Jobs
	// Roles represents the SHOW ROLES command.
	Roles
	// Schedules represents the SHOW SCHEDULE command.
	Schedules
)

var showTelemetryNameMap = map[ShowTelemetryType]string{
	Ranges:      "ranges",
	Partitions:  "partitions",
	Locality:    "locality",
	Create:      "create",
	RangeForRow: "rangeforrow",
	Queries:     "queries",
	Indexes:     "indexes",
	Constraints: "constraints",
	Jobs:        "jobs",
	Roles:       "roles",
	Schedules:   "schedules",
}

func (s ShowTelemetryType) String() string {
	return showTelemetryNameMap[s]
}

var showTelemetryCounters map[ShowTelemetryType]telemetry.Counter

func init() {
	showTelemetryCounters = make(map[ShowTelemetryType]telemetry.Counter)
	for ty, s := range showTelemetryNameMap {
		showTelemetryCounters[ty] = telemetry.GetCounterOnce(fmt.Sprintf("sql.show.%s", s))
	}
}

// IncrementShowCounter is used to increment the telemetry counter for a particular show command.
func IncrementShowCounter(showType ShowTelemetryType) {
	telemetry.Inc(showTelemetryCounters[showType])
}
