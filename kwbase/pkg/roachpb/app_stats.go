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

package roachpb

import (
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// GetVariance retrieves the variance of the values.
func (l *NumericStat) GetVariance(count int64) float64 {
	return l.SquaredDiffs / (float64(count) - 1)
}

// Record updates the underlying running counts, incorporating the given value.
// It follows Welford's algorithm (Technometrics, 1962). The running count must
// be stored as it is required to finalize and retrieve the variance.
func (l *NumericStat) Record(count int64, val float64) {
	delta := val - l.Mean
	l.Mean += delta / float64(count)
	l.SquaredDiffs += delta * (val - l.Mean)
}

// Add combines b into this derived statistics.
func (l *NumericStat) Add(b NumericStat, countA, countB int64) {
	*l = AddNumericStats(*l, b, countA, countB)
}

// AlmostEqual compares two NumericStats between a window of size eps.
func (l *NumericStat) AlmostEqual(b NumericStat, eps float64) bool {
	return math.Abs(l.Mean-b.Mean) <= eps &&
		math.Abs(l.SquaredDiffs-b.SquaredDiffs) <= eps
}

// AddNumericStats combines derived statistics.
// Adapted from https://www.johndcook.com/blog/skewness_kurtosis/
func AddNumericStats(a, b NumericStat, countA, countB int64) NumericStat {
	total := float64(countA + countB)
	delta := b.Mean - a.Mean

	return NumericStat{
		Mean: ((a.Mean * float64(countA)) + (b.Mean * float64(countB))) / total,
		SquaredDiffs: (a.SquaredDiffs + b.SquaredDiffs) +
			delta*delta*float64(countA)*float64(countB)/total,
	}
}

// GetScrubbedCopy returns a copy of the given SensitiveInfo with its fields redacted
// or omitted entirely. By default, fields are omitted: if a new field is
// added to the SensitiveInfo proto, it must be added here to make it to the
// reg cluster.
func (si SensitiveInfo) GetScrubbedCopy() SensitiveInfo {
	output := SensitiveInfo{}
	output.LastErr = log.Redact(si.LastErr)
	// Not copying over MostRecentPlanDescription until we have an algorithm to scrub plan nodes.
	return output
}

// Add combines other into this TxnStats.
func (s *TxnStats) Add(other TxnStats) {
	s.TxnTimeSec.Add(other.TxnTimeSec, s.TxnCount, other.TxnCount)
	s.TxnCount += other.TxnCount
	s.ImplicitCount += other.ImplicitCount
	s.CommittedCount += other.CommittedCount
}

// Add combines other into this StatementStatistics.
func (s *StatementStatistics) Add(other *StatementStatistics) {
	s.FirstAttemptCount += other.FirstAttemptCount
	if other.MaxRetries > s.MaxRetries {
		s.MaxRetries = other.MaxRetries
	}
	s.NumRows.Add(other.NumRows, s.Count, other.Count)
	s.ParseLat.Add(other.ParseLat, s.Count, other.Count)
	s.PlanLat.Add(other.PlanLat, s.Count, other.Count)
	s.RunLat.Add(other.RunLat, s.Count, other.Count)
	s.ServiceLat.Add(other.ServiceLat, s.Count, other.Count)
	s.OverheadLat.Add(other.OverheadLat, s.Count, other.Count)

	if other.SensitiveInfo.LastErr != "" {
		s.SensitiveInfo.LastErr = other.SensitiveInfo.LastErr
	}

	if s.SensitiveInfo.MostRecentPlanTimestamp.Before(other.SensitiveInfo.MostRecentPlanTimestamp) {
		s.SensitiveInfo = other.SensitiveInfo
	}

	s.BytesRead += other.BytesRead
	s.RowsRead += other.RowsRead
	s.Count += other.Count
	s.FailedCount += other.FailedCount
}

// AlmostEqual compares two StatementStatistics and their contained NumericStats
// objects within an window of size eps.
func (s *StatementStatistics) AlmostEqual(other *StatementStatistics, eps float64) bool {
	return s.Count == other.Count &&
		s.FirstAttemptCount == other.FirstAttemptCount &&
		s.MaxRetries == other.MaxRetries &&
		s.NumRows.AlmostEqual(other.NumRows, eps) &&
		s.ParseLat.AlmostEqual(other.ParseLat, eps) &&
		s.PlanLat.AlmostEqual(other.PlanLat, eps) &&
		s.RunLat.AlmostEqual(other.RunLat, eps) &&
		s.ServiceLat.AlmostEqual(other.ServiceLat, eps) &&
		s.OverheadLat.AlmostEqual(other.OverheadLat, eps) &&
		s.SensitiveInfo.Equal(other.SensitiveInfo) &&
		s.BytesRead == other.BytesRead &&
		s.RowsRead == other.RowsRead &&
		s.FailedCount == other.FailedCount
}
