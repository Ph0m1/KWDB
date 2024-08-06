// Copyright 2019 The Cockroach Authors.
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

package log

import (
	"strconv"
	"strings"
	"sync/atomic"
)

const severityChar = "IWEF"

// get returns the value of the Severity.
func (s *Severity) get() Severity {
	return Severity(atomic.LoadInt32((*int32)(s)))
}

// set sets the value of the Severity.
func (s *Severity) set(val Severity) {
	atomic.StoreInt32((*int32)(s), int32(val))
}

// Set is part of the flag.Value interface.
func (s *Severity) Set(value string) error {
	var threshold Severity
	// Is it a known name?
	if v, ok := SeverityByName(value); ok {
		threshold = v
	} else {
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		threshold = Severity(v)
	}
	s.set(threshold)
	return nil
}

// Name returns the string representation of the severity (i.e. ERROR, INFO).
func (s *Severity) Name() string {
	return s.String()
}

// SeverityByName attempts to parse the passed in string into a severity. (i.e.
// ERROR, INFO). If it succeeds, the returned bool is set to true.
func SeverityByName(s string) (Severity, bool) {
	s = strings.ToUpper(s)
	if i, ok := Severity_value[s]; ok {
		return Severity(i), true
	}
	switch s {
	case "TRUE":
		return Severity_INFO, true
	case "FALSE":
		return Severity_NONE, true
	}
	return 0, false
}
