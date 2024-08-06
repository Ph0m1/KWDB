// Copyright 2015 The Cockroach Authors.
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

package testutils

import (
	"regexp"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
)

// IsError returns true if the error string matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	errString := pgerror.FullError(err)
	matched, merr := regexp.MatchString(re, errString)
	if merr != nil {
		return false
	}
	return matched
}

// IsPError returns true if pErr's message matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsPError(pErr *roachpb.Error, re string) bool {
	if pErr == nil && re == "" {
		return true
	}
	if pErr == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, pErr.Message)
	if merr != nil {
		return false
	}
	return matched
}
