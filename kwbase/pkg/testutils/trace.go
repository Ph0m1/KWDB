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

package testutils

import (
	"regexp"

	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/tracing"
	"github.com/pkg/errors"
)

// MakeAmbientCtx creates an AmbientContext with a Tracer in it.
func MakeAmbientCtx() log.AmbientContext {
	return log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}
}

// MatchInOrder matches interprets the given slice of strings as a slice of
// regular expressions and checks that they match, in order and without overlap,
// against the given string. For example, if s=abcdefg and res=ab,cd,fg no error
// is returned, whereas res=abc,cde would return a descriptive error about
// failing to match cde.
func MatchInOrder(s string, res ...string) error {
	sPos := 0
	for i := range res {
		reStr := "(?ms)" + res[i]
		re, err := regexp.Compile(reStr)
		if err != nil {
			return errors.Errorf("regexp %d (%q) does not compile: %s", i, reStr, err)
		}
		loc := re.FindStringIndex(s[sPos:])
		if loc == nil {
			// Not found.
			return errors.Errorf(
				"unable to find regexp %d (%q) in remaining string:\n\n%s\n\nafter having matched:\n\n%s",
				i, reStr, s[sPos:], s[:sPos],
			)
		}
		sPos += loc[1]
	}
	return nil
}
