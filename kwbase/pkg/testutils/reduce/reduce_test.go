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

package reduce_test

import (
	"context"
	"go/parser"
	"regexp"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/reduce"
)

func TestReduceGo(t *testing.T) {
	reduce.Walk(t, "testdata", nil /* filter */, isInterestingGo, reduce.ModeInteresting, goPasses)
}

var (
	goPasses = []reduce.Pass{
		removeLine,
		simplifyConsts,
	}
	removeLine = reduce.MakeIntPass("remove line", func(s string, i int) (string, bool, error) {
		sp := strings.Split(s, "\n")
		if i >= len(sp) {
			return "", false, nil
		}
		out := strings.Join(append(sp[:i], sp[i+1:]...), "\n")
		return out, true, nil
	})
	simplifyConstsRE = regexp.MustCompile(`[a-z0-9][a-z0-9]+`)
	simplifyConsts   = reduce.MakeIntPass("simplify consts", func(s string, i int) (string, bool, error) {
		out := simplifyConstsRE.ReplaceAllStringFunc(s, func(found string) string {
			i--
			if i == -1 {
				return found[:1]
			}
			return found
		})
		return out, i < 0, nil
	})
)

func isInterestingGo(contains string) reduce.InterestingFn {
	return func(ctx context.Context, f reduce.File) bool {
		_, err := parser.ParseExpr(string(f))
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), contains)
	}
}
