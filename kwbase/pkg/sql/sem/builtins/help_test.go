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

package builtins

import (
	"regexp"
	"strings"
	"testing"
	"unicode"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestHelpFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test checks that all the built-in functions receive contextual help.
	for f := range builtins {
		if unicode.IsUpper(rune(f[0])) {
			continue
		}
		t.Run(f, func(t *testing.T) {
			_, err := parser.Parse("select " + f + "(??")
			if err == nil {
				t.Errorf("parser didn't trigger error")
				return
			}
			if !strings.HasPrefix(err.Error(), "help token in input") {
				t.Fatal(err)
			}
			pgerr := pgerror.Flatten(err)
			if !strings.HasPrefix(pgerr.Hint, "help:\n") {
				t.Errorf("expected 'help: ' prefix, got %q", pgerr.Hint)
				return
			}
			help := pgerr.Hint[6:]
			pattern := "Function:\\s+" + f + "\n"
			if m, err := regexp.MatchString(pattern, help); err != nil || !m {
				if err != nil {
					t.Errorf("pattern match failure: %v", err)
					return
				}
				t.Errorf("help text didn't match %q:\n%s", pattern, help)
			}
		})
	}
}
