// Copyright 2018 The Cockroach Authors.
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

package hba

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/kr/pretty"
)

func TestParse(t *testing.T) {
	datadriven.RunTest(t, filepath.Join("testdata", "parse"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "multiline":
				conf, err := Parse(td.Input)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				var out strings.Builder
				fmt.Fprintf(&out, "# String render check:\n%s", conf)
				fmt.Fprintf(&out, "# Detail:\n%# v", pretty.Formatter(conf))
				return out.String()

			case "line":
				tokens, err := tokenize(td.Input)
				if err != nil {
					td.Fatalf(t, "%v", err)
				}
				if len(tokens.lines) != 1 {
					td.Fatalf(t, "line parse only valid with one line of input")
				}
				prefix := "" // For debugging, use prefix := pretty.Sprint(tokens.lines[0]) + "\n"
				entry, err := parseHbaLine(tokens.lines[0])
				if err != nil {
					return prefix + fmt.Sprintf("error: %v\n", err)
				}
				return prefix + entry.String()

			default:
				return fmt.Sprintf("unknown directive: %s", td.Cmd)
			}
		})
}

func TestParseAndNormalizeAuthConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.RunTest(t, filepath.Join("testdata", "normalization"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "hba":
				conf, err := ParseAndNormalize(td.Input)
				if err != nil {
					return fmt.Sprintf("error: %v\n", err)
				}
				return conf.String()
			default:
				t.Fatalf("unknown directive: %s", td.Cmd)
			}
			return ""
		})
}

func TestMatchConnType(t *testing.T) {
	testCases := []struct {
		conf, conn ConnType
		match      bool
	}{
		{ConnLocal, ConnHostSSL, false},
		{ConnLocal, ConnHostNoSSL, false},
		{ConnLocal, ConnLocal, true},
		{ConnHostAny, ConnLocal, false},
		{ConnHostAny, ConnHostSSL, true},
		{ConnHostAny, ConnHostNoSSL, true},
		{ConnHostSSL, ConnLocal, false},
		{ConnHostSSL, ConnHostSSL, true},
		{ConnHostSSL, ConnHostNoSSL, false},
		{ConnHostNoSSL, ConnLocal, false},
		{ConnHostNoSSL, ConnHostSSL, false},
		{ConnHostNoSSL, ConnHostNoSSL, true},
	}
	for _, tc := range testCases {
		entry := Entry{ConnType: tc.conf}
		if m := entry.ConnTypeMatches(tc.conn); m != tc.match {
			t.Errorf("%s vs %s: expected %v, got %v", tc.conf, tc.conn, tc.match, m)
		}
	}
}

// TODO(mjibson): these are untested outside ccl +gss builds.
var _ = Entry.GetOption
var _ = Entry.GetOptions
