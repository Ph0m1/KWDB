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

package version

import (
	"fmt"
	"testing"
)

func TestGetters(t *testing.T) {
	v, err := Parse("V1.2.3-beta+md")
	if err != nil {
		t.Fatal(err)
	}
	str := fmt.Sprintf(
		"%d %d %d %s %s", v.Major(), v.Minor(), v.Patch(), v.PreRelease(), v.Metadata(),
	)
	exp := "1 2 3 beta md"
	if str != exp {
		t.Errorf("got '%s', expected '%s'", str, exp)
	}
}

func TestValid(t *testing.T) {
	testData := []string{
		"V0.0.0",
		"V0.0.1",
		"V0.1.0",
		"V1.0.0",
		"V1.0.0-alpha",
		"V1.0.0-beta.20190101",
		"V1.0.0-rc1-with-hyphen",
		"V1.0.0-rc2.dot.dot",
		"V1.2.3+metadata",
		"V1.2.3+metadata-with-hyphen",
		"V1.2.3+metadata.with.dots",
		"V1.1.2-beta.20190101+metadata",
		"V1.2.3-rc1-with-hyphen+metadata-with-hyphen",
	}
	for _, str := range testData {
		v, err := Parse(str)
		if err != nil {
			t.Errorf("%s: %s", str, err)
		}
		if v.String() != str {
			t.Errorf("%s roundtripped to %s", str, v.String())
		}
	}
}

func TestInvalid(t *testing.T) {
	testData := []string{
		"V1",
		"V1.2",
		"V1.2-beta",
		"V1x2.3",
		"V1.2x3",
		"1.0.0",
		" V1.0.0",
		"V1.0.0  ",
		"V1.2.beta",
		"V1.2-beta",
		"V1.2.3.beta",
		"V1.2.3-beta$",
		"V1.2.3-bet;a",
		"V1.2.3+metadata%",
		"V01.2.3",
		"V1.02.3",
		"V1.2.03",
	}
	for _, str := range testData {
		if _, err := Parse(str); err == nil {
			t.Errorf("expected error for %s", str)
		}
	}
}

func TestCompare(t *testing.T) {
	testData := []struct {
		a, b string
		cmp  int
	}{
		{"V1.0.0", "V1.0.0", 0},
		{"V1.0.0", "V1.0.1", -1},
		{"V1.2.3", "V1.3.0", -1},
		{"V1.2.3", "V2.0.0", -1},
		{"V1.0.0+metadata", "V1.0.0", 0},
		{"V1.0.0+metadata", "V1.0.0+other.metadata", 0},
		{"V1.0.1+metadata", "V1.0.0+other.metadata", +1},
		{"V1.0.0", "V1.0.0-alpha", +1},
		{"V1.0.0", "V1.0.0-rc2", +1},
		{"V1.0.0-alpha", "V1.0.0-beta", -1},
		{"V1.0.0-beta", "V1.0.0-rc.2", -1},
		{"V1.0.0-rc.2", "V1.0.0-rc.10", -1},
		{"V1.0.1", "V1.0.0-alpha", +1},

		// Tests below taken from coreos/go-semver, which carries the following
		// copyright:
		//
		// Copyright 2013-2015 CoreOS, Inc.
		// Copyright 2018 The Cockroach Authors.
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
		{"V0.0.0", "V0.0.0-foo", +1},
		{"V0.0.1", "V0.0.0", +1},
		{"V1.0.0", "V0.9.9", +1},
		{"V0.10.0", "V0.9.0", +1},
		{"V0.99.0", "V0.10.0", +1},
		{"V2.0.0", "V1.2.3", +1},
		{"V0.0.0", "V0.0.0-foo", +1},
		{"V0.0.1", "V0.0.0", +1},
		{"V1.0.0", "V0.9.9", +1},
		{"V0.10.0", "V0.9.0", +1},
		{"V0.99.0", "V0.10.0", +1},
		{"V2.0.0", "V1.2.3", +1},
		{"V0.0.0", "V0.0.0-foo", +1},
		{"V0.0.1", "V0.0.0", +1},
		{"V1.0.0", "V0.9.9", +1},
		{"V0.10.0", "V0.9.0", +1},
		{"V0.99.0", "V0.10.0", +1},
		{"V2.0.0", "V1.2.3", +1},
		{"V1.2.3", "V1.2.3-asdf", +1},
		{"V1.2.3", "V1.2.3-4", +1},
		{"V1.2.3", "V1.2.3-4-foo", +1},
		{"V1.2.3-5-foo", "V1.2.3-5", +1},
		{"V1.2.3-5", "V1.2.3-4", +1},
		{"V1.2.3-5-foo", "V1.2.3-5-Foo", +1},
		{"V3.0.0", "V2.7.2+asdf", +1},
		{"V3.0.0+foobar", "V2.7.2", +1},
		{"V1.2.3-a.10", "V1.2.3-a.5", +1},
		{"V1.2.3-a.b", "V1.2.3-a.5", +1},
		{"V1.2.3-a.b", "V1.2.3-a", +1},
		{"V1.2.3-a.b.c.10.d.5", "V1.2.3-a.b.c.5.d.100", +1},
		{"V1.0.0", "V1.0.0-rc.1", +1},
		{"V1.0.0-rc.2", "V1.0.0-rc.1", +1},
		{"V1.0.0-rc.1", "V1.0.0-beta.11", +1},
		{"V1.0.0-beta.11", "V1.0.0-beta.2", +1},
		{"V1.0.0-beta.2", "V1.0.0-beta", +1},
		{"V1.0.0-beta", "V1.0.0-alpha.beta", +1},
		{"V1.0.0-alpha.beta", "V1.0.0-alpha.1", +1},
		{"V1.0.0-alpha.1", "V1.0.0-alpha", +1},
	}
	for _, tc := range testData {
		a, err := Parse(tc.a)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Parse(tc.b)
		if err != nil {
			t.Fatal(err)
		}
		if cmp := a.Compare(b); cmp != tc.cmp {
			t.Errorf("'%s' vs '%s': expected %d, got %d", tc.a, tc.b, tc.cmp, cmp)
		}
		if cmp := b.Compare(a); cmp != -tc.cmp {
			t.Errorf("'%s' vs '%s': expected %d, got %d", tc.b, tc.a, -tc.cmp, cmp)
		}
	}
}
