// Copyright 2016 The Cockroach Authors.
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

package util

import "testing"

func TestGetSingleRune(t *testing.T) {
	tests := []struct {
		s        string
		expected rune
		err      bool
	}{
		{"a", 'a', false},
		{"", 0, false},
		{"🐛"[:1], 0, true},
		{"aa", 'a', true},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := GetSingleRune(tc.s)
			if (err != nil) != tc.err {
				t.Fatalf("got unexpected err: %v", err)
			}
			if tc.expected != got {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	testCases := []struct {
		s string
		// res stores the expected results for maxRunes=0,1,2,3,etc.
		res []string
	}{
		{"", []string{"", ""}},
		{"abcd", []string{"", "a", "ab", "abc", "abcd", "abcd", "abcd"}},
		{"🐛🏠", []string{"", "🐛", "🐛🏠", "🐛🏠", "🐛🏠"}},
		{"a🐛b🏠c", []string{"", "a", "a🐛", "a🐛b", "a🐛b🏠", "a🐛b🏠c", "a🐛b🏠c"}},
		{
			// Test with an invalid UTF-8 sequence.
			"\xf0\x90\x28\xbc",
			[]string{"", "\xf0", "\xf0\x90", "\xf0\x90\x28", "\xf0\x90\x28\xbc", "\xf0\x90\x28\xbc"},
		},
	}

	for _, tc := range testCases {
		for i := range tc.res {
			if r := TruncateString(tc.s, i); r != tc.res[i] {
				t.Errorf("TruncateString(\"%q\", %d) = \"%q\"; expected \"%q\"", tc.s, i, r, tc.res[i])
			}
		}
	}
}
