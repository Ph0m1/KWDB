// Copyright 2020 The Cockroach Authors.
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

package unique

import (
	"fmt"
	"reflect"
	"testing"
)

func TestUniquifyByteSlices(t *testing.T) {
	tests := []struct {
		input    []string
		expected []string
	}{
		{
			input:    []string{"foo", "foo"},
			expected: []string{"foo"},
		},
		{
			input:    []string{},
			expected: []string{},
		},
		{
			input:    []string{"", ""},
			expected: []string{""},
		},
		{
			input:    []string{"foo"},
			expected: []string{"foo"},
		},
		{
			input:    []string{"foo", "bar", "foo"},
			expected: []string{"bar", "foo"},
		},
		{
			input:    []string{"foo", "bar"},
			expected: []string{"bar", "foo"},
		},
		{
			input:    []string{"bar", "bar", "foo"},
			expected: []string{"bar", "foo"},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			input := make([][]byte, len(tt.input))
			expected := make([][]byte, len(tt.expected))
			for i := range tt.input {
				input[i] = []byte(tt.input[i])
			}
			for i := range tt.expected {
				expected[i] = []byte(tt.expected[i])
			}
			if got := UniquifyByteSlices(input); !reflect.DeepEqual(got, expected) {
				t.Errorf("UniquifyByteSlices() = %v, expected %v", got, expected)
			}
		})
	}
}
