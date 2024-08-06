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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package vm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZonePlacement(t *testing.T) {
	for i, c := range []struct {
		numZones, numNodes int
		expected           []int
	}{
		{1, 1, []int{0}},
		{1, 2, []int{0, 0}},
		{2, 4, []int{0, 0, 1, 1}},
		{2, 5, []int{0, 0, 1, 1, 0}},
		{3, 11, []int{0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 1}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.EqualValues(t, c.expected, ZonePlacement(c.numZones, c.numNodes))
		})
	}
}

func TestExpandZonesFlag(t *testing.T) {
	for i, c := range []struct {
		input, output []string
		expErr        string
	}{
		{
			input:  []string{"us-east1-b:3", "us-west2-c:2"},
			output: []string{"us-east1-b", "us-east1-b", "us-east1-b", "us-west2-c", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b:3", "us-west2-c"},
			output: []string{"us-east1-b", "us-east1-b", "us-east1-b", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b", "us-west2-c"},
			output: []string{"us-east1-b", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b", "us-west2-c:a2"},
			expErr: "failed to parse",
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			expanded, err := ExpandZonesFlag(c.input)
			if c.expErr != "" {
				if assert.Error(t, err) {
					assert.Regexp(t, c.expErr, err.Error())
				}
			} else {
				assert.EqualValues(t, c.output, expanded)
			}
		})
	}
}
