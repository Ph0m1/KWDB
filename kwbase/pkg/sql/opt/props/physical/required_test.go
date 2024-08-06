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

package physical_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

func TestRequiredProps(t *testing.T) {
	// Empty props.
	phys := &physical.Required{}
	testRequiredProps(t, phys, "[]")

	if phys.Defined() {
		t.Error("no props should be defined")
	}

	// Presentation props.
	presentation := physical.Presentation{
		opt.AliasedColumn{Alias: "a", ID: 1},
		opt.AliasedColumn{Alias: "b", ID: 2},
	}
	phys = &physical.Required{Presentation: presentation}
	testRequiredProps(t, phys, "[presentation: a:1,b:2]")

	if presentation.Any() {
		t.Error("presentation should not be empty")
	}

	if !presentation.Equals(presentation) {
		t.Error("presentation should equal itself")
	}

	if presentation.Equals(physical.Presentation(nil)) {
		t.Error("presentation should not equal the empty presentation")
	}

	if presentation.Equals(physical.Presentation{}) {
		t.Error("presentation should not equal the 0 column presentation")
	}

	if (physical.Presentation{}).Equals(physical.Presentation(nil)) {
		t.Error("0 column presentation should not equal the empty presentation")
	}

	// Add ordering props.
	ordering := physical.ParseOrderingChoice("+1,+5")
	phys.Ordering = ordering
	testRequiredProps(t, phys, "[presentation: a:1,b:2] [ordering: +1,+5]")
}

func testRequiredProps(t *testing.T, physProps *physical.Required, expected string) {
	t.Helper()
	actual := physProps.String()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual: %s", expected, actual)
	}
}
