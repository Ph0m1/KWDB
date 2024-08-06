// Copyright 2016 The Cockroach Authors.
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

package base_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestNodeIDContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	n := &base.NodeIDContainer{}

	if val := n.Get(); val != 0 {
		t.Errorf("initial value should be 0, not %d", val)
	}
	if str := n.String(); str != "?" {
		t.Errorf("initial string should be ?, not %s", str)
	}

	for i := 0; i < 2; i++ {
		n.Set(context.TODO(), 5)
		if val := n.Get(); val != 5 {
			t.Errorf("value should be 5, not %d", val)
		}
		if str := n.String(); str != "5" {
			t.Errorf("string should be 5, not %s", str)
		}
	}

	n.Reset(6)
	if val := n.Get(); val != 6 {
		t.Errorf("value should be 6, not %d", val)
	}
	if str := n.String(); str != "6" {
		t.Errorf("string should be 6, not %s", str)
	}
}
