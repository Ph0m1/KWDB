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

package base_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

func TestClusterIDContainerEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := &base.ClusterIDContainer{}

	if val := c.Get(); val != uuid.Nil {
		t.Errorf("initial value should be uuid.Nil, not %s", val)
	}
	if str := c.String(); str != "?" {
		t.Errorf("initial string should be ?, not %s", str)
	}
}

func TestClusterIDContainerSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := &base.ClusterIDContainer{}
	u := uuid.MakeV4()

	for i := 0; i < 2; i++ {
		c.Set(context.Background(), u)
		if val := c.Get(); val != u {
			t.Errorf("value should be %s, not %s", u, val)
		}
		if str := c.String(); str != u.String() {
			t.Errorf("string should be %s, not %s", u.String(), str)
		}
	}
}

func TestClusterIDContainerReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := &base.ClusterIDContainer{}
	uuid1 := uuid.MakeV4()
	uuid2 := uuid.MakeV4()

	c.Set(context.Background(), uuid1)
	c.Reset(uuid2)
	if val := c.Get(); val != uuid2 {
		t.Errorf("value should be %s, not %s", uuid2, val)
	}
	if str := c.String(); str != uuid2.String() {
		t.Errorf("string should be %s, not %s", uuid2.String(), str)
	}
}
