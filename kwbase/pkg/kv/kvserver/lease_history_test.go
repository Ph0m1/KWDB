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

package kvserver

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestLeaseHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	history := newLeaseHistory()

	for i := 0; i < leaseHistoryMaxEntries; i++ {
		leases := history.get()
		if e, a := i, len(leases); e != a {
			t.Errorf("%d: expected history len to be %d , actual %d:\n%+v", i, e, a, leases)
		}
		if i > 0 {
			if e, a := int64(0), leases[0].Epoch; e != a {
				t.Errorf("%d: expected oldest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
			}
			if e, a := int64(i-1), leases[len(leases)-1].Epoch; e != a {
				t.Errorf("%d: expected newest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
			}
		}

		history.add(roachpb.Lease{
			Epoch: int64(i),
		})
	}

	// Now overflow the circular buffer.
	for i := 0; i < leaseHistoryMaxEntries; i++ {
		leases := history.get()
		if e, a := leaseHistoryMaxEntries, len(leases); e != a {
			t.Errorf("%d: expected history len to be %d , actual %d:\n%+v", i, e, a, leases)
		}
		if e, a := int64(i), leases[0].Epoch; e != a {
			t.Errorf("%d: expected oldest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
		}
		if e, a := int64(i+leaseHistoryMaxEntries-1), leases[leaseHistoryMaxEntries-1].Epoch; e != a {
			t.Errorf("%d: expected newest lease to have epoch of %d , actual %d:\n%+v", i, e, a, leases)
		}

		history.add(roachpb.Lease{
			Epoch: int64(i + leaseHistoryMaxEntries),
		})
	}
}
