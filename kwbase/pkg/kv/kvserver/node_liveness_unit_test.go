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

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestShouldReplaceLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := func(epo int64, expiration hlc.Timestamp, draining, decom bool) storagepb.Liveness {
		return storagepb.Liveness{
			Epoch:           epo,
			Expiration:      hlc.LegacyTimestamp(expiration),
			Draining:        draining,
			Decommissioning: decom,
		}
	}
	const (
		no  = false
		yes = true
	)
	now := hlc.Timestamp{WallTime: 12345}

	for _, test := range []struct {
		old, new storagepb.Liveness
		exp      bool
	}{
		{
			// Epoch update only.
			storagepb.Liveness{},
			l(1, hlc.Timestamp{}, false, false),
			yes,
		},
		{
			// No Epoch update, but Expiration update.
			l(1, now, false, false),
			l(1, now.Add(0, 1), false, false),
			yes,
		},
		{
			// No update.
			l(1, now, false, false),
			l(1, now, false, false),
			no,
		},
		{
			// Only Decommissioning changes.
			l(1, now, false, false),
			l(1, now, false, true),
			yes,
		},
		{
			// Only Draining changes.
			l(1, now, false, false),
			l(1, now, true, false),
			yes,
		},
		{
			// Decommissioning changes, but Epoch moves backwards.
			l(10, now, true, true),
			l(9, now, true, false),
			no,
		},
		{
			// Draining changes, but Expiration moves backwards..
			l(10, now, false, false),
			l(10, now.Add(-1, 0), true, false),
			no,
		},
	} {
		t.Run("", func(t *testing.T) {
			if act := shouldReplaceLiveness(test.old, test.new); act != test.exp {
				t.Errorf("unexpected update: %+v", test)
			}
		})
	}
}
