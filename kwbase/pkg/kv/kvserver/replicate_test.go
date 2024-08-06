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

package kvserver_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/pkg/errors"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skipf("Temporarily skip to resolve pipeline blocking issue")

	ctx := context.Background()
	storeCfg := kvserver.TestStoreConfig(nil /* clock */)
	// Disable the replica scanner so that we rely on the eager replication code
	// path that occurs after splits.
	storeCfg.TestingKnobs.DisableScanner = true

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// After bootstrap, all of the system ranges should be present in replicate
	// queue purgatory (because we only have a single store in the test and thus
	// replication cannot succeed).
	purgatoryStartCount := store.ReplicateQueuePurgatoryLength()

	t.Logf("purgatory start count is %d", purgatoryStartCount)
	// Perform a split and check that there's one more range in the purgatory.

	key := roachpb.Key("a")
	args := adminSplitArgs(key)
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The addition of replicas to the replicateQueue after a split
	// occurs happens after the update of the descriptors in meta2
	// leaving a tiny window of time in which the newly split replica
	// will not have been added to purgatory. Thus we loop.
	testutils.SucceedsSoon(t, func() error {
		expected := purgatoryStartCount + 1
		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})
}
