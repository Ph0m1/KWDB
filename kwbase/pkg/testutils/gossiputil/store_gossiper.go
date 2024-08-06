// Copyright 2015 The Cockroach Authors.
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

package gossiputil

import (
	"sync"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/gossip"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// StoreGossiper allows tests to push storeDescriptors into gossip and
// synchronize on their callbacks. There can only be one storeGossiper used per
// gossip instance.
type StoreGossiper struct {
	g           *gossip.Gossip
	mu          syncutil.Mutex
	cond        *sync.Cond
	storeKeyMap map[string]struct{}
}

// NewStoreGossiper creates a store gossiper for use by tests. It adds the
// callback to gossip.
func NewStoreGossiper(g *gossip.Gossip) *StoreGossiper {
	sg := &StoreGossiper{
		g:           g,
		storeKeyMap: make(map[string]struct{}),
	}
	sg.cond = sync.NewCond(&sg.mu)
	// Redundant callbacks are required by StoreGossiper. See GossipWithFunction
	// which waits for all of the callbacks to be invoked.
	g.RegisterCallback(gossip.MakePrefixPattern(gossip.KeyStorePrefix), func(key string, _ roachpb.Value) {
		sg.mu.Lock()
		defer sg.mu.Unlock()
		delete(sg.storeKeyMap, key)
		sg.cond.Broadcast()
	}, gossip.Redundant)
	return sg
}

// GossipStores queues up a list of stores to gossip and blocks until each one
// is gossiped before returning.
func (sg *StoreGossiper) GossipStores(storeDescs []*roachpb.StoreDescriptor, t *testing.T) {
	storeIDs := make([]roachpb.StoreID, len(storeDescs))
	for i, store := range storeDescs {
		storeIDs[i] = store.StoreID
	}
	sg.GossipWithFunction(storeIDs, func() {
		for i, storeDesc := range storeDescs {
			if err := sg.g.AddInfoProto(gossip.MakeStoreKey(storeIDs[i]), storeDesc, 0); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// GossipWithFunction calls gossipFn and blocks until gossip callbacks have
// fired on each of the stores specified by storeIDs.
func (sg *StoreGossiper) GossipWithFunction(storeIDs []roachpb.StoreID, gossipFn func()) {
	sg.mu.Lock()
	defer sg.mu.Unlock()
	sg.storeKeyMap = make(map[string]struct{})
	for _, storeID := range storeIDs {
		storeKey := gossip.MakeStoreKey(storeID)
		sg.storeKeyMap[storeKey] = struct{}{}
	}

	gossipFn()

	// Wait for gossip callbacks to be invoked on all the stores.
	for len(sg.storeKeyMap) > 0 {
		sg.cond.Wait()
	}
}
