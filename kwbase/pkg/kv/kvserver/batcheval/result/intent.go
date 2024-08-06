// Copyright 2014 The Cockroach Authors.
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

package result

import (
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/concurrency/lock"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
)

// FromEncounteredIntents creates a Result communicating that the intents were
// encountered and should be handled.
func FromEncounteredIntents(intents []roachpb.Intent) Result {
	var pd Result
	if len(intents) == 0 {
		return pd
	}
	pd.Local.EncounteredIntents = intents
	return pd
}

// FromAcquiredLocks creates a Result communicating that the locks were
// acquired or re-acquired by the given transaction and should be handled.
func FromAcquiredLocks(txn *roachpb.Transaction, keys ...roachpb.Key) Result {
	var pd Result
	if txn == nil {
		return pd
	}
	pd.Local.AcquiredLocks = make([]roachpb.LockUpdate, len(keys))
	for i := range pd.Local.AcquiredLocks {
		pd.Local.AcquiredLocks[i] = roachpb.LockUpdate{
			Span:       roachpb.Span{Key: keys[i]},
			Txn:        txn.TxnMeta,
			Status:     roachpb.PENDING,
			Durability: lock.Replicated,
		}
	}
	return pd
}

// EndTxnIntents contains a finished transaction and a bool (Always),
// which indicates whether the intents should be resolved whether or
// not the command succeeds through Raft.
type EndTxnIntents struct {
	Txn    *roachpb.Transaction
	Always bool
	Poison bool
}

// FromEndTxn creates a Result communicating that a transaction was
// completed and its locks should be resolved.
func FromEndTxn(txn *roachpb.Transaction, alwaysReturn, poison bool) Result {
	var pd Result
	if len(txn.LockSpans) == 0 {
		return pd
	}
	pd.Local.EndTxns = []EndTxnIntents{{Txn: txn, Always: alwaysReturn, Poison: poison}}
	return pd
}
