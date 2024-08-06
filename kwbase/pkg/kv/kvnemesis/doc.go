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

// Package kvnemesis exercises the KV api with random traffic and then validates
// that the observed behaviors are consistent with our guarantees.
//
// A set of Operations are generated which represent usage of the public KV api.
// These include both "workload" operations like Gets and Puts as well as
// "admin" operations like rebalances. These Operations can be handed to an
// Applier, which runs them against the KV api and records the results.
//
// Operations do allow for concurrency (this testing is much less interesting
// otherwise), which means that the state of the KV map is not recoverable from
// _only_ the input. TODO(dan): We can use RangeFeed to recover the exact KV
// history. This plus some Kyle magic can be used to check our transactional
// guarantees.
//
// TODO
// - Validate read only transactions
// - CPut/InitPut/Increment/Delete
// - DeleteRange/ClearRange/RevertRange/Scan/ReverseScan
// - TransferLease
// - ExportRequest
// - AddSSTable
// - Root and leaf transactions
// - GCRequest
// - Protected timestamps
package kvnemesis
