// Copyright 2019 The Cockroach Authors.
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

package kvcoord

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/storagebase"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	abortKey := roachpb.Key("abort")
	errKey := roachpb.Key("injectErr")

	datadriven.Walk(t, "testdata/savepoints", func(t *testing.T, path string) {
		// We want to inject txn abort errors in some cases.
		//
		// We do this by injecting the error from "underneath" the
		// TxnCoordSender, from storage.
		params := base.TestServerArgs{}
		var doAbort int64
		params.Knobs.Store = &kvserver.StoreTestingKnobs{
			EvalKnobs: storagebase.BatchEvalTestingKnobs{
				TestingEvalFilter: func(args storagebase.FilterArgs) *roachpb.Error {
					key := args.Req.Header().Key
					if atomic.LoadInt64(&doAbort) != 0 && key.Equal(abortKey) {
						return roachpb.NewErrorWithTxn(
							roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_UNKNOWN), args.Hdr.Txn)
					}
					if key.Equal(errKey) {
						return roachpb.NewErrorf("injected error")
					}
					return nil
				},
			},
		}

		// New database for each test file.
		s, _, db := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		// Transient state during the test.
		sp := make(map[string]kv.SavepointToken)
		var txn *kv.Txn

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			var buf strings.Builder

			ptxn := func() {
				tc := txn.Sender().(*TxnCoordSender)
				fmt.Fprintf(&buf, "%d ", tc.interceptorAlloc.txnSeqNumAllocator.writeSeq)
				if len(tc.mu.txn.IgnoredSeqNums) == 0 {
					buf.WriteString("<noignore>")
				}
				for _, r := range tc.mu.txn.IgnoredSeqNums {
					fmt.Fprintf(&buf, "[%d-%d]", r.Start, r.End)
				}
				fmt.Fprintln(&buf)
			}

			switch td.Cmd {
			case "begin":
				txn = kv.NewTxn(ctx, db, 0)
				ptxn()

			case "commit":
				if err := txn.CommitOrCleanup(ctx); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				}

			case "retry":
				epochBefore := txn.Epoch()
				retryErr := txn.GenerateForcedRetryableError(ctx, "forced retry")
				epochAfter := txn.Epoch()
				fmt.Fprintf(&buf, "synthetic error: %v\n", retryErr)
				fmt.Fprintf(&buf, "epoch: %d -> %d\n", epochBefore, epochAfter)

			// inject-error runs a Get with an untyped error injected into request
			// evaluation.
			case "inject-error":
				_, err := txn.Get(ctx, errKey)
				require.Regexp(t, "injected error", err.Error())
				fmt.Fprint(&buf, "injected error\n")

			case "abort":
				prevID := txn.ID()
				atomic.StoreInt64(&doAbort, 1)
				defer func() { atomic.StoreInt64(&doAbort, 00) }()
				err := txn.Put(ctx, abortKey, []byte("value"))
				fmt.Fprintf(&buf, "(%T)\n", err)
				changed := "changed"
				if prevID == txn.ID() {
					changed = "not changed"
				}
				fmt.Fprintf(&buf, "txn id %s\n", changed)

			case "put":
				if err := txn.Put(ctx,
					roachpb.Key(td.CmdArgs[0].Key),
					[]byte(td.CmdArgs[1].Key)); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				}

			// cput takes <key> <value> <expected value>. The expected value can be
			// "nil".
			case "cput":
				expS := td.CmdArgs[2].Key
				var expVal *roachpb.Value
				if expS != "nil" {
					val := roachpb.MakeValueFromBytes([]byte(expS))
					expVal = &val
				}
				if err := txn.CPut(ctx,
					roachpb.Key(td.CmdArgs[0].Key),
					[]byte(td.CmdArgs[1].Key),
					expVal,
				); err != nil {
					if _, ok := err.(*roachpb.ConditionFailedError); ok {
						// Print an easier to match message.
						fmt.Fprintf(&buf, "(%T) unexpected value\n", err)
					} else {
						fmt.Fprintf(&buf, "(%T) %v\n", err, err)
					}
				}

			case "get":
				v, err := txn.Get(ctx, td.CmdArgs[0].Key)
				if err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ba, _ := v.Value.GetBytes()
					fmt.Fprintf(&buf, "%v -> %v\n", v.Key, string(ba))
				}

			case "savepoint":
				spt, err := txn.CreateSavepoint(ctx)
				if err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					sp[td.CmdArgs[0].Key] = spt
					ptxn()
				}

			case "release":
				spn := td.CmdArgs[0].Key
				spt := sp[spn]
				if err := txn.ReleaseSavepoint(ctx, spt); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ptxn()
				}

			case "rollback":
				spn := td.CmdArgs[0].Key
				spt := sp[spn]
				if err := txn.RollbackToSavepoint(ctx, spt); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ptxn()
				}

			default:
				td.Fatalf(t, "unknown directive: %s", td.Cmd)
			}
			return buf.String()
		})
	})
}
