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

package row

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// fkExistenceCheckForInsert is an auxiliary object that facilitates the existence
// checks on the referenced table when inserting new rows in
// a referencing table.
type fkExistenceCheckForInsert struct {
	// fks maps mutated index id to slice of fkExistenceCheckBaseHelper, the outgoing
	// foreign key existence checkers for each mutated index.
	//
	// In an fkInsertHelper, these slices will have at most one entry,
	// since there can be (currently) at most one outgoing foreign key
	// per mutated index. We use this data structure instead of a
	// one-to-one map for consistency with the other helpers.
	//
	// TODO(knz): this limitation in CockroachDB is arbitrary and
	// incompatible with PostgreSQL. pg supports potentially multiple
	// referencing FK constraints for a single column tuple.
	fks map[sqlbase.IndexID][]fkExistenceCheckBaseHelper

	// checker is the object that actually carries out the lookups in
	// KV.
	checker *fkExistenceBatchChecker
}

// makeFkExistenceCheckHelperForInsert instantiates an insert helper.
func makeFkExistenceCheckHelperForInsert(
	ctx context.Context,
	txn *kv.Txn,
	table *sqlbase.ImmutableTableDescriptor,
	otherTables FkTableMetadata,
	colMap map[sqlbase.ColumnID]int,
	alloc *sqlbase.DatumAlloc,
) (fkExistenceCheckForInsert, error) {
	h := fkExistenceCheckForInsert{
		checker: &fkExistenceBatchChecker{
			txn: txn,
		},
	}

	// We need an existence check helper for every referenced table.
	for i := range table.OutboundFKs {
		ref := &table.OutboundFKs[i]
		// Look up the searched table.
		searchTable := otherTables[ref.ReferencedTableID].Desc
		if searchTable == nil {
			return h, errors.AssertionFailedf("referenced table %d not in provided table map %+v", ref.ReferencedTableID,
				otherTables)
		}
		searchIdx, err := sqlbase.FindFKReferencedIndex(searchTable.TableDesc(), ref.ReferencedColumnIDs)
		if err != nil {
			return h, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to find suitable search index for fk %q", ref.Name)
		}
		mutatedIdx, err := sqlbase.FindFKOriginIndex(table.TableDesc(), ref.OriginColumnIDs)
		if err != nil {
			return h, errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to find suitable search index for fk %q", ref.Name)
		}
		fk, err := makeFkExistenceCheckBaseHelper(txn, otherTables, ref, searchIdx, mutatedIdx, colMap, alloc, CheckInserts)
		if err == errSkipUnusedFK {
			continue
		}
		if err != nil {
			return h, err
		}
		if h.fks == nil {
			h.fks = make(map[sqlbase.IndexID][]fkExistenceCheckBaseHelper)
		}
		h.fks[mutatedIdx.ID] = append(h.fks[mutatedIdx.ID], fk)
	}

	if len(h.fks) > 0 {
		// TODO(knz,radu): FK existence checks need to see the writes
		// performed by the mutation.
		//
		// In order to make this true, we need to split the existence
		// checks into a separate sequencing step, and have the first
		// check happen no early than the end of all the "main" part of
		// the statement. Unfortunately, the organization of the code does
		// not allow this today.
		//
		// See: https://gitee.com/kwbasedb/kwbase/issues/33475
		//
		// In order to "make do" and preserve a modicum of FK semantics we
		// thus need to disable step-wise execution here. The result is that
		// it will also enable any interleaved read part to observe the
		// mutation, and thus introduce the risk of a Halloween problem for
		// any mutation that uses FK relationships.
		_ = txn.ConfigureStepping(ctx, kv.SteppingDisabled)
	}

	return h, nil
}

// addAllIdxChecks queues a FK existence check for every referenced table.
func (h fkExistenceCheckForInsert) addAllIdxChecks(
	ctx context.Context, row tree.Datums, traceKV bool,
) error {
	for idx := range h.fks {
		if err := queueFkExistenceChecksForRow(ctx, h.checker, h.fks[idx], row, traceKV); err != nil {
			return err
		}
	}
	return nil
}
