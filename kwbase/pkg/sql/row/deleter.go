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

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Deleter abstracts the key/value operations for deleting table rows.
type Deleter struct {
	Helper    rowHelper
	FetchCols []sqlbase.ColumnDescriptor
	// FetchColIDtoRowIndex must be kept in sync with FetchCols.
	FetchColIDtoRowIndex map[sqlbase.ColumnID]int
	Fks                  fkExistenceCheckForDelete
	cascader             *cascader
	// For allocation avoidance.
	key roachpb.Key
}

// MakeDeleter creates a Deleter for the given table.
//
// The returned Deleter contains a FetchCols field that defines the
// expectation of which values are passed as values to DeleteRow. If
// requestedCols is non-nil, then only the requested columns are included in
// FetchCols; otherwise, all columns that are part of the key of any index
// (either primary or secondary) are included in FetchCols.
func MakeDeleter(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	evalCtx *tree.EvalContext,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {

	rowDeleter, err := makeRowDeleterWithoutCascader(
		ctx, txn, tableDesc, fkTables, requestedCols, checkFKs, alloc,
	)
	if err != nil {
		return Deleter{}, err
	}
	if checkFKs == CheckFKs {
		var err error
		rowDeleter.cascader, err = makeDeleteCascader(ctx, txn, tableDesc, fkTables, evalCtx, alloc)
		if err != nil {
			return Deleter{}, err
		}
		// If we are performing a cascade operation for a particular foreign
		// key constraint, we don't also need to perform a foreign key
		// existence check after the delete for the same foreign key
		// constraint. This pass removes unnecessary existence helpers.
		// In particular, we omit checks for CASCADE and SET NULL because
		// after the cascader has finished deleting or setting rows to
		// NULL, we don't need to verify the result of those operations.
		// TODO (rohany): This code will be removed once the optimizer
		//  handles cascade operations.
		for k, helpers := range rowDeleter.Fks.fks {
			index := 0
			for i := range helpers {
				helper := &helpers[i]
				if helper.ref.OnDelete == sqlbase.ForeignKeyReference_CASCADE ||
					helper.ref.OnDelete == sqlbase.ForeignKeyReference_SET_NULL {
					continue
				}
				helpers[index] = *helper
				index++
			}
			rowDeleter.Fks.fks[k] = helpers[:index]
		}
	}
	return rowDeleter, nil
}

// makeRowDeleterWithoutCascader creates a rowDeleter but does not create an
// additional cascader.
func makeRowDeleterWithoutCascader(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	fkTables FkTableMetadata,
	requestedCols []sqlbase.ColumnDescriptor,
	checkFKs checkFKConstraints,
	alloc *sqlbase.DatumAlloc,
) (Deleter, error) {
	indexes := tableDesc.DeletableIndexes()

	var fetchCols []sqlbase.ColumnDescriptor
	var fetchColIDtoRowIndex map[sqlbase.ColumnID]int
	if requestedCols != nil {
		fetchCols = requestedCols[:len(requestedCols):len(requestedCols)]
		fetchColIDtoRowIndex = ColIDtoRowIndexFromCols(fetchCols)
	} else {
		fetchColIDtoRowIndex = make(map[sqlbase.ColumnID]int)
		maybeAddCol := func(colID sqlbase.ColumnID) error {
			if _, ok := fetchColIDtoRowIndex[colID]; !ok {
				col, err := tableDesc.FindColumnByID(colID)
				if err != nil {
					return err
				}
				fetchColIDtoRowIndex[col.ID] = len(fetchCols)
				fetchCols = append(fetchCols, *col)
			}
			return nil
		}
		for _, colID := range tableDesc.PrimaryIndex.ColumnIDs {
			if err := maybeAddCol(colID); err != nil {
				return Deleter{}, err
			}
		}
		for _, index := range indexes {
			for _, colID := range index.ColumnIDs {
				if err := maybeAddCol(colID); err != nil {
					return Deleter{}, err
				}
			}
			// The extra columns are needed to fix #14601.
			for _, colID := range index.ExtraColumnIDs {
				if err := maybeAddCol(colID); err != nil {
					return Deleter{}, err
				}
			}
		}
	}

	rd := Deleter{
		Helper:               newRowHelper(tableDesc, indexes),
		FetchCols:            fetchCols,
		FetchColIDtoRowIndex: fetchColIDtoRowIndex,
	}
	if checkFKs == CheckFKs {
		var err error
		if rd.Fks, err = makeFkExistenceCheckHelperForDelete(ctx, txn, tableDesc, fkTables,
			fetchColIDtoRowIndex, alloc); err != nil {
			return Deleter{}, err
		}
	}

	return rd, nil
}

// DeleteRow adds to the batch the kv operations necessary to delete a table row
// with the given values. It also will cascade as required and check for
// orphaned rows. The bytesMonitor is only used if cascading/fk checking and can
// be nil if not.
func (rd *Deleter) DeleteRow(
	ctx context.Context,
	b *kv.Batch,
	values []tree.Datum,
	checkFKs checkFKConstraints,
	traceKV bool,
	txn *kv.Txn,
) error {

	// todo(fxy/zhuodong):
	// 1. We can report err with tbl name later
	if txn != nil {
		tblDesc := rd.Helper.TableDesc.TableDesc()
		isReplTbl := tblDesc.IsReplTable && tblDesc.ParentID > 1
		if isReplTbl {
			txn.ModifyReplTbl = true
		} else {
			txn.ModifyNonReplTbl = true
		}
		if txn.ModifyReplTbl && txn.ModifyNonReplTbl {
			return errors.Errorf("Txn don't support modify repl tables and non repl tables!")
		}
	}

	// Delete the row from any secondary indices.
	for i := range rd.Helper.Indexes {
		// We want to include empty k/v pairs because we want to delete all k/v's for this row.
		entries, err := sqlbase.EncodeSecondaryIndex(
			rd.Helper.TableDesc.TableDesc(), &rd.Helper.Indexes[i], rd.FetchColIDtoRowIndex, values, true /* includeEmpty */)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if traceKV {
				log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.secIndexValDirs[i], e.Key))
			}
			b.Del(&e.Key)
		}
	}

	primaryIndexKey, err := rd.Helper.encodePrimaryIndex(rd.FetchColIDtoRowIndex, values)
	if err != nil {
		return err
	}

	// Delete the row.
	for i := range rd.Helper.TableDesc.Families {
		if i > 0 {
			// HACK: MakeFamilyKey appends to its argument, so on every loop iteration
			// after the first, trim primaryIndexKey so nothing gets overwritten.
			// TODO(dan): Instead of this, use something like engine.ChunkAllocator.
			primaryIndexKey = primaryIndexKey[:len(primaryIndexKey):len(primaryIndexKey)]
		}
		familyID := rd.Helper.TableDesc.Families[i].ID
		rd.key = keys.MakeFamilyKey(primaryIndexKey, uint32(familyID))
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", keys.PrettyPrint(rd.Helper.primIndexValDirs, rd.key))
		}
		b.Del(&rd.key)
		rd.key = nil
	}

	if rd.cascader != nil {
		if err := rd.cascader.cascadeAll(
			ctx,
			rd.Helper.TableDesc,
			tree.Datums(values),
			nil, /* updatedValues */
			rd.FetchColIDtoRowIndex,
			traceKV,
		); err != nil {
			return err
		}
	}
	if rd.Fks.checker != nil && checkFKs == CheckFKs {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		return rd.Fks.checker.runCheck(ctx, values, nil)
	}
	return nil
}

// DeleteIndexRow adds to the batch the kv operations necessary to delete a
// table row from the given index.
func (rd *Deleter) DeleteIndexRow(
	ctx context.Context,
	b *kv.Batch,
	idx *sqlbase.IndexDescriptor,
	values []tree.Datum,
	traceKV bool,
	txn *kv.Txn,
) error {

	// todo(fxy/zhuodong):
	// 1. We can report err with tbl name later
	if txn != nil {
		tblDesc := rd.Helper.TableDesc.TableDesc()
		isReplTbl := tblDesc.IsReplTable && tblDesc.ParentID > 1
		if isReplTbl {
			txn.ModifyReplTbl = true
		} else {
			txn.ModifyNonReplTbl = true
		}
		if txn.ModifyReplTbl && txn.ModifyNonReplTbl {
			return errors.Errorf("Txn don't support modify repl tables and non repl tables!")
		}
	}

	if rd.Fks.checker != nil {
		if err := rd.Fks.addAllIdxChecks(ctx, values, traceKV); err != nil {
			return err
		}
		if err := rd.Fks.checker.runCheck(ctx, values, nil); err != nil {
			return err
		}
	}
	// We want to include empty k/v pairs because we want
	// to delete all k/v's for this row. By setting includeEmpty
	// to true, we will get a k/v pair for each family in the row,
	// which will guarantee that we delete all the k/v's in this row.
	secondaryIndexEntry, err := sqlbase.EncodeSecondaryIndex(
		rd.Helper.TableDesc.TableDesc(), idx, rd.FetchColIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		return err
	}

	for _, entry := range secondaryIndexEntry {
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", entry.Key)
		}
		b.Del(entry.Key)
	}
	return nil
}
