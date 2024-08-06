// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sqlbase

import (
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// MakeKWDBMetadataKeyInt returns the primaryKey of a kwdb system table containing a single primary key
// column or a union primary key, used to obtain the kwdb metadata process. When primaryValues do not
// correspond one-to-one with the primary key column, it returns the startKey for scanning the entire
// or partial table
func MakeKWDBMetadataKeyInt(
	systemTable TableDescriptor, primaryValues []uint64,
) (roachpb.Key, error) {
	k := keys.MakeTablePrefix(uint32(systemTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(systemTable.PrimaryIndex.ID))
	for _, val := range primaryValues {
		k = encoding.EncodeUvarintAscending(k, val)
	}
	if primaryValues != nil && len(primaryValues) != 0 && len(primaryValues) == len(systemTable.PrimaryIndex.ColumnIDs) {
		k = keys.MakeFamilyKey(k, uint32(systemTable.NextFamilyID-1))
	}
	return k, nil
}

// MakeDropKWDBMetadataKeyInt Returns the family key of the row to be deleted
// from the kwdb system table.
func MakeDropKWDBMetadataKeyInt(
	systemTable TableDescriptor, primaryValues []uint64, idxValues []tree.Datum,
) ([]roachpb.Key, error) {
	var res []roachpb.Key

	for i := range systemTable.Indexes {
		idx := systemTable.Indexes[i]
		colMap := make(map[ColumnID]int)
		for i, id := range idx.ColumnIDs {
			colMap[id] = i
		}
		// We want to include empty k/v pairs because we want to delete all k/v's for this row.
		entries, err := EncodeSecondaryIndex(
			&systemTable, &systemTable.Indexes[i], colMap, idxValues, true /* includeEmpty */)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			res = append(res, e.Key)
		}
	}

	pk := keys.MakeTablePrefix(uint32(systemTable.ID))
	pk = encoding.EncodeUvarintAscending(pk, uint64(systemTable.PrimaryIndex.ID))
	for _, val := range primaryValues {
		pk = encoding.EncodeUvarintAscending(pk, val)
	}
	for i := range systemTable.Families {
		famkey := keys.MakeFamilyKey(pk, uint32(systemTable.Families[i].ID))
		k := make(roachpb.Key, len(famkey), cap(famkey))
		copy(k, famkey)
		res = append(res, k)
	}
	return res, nil
}

// MakeKWDBMetadataKeyString Returns a primaryKey that contains a single primary key column
// or a joint primary key in the kwdb system table, mostly used to obtain kwdb metadata flow.
// When primaryValues and primary key columns do not correspond, startKey for scanning all
// or part of the table is returned.
func MakeKWDBMetadataKeyString(systemTable TableDescriptor, primaryValues []string) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(systemTable.ID))
	k = encoding.EncodeUvarintAscending(k, uint64(systemTable.PrimaryIndex.ID))
	for _, val := range primaryValues {
		k = encoding.EncodeStringAscending(k, val)
	}
	if primaryValues != nil && len(primaryValues) != 0 && len(primaryValues) == len(systemTable.PrimaryIndex.ColumnIDs) {
		if len(systemTable.Families) > 2 {
			k = keys.MakeFamilyKey(k, uint32(systemTable.Families[1].ID))
		}
		k = keys.MakeFamilyKey(k, uint32(systemTable.NextFamilyID-1))
	}
	return k
}

// DecodeKWDBTableKey decode the primary key of the kwdb system table, which can be a
// single primary key column or a joint primary key. The column types can be int and string
func DecodeKWDBTableKey(
	k roachpb.Key, index *IndexDescriptor, systemTable TableDescriptor,
) (tree.Datums, error) {
	var res tree.Datums
	// decode table id
	k, _, err := keys.DecodeTablePrefix(k)
	if err != nil {
		return nil, err
	}
	var buf uint64
	// decode primary index id
	k, buf, err = encoding.DecodeUvarintAscending(k)
	if err != nil {
		return nil, err
	}
	if buf != uint64(index.ID) {
		return nil, errors.Newf("tried get table %d, but got %d", index.ID, buf)
	}

	for _, id := range index.ColumnIDs {
		col, err := systemTable.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		switch col.Type.Family() {
		case types.IntFamily:
			k, buf, err = encoding.DecodeUvarintAscending(k)
			if err != nil {
				return nil, err
			}
			res = append(res, tree.NewDInt(tree.DInt(buf)))
		case types.StringFamily:
			var bytesBuf []byte
			k, bytesBuf, err = encoding.DecodeBytesAscending(k, nil)
			if err != nil {
				return nil, err
			}
			res = append(res, tree.NewDString(string(bytesBuf)))
		default:
			return nil, errors.New("unexpected column type")
		}
	}
	return res, nil
}
