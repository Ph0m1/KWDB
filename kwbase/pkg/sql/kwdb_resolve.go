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

package sql

import (
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// GetKWDBHashRoutingByID gets hash routing info by id.
func GetKWDBHashRoutingByID(
	ctx context.Context, txn *kv.Txn, id uint64,
) (*api.KWDBHashRouting, error) {
	hashRoutingKey, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, []uint64{id})
	if err != nil {
		return nil, err
	}
	row, err := sqlbase.GetKWDBMetadataRowWithUnConsistency(ctx, txn, hashRoutingKey, sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return nil, err
	}
	return makeHashRoutingByRow(row)
}

// GetKWDBHashRoutingByIDInTxn gets hash routing info by id.
// It must be called in a txn flow, the request will be sent directly bt sender.
func GetKWDBHashRoutingByIDInTxn(
	ctx context.Context, id uint64, sender kv.Sender, header *roachpb.Header,
) (*api.KWDBHashRouting, error) {
	hashRoutingKey, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, []uint64{id})
	if err != nil {
		return nil, err
	}
	var ba roachpb.BatchRequest
	ba.Header = *header
	ba.Add(roachpb.NewGet(hashRoutingKey))
	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	if len(br.Responses) == 0 {
		return nil, errors.New("get empty result")
	}
	keyVal := kv.KeyValue{
		Key:   hashRoutingKey,
		Value: br.Responses[0].GetInner().(*roachpb.GetResponse).Value,
	}
	row, err := sqlbase.GetMetadataRowsFromKeyValue(ctx, &keyVal, &sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return nil, err
	}
	return makeHashRoutingByRow(row)
}

// GetKWDBHashRoutingsByTableID gets hash routing info by table id.
func GetKWDBHashRoutingsByTableID(
	ctx context.Context, txn *kv.Txn, tableID uint32,
) ([]*api.KWDBHashRouting, error) {
	tableIDKey := keys.MakeTablePrefix(uint32(sqlbase.KWDBHashRoutingTable.ID))
	tableIDKey = encoding.EncodeUvarintAscending(tableIDKey, uint64(sqlbase.KWDBHashRoutingTable.Indexes[0].ID))
	tableIDKey = encoding.EncodeVarintAscending(tableIDKey, int64(tableID))

	pks, err := sqlbase.GetKWDBMetadataRowsWithUnConsistency(ctx, txn, tableIDKey, sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return nil, err
	}

	// get rows by primary key
	var hashRoutings []*api.KWDBHashRouting
	for _, pk := range pks {
		id := uint64(tree.MustBeDInt(pk[1]))
		hashRouting, err := GetKWDBHashRoutingByID(ctx, txn, id)
		if err != nil {
			return nil, err
		}
		hashRoutings = append(hashRoutings, hashRouting)
	}
	return hashRoutings, nil
}

// GetAllKWDBHashRoutings gets all hash routing info.
func GetAllKWDBHashRoutings(ctx context.Context, txn *kv.Txn) ([]*api.KWDBHashRouting, error) {
	tableKey, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, nil)
	if err != nil {
		return nil, err
	}
	rows, err := sqlbase.GetKWDBMetadataRowsWithUnConsistency(ctx, txn, tableKey, sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return nil, err
	}
	var hashRoutings []*api.KWDBHashRouting
	for i := range rows {
		hashRouting, err := makeHashRoutingByRow(rows[i])
		if err != nil {
			return nil, err
		}
		hashRoutings = append(hashRoutings, hashRouting)
	}
	return hashRoutings, nil
}

// DeleteKWDBHashRoutingByID delete all rows from kwdb_hash_routing.
func DeleteKWDBHashRoutingByID(ctx context.Context, txn *kv.Txn, id uint64) error {
	b := txn.NewBatch()
	pk, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, []uint64{id})
	if err != nil {
		return err
	}
	hashRoutingKey, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, []uint64{id})
	if err != nil {
		return err
	}
	row, err := sqlbase.GetKWDBMetadataRow(ctx, txn, hashRoutingKey, sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return err
	}
	if len(row) < 2 {
		return pgerror.Newf(pgcode.DataException, "get primary key and index key failed:%v", row)
	}
	indexKey := []tree.Datum{row[1], row[0]}
	key, err := getHashRoutingIndexKey(sqlbase.KWDBHashRoutingTable, indexKey)
	if err != nil {
		return err
	}
	for _, v := range key {
		b.Del(v)
	}
	b.Del(pk)
	return txn.Run(ctx, b)
}

// DeleteKWDBHashRoutingByTableID delete all rows from kwdb_hash_routing.
func DeleteKWDBHashRoutingByTableID(ctx context.Context, txn *kv.Txn, tableID uint64) error {
	idKey := keys.MakeTablePrefix(uint32(sqlbase.KWDBHashRoutingTable.ID))
	idKey = encoding.EncodeUvarintAscending(idKey, uint64(sqlbase.KWDBHashRoutingTable.Indexes[0].ID))
	idKey = encoding.EncodeVarintAscending(idKey, int64(tableID))

	pks, err := sqlbase.GetKWDBMetadataRows(ctx, txn, idKey, sqlbase.KWDBHashRoutingTable)
	if err != nil {
		return err
	}
	b := txn.NewBatch()

	// delete rows by primary key
	for _, pk := range pks {
		id := uint64(tree.MustBeDInt(pk[1]))
		k, err := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.KWDBHashRoutingTable, []uint64{id})
		if err != nil {
			return err
		}
		b.Del(k)
		key, err := getHashRoutingIndexKey(sqlbase.KWDBHashRoutingTable, pk)
		for _, v := range key {
			b.Del(v)
		}
	}
	return txn.Run(ctx, b)
}

// getHashRoutingIndexKey get Index for delete.
func getHashRoutingIndexKey(
	systemTable TableDescriptor, indexKey tree.Datums,
) ([]roachpb.Key, error) {
	var key []roachpb.Key
	for i := range systemTable.Indexes {
		idx := systemTable.Indexes[i]
		colMap := make(map[sqlbase.ColumnID]int)
		for i, id := range idx.ColumnIDs {
			colMap[id] = i
		}
		colMap[idx.ExtraColumnIDs[0]] = 1
		// We want to include empty k/v pairs because we want to delete all k/v's for this row.
		entries, err := sqlbase.EncodeSecondaryIndex(
			&systemTable, &systemTable.Indexes[i], colMap, []tree.Datum{indexKey[0], indexKey[1]}, false /* includeEmpty */)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			key = append(key, e.Key)
		}
	}
	return key, nil
}

// GetNameSpaceByParentID gets NameSpace by dbID and schemaID.
func GetNameSpaceByParentID(
	ctx context.Context, txn *kv.Txn, dbID, schemaID sqlbase.ID,
) ([]sqlbase.Namespace, error) {
	NameKey, _ := sqlbase.MakeKWDBMetadataKeyInt(sqlbase.NamespaceTable, []uint64{uint64(dbID), uint64(schemaID)})
	rows, err := sqlbase.GetKWDBMetadataRows(ctx, txn, NameKey, sqlbase.NamespaceTable)
	if err != nil {
		if IsObjectCannotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	var res []sqlbase.Namespace
	for i := range rows {
		res = append(res, makeNameSpaceByRow(rows[i]))
	}
	return res, nil
}

func makeHashRoutingByRow(row tree.Datums) (*api.KWDBHashRouting, error) {
	// system.kwdb_hash_routing
	entityRangeGroup := api.EntityRangeGroup{}
	err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(row[2])), &entityRangeGroup)
	if err != nil {
		return nil, err
	}

	hashRouting := api.KWDBHashRouting{
		EntityRangeGroupId: api.EntityRangeGroupID(tree.MustBeDInt(row[0])),
		TableID:            uint32(tree.MustBeDInt(row[1])),
		EntityRangeGroup:   entityRangeGroup,
		TsPartitionSize:    int32(tree.MustBeDInt(row[3])),
	}
	return &hashRouting, err
}

func makeNameSpaceByRow(row tree.Datums) sqlbase.Namespace {
	// from system.namespace
	return sqlbase.Namespace{
		ParentID:       uint64(tree.MustBeDInt(row[0])),
		ParentSchemaID: uint64(tree.MustBeDInt(row[1])),
		Name:           string(tree.MustBeDString(row[2])),
		ID:             uint64(tree.MustBeDInt(row[3])),
	}
}

// IsObjectCannotFoundError checks if error is object cannot found.
func IsObjectCannotFoundError(err error) bool {
	if strings.Contains(err.Error(), "object cannot found") {
		return true
	}
	return false
}
