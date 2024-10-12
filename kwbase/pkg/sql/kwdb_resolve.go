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

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

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
