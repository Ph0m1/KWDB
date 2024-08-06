// Copyright 2020 The Cockroach Authors.
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

// Package schema is to avoid a circular dependency on pkg/sql/row and pkg/sql.
// It can be removed once schemas can be resolved to a protobuf.
package schema

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ResolveNameByID resolves a schema's name based on db and schema id.
// TODO(sqlexec): this should return the descriptor instead if given an ID.
// Instead, we have to rely on a scan of the kv table.
// TODO(sqlexec): this should probably be cached.
func ResolveNameByID(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, schemaID sqlbase.ID,
) (string, error) {
	// Fast-path for public schema, to avoid hot lookups.
	if schemaID == keys.PublicSchemaID {
		return string(tree.PublicSchemaName), nil
	}
	schemas, err := GetForDatabase(ctx, txn, dbID)
	if err != nil {
		return "", err
	}
	if schema, ok := schemas[schemaID]; ok {
		return schema, nil
	}
	return "", errors.Newf("unable to resolve schema id %d for db %d", schemaID, dbID)
}

// GetForDatabase looks up and returns all available
// schema ids to names for a given database.
func GetForDatabase(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID,
) (map[sqlbase.ID]string, error) {
	log.Eventf(ctx, "fetching all schema descriptor IDs for %d", dbID)

	nameKey := sqlbase.NewSchemaKey(dbID, "" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /* maxRows */)
	if err != nil {
		return nil, err
	}

	// Always add public schema ID.
	// TODO(solon): This can be removed in 20.2, when this is always written.
	// In 20.1, in a migrating state, it may be not included yet.
	ret := make(map[sqlbase.ID]string, len(kvs)+1)
	ret[sqlbase.ID(keys.PublicSchemaID)] = tree.PublicSchema

	for _, kv := range kvs {
		id := sqlbase.ID(kv.ValueInt())
		if _, ok := ret[id]; ok {
			continue
		}
		_, _, name, err := sqlbase.DecodeNameMetadataKey(kv.Key)
		if err != nil {
			return nil, err
		}
		ret[id] = name
	}
	return ret, nil
}
