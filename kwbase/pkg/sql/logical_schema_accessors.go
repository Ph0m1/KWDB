// Copyright 2018 The Cockroach Authors.
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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
)

// This file provides reference implementations of the schema accessor
// interfaces defined in schema_accessors.go.
//

// LogicalSchemaAccessor extends an existing DatabaseLister with the
// ability to list tables in a virtual schema.
type LogicalSchemaAccessor struct {
	SchemaAccessor
	vt VirtualTabler
}

var _ SchemaAccessor = &LogicalSchemaAccessor{}

// GetSchema implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetSchema(
	ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string,
) (bool, sqlbase.ResolvedSchema, error) {
	if _, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		return true, sqlbase.ResolvedSchema{Kind: sqlbase.SchemaVirtual}, nil
	}

	// Fallthrough.
	return l.SchemaAccessor.GetSchema(ctx, txn, dbID, scName)
}

// GetObjectNames implements the DatabaseLister interface.
func (l *LogicalSchemaAccessor) GetObjectNames(
	ctx context.Context,
	txn *kv.Txn,
	dbDesc *DatabaseDescriptor,
	scName string,
	flags tree.DatabaseListFlags,
) (TableNames, error) {
	if entry, ok := l.vt.getVirtualSchemaEntry(scName); ok {
		names := make(TableNames, len(entry.orderedDefNames))
		for i, name := range entry.orderedDefNames {
			names[i] = tree.MakeTableNameWithSchema(
				tree.Name(dbDesc.Name), tree.Name(entry.desc.Name), tree.Name(name))
			names[i].ExplicitCatalog = flags.ExplicitPrefix
			names[i].ExplicitSchema = flags.ExplicitPrefix
		}

		return names, nil
	}

	// Fallthrough.
	return l.SchemaAccessor.GetObjectNames(ctx, txn, dbDesc, scName, flags)
}

// GetObjectDesc implements the ObjectAccessor interface.
func (l *LogicalSchemaAccessor) GetObjectDesc(
	ctx context.Context,
	txn *kv.Txn,
	settings *cluster.Settings,
	name *ObjectName,
	flags tree.ObjectLookupFlags,
) (ObjectDescriptor, error) {
	if scEntry, ok := l.vt.getVirtualSchemaEntry(name.Schema()); ok {
		tableName := name.Table()
		if t, ok := scEntry.defs[tableName]; ok {
			if flags.RequireMutable {
				return sqlbase.NewMutableExistingTableDescriptor(*t.desc), nil
			}
			return sqlbase.NewImmutableTableDescriptor(*t.desc), nil
		}
		if _, ok := scEntry.allTableNames[tableName]; ok {
			return nil, unimplemented.Newf(name.Schema()+"."+tableName,
				"virtual schema table not implemented: %s.%s", name.Schema(), tableName)
		}

		if flags.Required {
			return nil, sqlbase.NewUndefinedRelationError(name)
		}
		return nil, nil
	}

	// Fallthrough.
	return l.SchemaAccessor.GetObjectDesc(ctx, txn, settings, name, flags)
}
