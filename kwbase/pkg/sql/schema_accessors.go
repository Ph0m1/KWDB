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
)

// This file provides high-level interfaces to abstract access to the
// SQL schema (the descriptors).
//
// The following interfaces are defined:
// - DatabaseAccessor, which provides access to database descriptors.
// - DatabaseLister, which provides a service to list the contents of a database.
// - ObjectAccessor, which provides access to individual object descriptors.
//
// A common interface SchemaAccessor is provided for convenience.
//
// See physical_schema_accessors.go and logical_schema_accessors.go for
// reference implementations of these interfaces.

type (
	// ObjectName is provided for convenience and to make the interface
	// definitions below more intuitive.
	ObjectName = tree.TableName
	// DatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	DatabaseDescriptor = sqlbase.DatabaseDescriptor
	// UncachedDatabaseDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	UncachedDatabaseDescriptor = sqlbase.DatabaseDescriptor
	// MutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	MutableTableDescriptor = sqlbase.MutableTableDescriptor
	// ImmutableTableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ImmutableTableDescriptor = sqlbase.ImmutableTableDescriptor
	// TableDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	TableDescriptor = sqlbase.TableDescriptor
	// ViewDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	ViewDescriptor = sqlbase.TableDescriptor
	// SequenceDescriptor is provided for convenience and to make the
	// interface definitions below more intuitive.
	SequenceDescriptor = sqlbase.TableDescriptor
	// TableNames is provided for convenience and to make the interface
	// definitions below more intuitive.
	TableNames = tree.TableNames
)

// ObjectDescriptor provides table information for results from a name lookup.
type ObjectDescriptor interface {
	tree.NameResolutionResult

	// TableDesc returns the underlying table descriptor.
	TableDesc() *TableDescriptor
}

// SchemaAccessor provides access to database descriptors.
type SchemaAccessor interface {
	// GetDatabaseDesc looks up a database by name and returns its
	// descriptor. If the database is not found and required is true,
	// an error is returned; otherwise a nil reference is returned.
	GetDatabaseDesc(ctx context.Context, txn *kv.Txn, dbName string, flags tree.DatabaseLookupFlags) (*DatabaseDescriptor, error)

	// GetSchema returns true and a ResolvedSchema object if the target schema
	// exists under the target database.
	GetSchema(ctx context.Context, txn *kv.Txn, dbID sqlbase.ID, scName string) (bool, sqlbase.ResolvedSchema, error)

	// GetObjectNames returns the list of all objects in the given
	// database and schema.
	// TODO(solon): when separate schemas are supported, this
	// API should be extended to use schema descriptors.
	GetObjectNames(ctx context.Context, txn *kv.Txn, db *DatabaseDescriptor, scName string, flags tree.DatabaseListFlags) (TableNames, error)

	// GetObjectDesc looks up an object by name and returns both its
	// descriptor and that of its parent database. If the object is not
	// found and flags.required is true, an error is returned, otherwise
	// a nil reference is returned.
	GetObjectDesc(ctx context.Context, txn *kv.Txn, settings *cluster.Settings, name *ObjectName, flags tree.ObjectLookupFlags) (ObjectDescriptor, error)
}
