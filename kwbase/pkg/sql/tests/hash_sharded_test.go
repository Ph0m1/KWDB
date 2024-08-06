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

package tests_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// getShardColumnID fetches the id of the shard column associated with the given sharded
// index.
func getShardColumnID(
	t *testing.T, tableDesc *sqlbase.TableDescriptor, shardedIndexName string,
) sqlbase.ColumnID {
	idx, _, err := tableDesc.FindIndexByName(shardedIndexName)
	if err != nil {
		t.Fatal(err)
	}
	shardCol, _, err := tableDesc.FindColumnByName(tree.Name(idx.Sharded.Name))
	if err != nil {
		t.Fatal(err)
	}
	return shardCol.ID
}

// verifyTableDescriptorStates ensures that the given table descriptor fulfills the
// following conditions after the creation of a sharded index:
// 1. A hidden shard column was created.
// 2. A hidden check constraint was created on the aforementioned shard column.
// 3. The first column in the index set is the aforementioned shard column.
func verifyTableDescriptorState(
	t *testing.T, tableDesc *sqlbase.TableDescriptor, shardedIndexName string,
) {
	idx, _, err := tableDesc.FindIndexByName(shardedIndexName)
	if err != nil {
		t.Fatal(err)
	}

	if !idx.IsSharded() {
		t.Fatalf(`Expected index %s to be sharded`, shardedIndexName)
	}
	// Note that this method call will fail if the shard column doesn't exist
	shardColID := getShardColumnID(t, tableDesc, shardedIndexName)
	foundCheckConstraint := false
	for _, check := range tableDesc.AllActiveAndInactiveChecks() {
		usesShard, err := check.UsesColumn(tableDesc, shardColID)
		if err != nil {
			t.Fatal(err)
		}
		if usesShard && check.Hidden {
			foundCheckConstraint = true
			break
		}
	}
	if !foundCheckConstraint {
		t.Fatalf(`Could not find hidden check constraint for shard column`)
	}
	if idx.ColumnIDs[0] != shardColID {
		t.Fatalf(`Expected shard column to be the first column in the set of index columns`)
	}
}

func TestBasicHashShardedIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`USE d`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET experimental_enable_hash_sharded_indexes = true`); err != nil {
		t.Fatal(err)
	}

	t.Run("primary", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_primary (
				k INT8 PRIMARY KEY USING HASH WITH BUCKET_COUNT=5,
				v BYTES
			)
		`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE INDEX foo ON kv_primary (v)`); err != nil {
			t.Fatal(err)
		}
		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_primary`)
		verifyTableDescriptorState(t, tableDesc, "primary" /* shardedIndexName */)
		shardColID := getShardColumnID(t, tableDesc, "primary" /* shardedIndexName */)

		// Ensure that secondary indexes on table `kv` have the shard column in their
		// `ExtraColumnIDs` field so they can reconstruct the sharded primary key.
		fooDesc, _, err := tableDesc.FindIndexByName("foo")
		if err != nil {
			t.Fatal(err)
		}
		foundShardColumn := false
		for _, colID := range fooDesc.ExtraColumnIDs {
			if colID == shardColID {
				foundShardColumn = true
				break
			}
		}
		if !foundShardColumn {
			t.Fatalf(`Secondary index cannot reconstruct sharded primary key`)
		}
	})

	t.Run("secondary_in_create_table", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_secondary (
				k INT8,
				v BYTES,
				INDEX sharded_secondary (k) USING HASH WITH BUCKET_COUNT = 12
			)
		`); err != nil {
			t.Fatal(err)
		}

		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_secondary`)
		verifyTableDescriptorState(t, tableDesc, "sharded_secondary" /* shardedIndexName */)
	})

	t.Run("secondary_in_separate_ddl", func(t *testing.T) {
		if _, err := db.Exec(`
			CREATE TABLE kv_secondary2 (
				k INT,
				v BYTES
			)
		`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`CREATE INDEX sharded_secondary2 ON kv_secondary2 (k) USING HASH WITH BUCKET_COUNT = 12`); err != nil {
			t.Fatal(err)
		}
		tableDesc := sqlbase.GetTableDescriptor(kvDB, `d`, `kv_secondary2`)
		verifyTableDescriptorState(t, tableDesc, "sharded_secondary2" /* shardedIndexName */)
	})
}
