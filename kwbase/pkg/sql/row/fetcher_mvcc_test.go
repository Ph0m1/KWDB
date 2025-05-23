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

package row_test

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func slurpUserDataKVs(t testing.TB, e storage.Engine) []roachpb.KeyValue {
	t.Helper()

	// Scan meta keys directly from engine. We put this in a retry loop
	// because the application of all of a transactions committed writes
	// is not always synchronous with it committing.
	var kvs []roachpb.KeyValue
	testutils.SucceedsSoon(t, func() error {
		kvs = nil
		it := e.NewIterator(storage.IterOptions{UpperBound: roachpb.KeyMax})
		defer it.Close()
		for it.SeekGE(storage.MVCCKey{Key: keys.UserTableDataMin}); ; it.NextKey() {
			ok, err := it.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if !it.UnsafeKey().IsValue() {
				return errors.Errorf("found intent key %v", it.UnsafeKey())
			}
			kvs = append(kvs, roachpb.KeyValue{
				Key:   it.Key().Key,
				Value: roachpb.Value{RawBytes: it.Value(), Timestamp: it.UnsafeKey().Timestamp},
			})
		}
		return nil
	})
	return kvs
}

func TestRowFetcherMVCCMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	store, _ := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `USE d`)
	sqlDB.Exec(t, `CREATE TABLE parent (
		a STRING PRIMARY KEY, b STRING, c STRING, d STRING,
		FAMILY (a, b, c), FAMILY (d)
	)`)
	sqlDB.Exec(t, `CREATE TABLE child (
		e STRING, f STRING, PRIMARY KEY (e, f)
	) INTERLEAVE IN PARENT parent (e)`)

	parentDesc := sqlbase.GetImmutableTableDescriptor(kvDB, `d`, `parent`)
	childDesc := sqlbase.GetImmutableTableDescriptor(kvDB, `d`, `child`)
	var args []row.FetcherTableArgs
	for _, desc := range []*sqlbase.ImmutableTableDescriptor{parentDesc, childDesc} {
		colIdxMap := make(map[sqlbase.ColumnID]int)
		var valNeededForCol util.FastIntSet
		for colIdx := range desc.Columns {
			id := desc.Columns[colIdx].ID
			colIdxMap[id] = colIdx
			valNeededForCol.Add(colIdx)
		}
		args = append(args, row.FetcherTableArgs{
			Spans:            desc.AllIndexSpans(),
			Desc:             desc,
			Index:            &desc.PrimaryIndex,
			ColIdxMap:        colIdxMap,
			IsSecondaryIndex: false,
			Cols:             desc.Columns,
			ValNeededForCol:  valNeededForCol,
		})
	}
	var rf row.Fetcher
	if err := rf.Init(
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* returnRangeInfo */
		true,  /* isCheck */
		&sqlbase.DatumAlloc{},
		args...,
	); err != nil {
		t.Fatal(err)
	}
	type rowWithMVCCMetadata struct {
		PrimaryKey      []string
		RowIsDeleted    bool
		RowLastModified string
	}
	kvsToRows := func(kvs []roachpb.KeyValue) []rowWithMVCCMetadata {
		t.Helper()
		for _, kv := range kvs {
			log.Info(ctx, kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
		}

		if err := rf.StartScanFrom(ctx, &row.SpanKVFetcher{KVs: kvs}); err != nil {
			t.Fatal(err)
		}
		var rows []rowWithMVCCMetadata
		for {
			datums, _, _, err := rf.NextRowDecoded(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if datums == nil {
				break
			}
			row := rowWithMVCCMetadata{
				RowIsDeleted:    rf.RowIsDeleted(),
				RowLastModified: tree.TimestampToDecimal(rf.RowLastModified()).String(),
			}
			for _, datum := range datums {
				if datum == tree.DNull {
					row.PrimaryKey = append(row.PrimaryKey, `NULL`)
				} else {
					row.PrimaryKey = append(row.PrimaryKey, string(*datum.(*tree.DString)))
				}
			}
			rows = append(rows, row)
		}
		return rows
	}

	var ts1 string
	sqlDB.QueryRow(t, `BEGIN;
		INSERT INTO parent VALUES ('1', 'a', 'a', 'a'), ('2', 'b', 'b', 'b');
		INSERT INTO child VALUES ('1', '10'), ('2', '20');
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts1)

	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.Engine())), []rowWithMVCCMetadata{
		{[]string{`1`, `a`, `a`, `a`}, false, ts1},
		{[]string{`1`, `10`}, false, ts1},
		{[]string{`2`, `b`, `b`, `b`}, false, ts1},
		{[]string{`2`, `20`}, false, ts1},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}

	var ts2 string
	sqlDB.QueryRow(t, `BEGIN;
		UPDATE parent SET b = NULL, c = NULL, d = NULL WHERE a = '1';
		UPDATE parent SET d = NULL WHERE a = '2';
		UPDATE child SET f = '21' WHERE e = '2';
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts2)

	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.Engine())), []rowWithMVCCMetadata{
		{[]string{`1`, `NULL`, `NULL`, `NULL`}, false, ts2},
		{[]string{`1`, `10`}, false, ts1},
		{[]string{`2`, `b`, `b`, `NULL`}, false, ts2},
		{[]string{`2`, `20`}, true, ts2},
		{[]string{`2`, `21`}, false, ts2},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}

	var ts3 string
	sqlDB.QueryRow(t, `BEGIN;
		DELETE FROM parent WHERE a = '1';
		DELETE FROM child WHERE e = '2';
		SELECT cluster_logical_timestamp();
	END;`).Scan(&ts3)
	if actual, expected := kvsToRows(slurpUserDataKVs(t, store.Engine())), []rowWithMVCCMetadata{
		{[]string{`1`, `NULL`, `NULL`, `NULL`}, true, ts3},
		{[]string{`1`, `10`}, false, ts1},
		{[]string{`2`, `b`, `b`, `NULL`}, false, ts2},
		{[]string{`2`, `20`}, true, ts2}, // ignore me: artifact of how the test is written
		{[]string{`2`, `21`}, true, ts3},
	}; !reflect.DeepEqual(expected, actual) {
		t.Errorf(`expected %v got %v`, expected, actual)
	}
}
