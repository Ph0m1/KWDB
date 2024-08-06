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

package sql

import (
	"context"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// The goal of this test is to validate a case that could happen in a
// multi-version cluster setting.
// The foreign key representation has been updated in many ways,
// and it is possible that a pre 19.2 table descriptor foreign key
// representation could lead to descriptor information loss when
// performing index drops in some cases. This test constructs an
// old version descriptor and ensures that everything is OK.
func TestOldForeignKeyRepresentationGetsUpgraded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.t1 (x INT);
CREATE TABLE t.t2 (x INT, UNIQUE INDEX (x));
ALTER TABLE t.t1 ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES t.t2 (x);
CREATE INDEX ON t.t1 (x);
`); err != nil {
		t.Fatal(err)
	}
	desc := sqlbase.GetTableDescriptor(kvDB, "t", "t1")
	desc = sqlbase.GetTableDescriptor(kvDB, "t", "t2")
	// Remember the old foreign keys.
	oldInboundFKs := append([]sqlbase.ForeignKeyConstraint{}, desc.InboundFKs...)
	// downgradeForeignKey downgrades a table descriptor's foreign key representation
	// to the pre-19.2 table descriptor format where foreign key information
	// is stored on the index.
	downgradeForeignKey := func(tbl *sqlbase.TableDescriptor) *sqlbase.TableDescriptor {
		// Downgrade the outbound foreign keys.
		for i := range tbl.OutboundFKs {
			fk := &tbl.OutboundFKs[i]
			idx, err := sqlbase.FindFKOriginIndex(tbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			referencedTbl, err := sqlbase.GetTableDescFromID(ctx, kvDB, fk.ReferencedTableID)
			if err != nil {
				t.Fatal(err)
			}
			refIdx, err := sqlbase.FindFKReferencedIndex(referencedTbl, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			idx.ForeignKey = sqlbase.ForeignKeyReference{
				Name:            fk.Name,
				Table:           fk.ReferencedTableID,
				Index:           refIdx.ID,
				Validity:        fk.Validity,
				SharedPrefixLen: int32(len(fk.OriginColumnIDs)),
				OnDelete:        fk.OnDelete,
				OnUpdate:        fk.OnUpdate,
				Match:           fk.Match,
			}
		}
		tbl.OutboundFKs = nil
		// Downgrade the inbound foreign keys.
		for i := range tbl.InboundFKs {
			fk := &tbl.InboundFKs[i]
			idx, err := sqlbase.FindFKReferencedIndex(desc, fk.ReferencedColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			originTbl, err := sqlbase.GetTableDescFromID(ctx, kvDB, fk.OriginTableID)
			if err != nil {
				t.Fatal(err)
			}
			originIdx, err := sqlbase.FindFKOriginIndex(originTbl, fk.OriginColumnIDs)
			if err != nil {
				t.Fatal(err)
			}
			// Back references only contain the table and index IDs in old format versions.
			fkRef := sqlbase.ForeignKeyReference{
				Table: fk.OriginTableID,
				Index: originIdx.ID,
			}
			idx.ReferencedBy = append(idx.ReferencedBy, fkRef)
		}
		tbl.InboundFKs = nil
		return tbl
	}
	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		newDesc := downgradeForeignKey(desc)
		if err := writeDescToBatch(ctx, false, s.ClusterSettings(), b, desc.ID, newDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Run a DROP INDEX statement and ensure that the downgraded descriptor gets upgraded successfully.
	if _, err := sqlDB.Exec(`DROP INDEX t.t1@t1_auto_index_fk1`); err != nil {
		t.Fatal(err)
	}
	desc = sqlbase.GetTableDescriptor(kvDB, "t", "t2")
	// Remove the validity field on all the descriptors for comparison, since
	// foreign keys on the referenced side's validity is not always updated correctly.
	for i := range desc.InboundFKs {
		desc.InboundFKs[i].Validity = sqlbase.ConstraintValidity_Validated
	}
	for i := range oldInboundFKs {
		oldInboundFKs[i].Validity = sqlbase.ConstraintValidity_Validated
	}
	if !reflect.DeepEqual(desc.InboundFKs, oldInboundFKs) {
		t.Error("expected fks", oldInboundFKs, "but found", desc.InboundFKs)
	}
}
