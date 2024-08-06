// Copyright 2016 The Cockroach Authors.
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

package rowexec_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/backfill"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sqlmigrations"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestWriteResumeSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()

	server, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Disable all schema change execution.
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				SchemaChangeJobNoOp: func() bool {
					return true
				},
			},
			// Disable backfill migrations, we still need the jobs table migration.
			SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
				DisableBackfillMigrations: true,
			},
		},
	})
	defer server.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
	CREATE DATABASE t;
	CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	CREATE UNIQUE INDEX vidx ON t.test (v);
	`); err != nil {
		t.Fatal(err)
	}

	resumeSpans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")},
		{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")},
		{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")},
		{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")},
		{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")},
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("r")},
	}

	registry := server.JobRegistry().(*jobs.Registry)
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	if err := kvDB.Put(
		ctx,
		sqlbase.MakeDescMetadataKey(tableDesc.ID),
		sqlbase.WrapDescriptor(tableDesc),
	); err != nil {
		t.Fatal(err)
	}

	mutationID := tableDesc.Mutations[0].MutationID
	var jobID int64

	if len(tableDesc.MutationJobs) > 0 {
		for _, job := range tableDesc.MutationJobs {
			if job.MutationID == mutationID {
				jobID = job.JobID
				break
			}
		}
	}

	details := jobspb.SchemaChangeDetails{ResumeSpanList: []jobspb.ResumeSpanList{
		{ResumeSpans: resumeSpans}}}

	job, err := registry.LoadJob(ctx, jobID)

	if err != nil {
		t.Fatal(errors.Wrapf(err, "can't find job %d", jobID))
	}

	err = job.SetDetails(ctx, details)
	if err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		orig   roachpb.Span
		resume roachpb.Span
	}{
		// Work performed in the middle of a span.
		{orig: roachpb.Span{Key: roachpb.Key("a1"), EndKey: roachpb.Key("a3")},
			resume: roachpb.Span{Key: roachpb.Key("a2"), EndKey: roachpb.Key("a3")}},
		// Work completed in the middle of a span.
		{orig: roachpb.Span{Key: roachpb.Key("c1"), EndKey: roachpb.Key("c2")},
			resume: roachpb.Span{}},
		// Work performed in the right of a span.
		{orig: roachpb.Span{Key: roachpb.Key("e1"), EndKey: roachpb.Key("f")},
			resume: roachpb.Span{Key: roachpb.Key("e2"), EndKey: roachpb.Key("f")}},
		// Work completed in the right of a span.
		{orig: roachpb.Span{Key: roachpb.Key("g1"), EndKey: roachpb.Key("h")},
			resume: roachpb.Span{}},
		// Work performed in the left of a span.
		{orig: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("i2")},
			resume: roachpb.Span{Key: roachpb.Key("i1"), EndKey: roachpb.Key("i2")}},
		// Work completed in the left of a span.
		{orig: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("k2")},
			resume: roachpb.Span{}},
		// Work performed on a span.
		{orig: roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")},
			resume: roachpb.Span{Key: roachpb.Key("m1"), EndKey: roachpb.Key("n")}},
		// Work completed on a span.
		{orig: roachpb.Span{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")},
			resume: roachpb.Span{}},
	}
	for _, test := range testData {
		finished := test.orig
		if test.resume.Key != nil {
			finished.EndKey = test.resume.Key
		}
		if err := rowexec.WriteResumeSpan(
			ctx, kvDB, tableDesc.ID, mutationID, backfill.IndexMutationFilter, roachpb.Spans{finished}, registry,
		); err != nil {
			t.Error(err)
		}
	}

	expected := []roachpb.Span{
		// Work performed in the middle of a span.
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("a1")},
		{Key: roachpb.Key("a2"), EndKey: roachpb.Key("b")},
		// Work completed in the middle of a span.
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("c1")},
		{Key: roachpb.Key("c2"), EndKey: roachpb.Key("d")},
		// Work performed in the right of a span.
		{Key: roachpb.Key("e"), EndKey: roachpb.Key("e1")},
		{Key: roachpb.Key("e2"), EndKey: roachpb.Key("f")},
		// Work completed in the right of a span.
		{Key: roachpb.Key("g"), EndKey: roachpb.Key("g1")},
		// Work performed in the left of a span.
		{Key: roachpb.Key("i1"), EndKey: roachpb.Key("j")},
		// Work completed in the left of a span.
		{Key: roachpb.Key("k2"), EndKey: roachpb.Key("l")},
		// Work performed on a span.
		{Key: roachpb.Key("m1"), EndKey: roachpb.Key("n")},
		// Work completed on a span; ["o", "p"] complete.
		{Key: roachpb.Key("q"), EndKey: roachpb.Key("r")},
	}

	var got []roachpb.Span
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		got, _, _, err = rowexec.GetResumeSpans(
			ctx, registry, txn, tableDesc.ID, mutationID, backfill.IndexMutationFilter)
		return err
	}); err != nil {
		t.Error(err)
	}
	if len(expected) != len(got) {
		t.Fatalf("expected = %+v\n got = %+v", expected, got)
	}
	for i, e := range expected {
		if !e.EqualValue(got[i]) {
			t.Fatalf("expected = %+v, got = %+v", e, got[i])
		}
	}
}
