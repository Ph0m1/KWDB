// Copyright 2019 The Cockroach Authors.
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

package bench

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
)

func runFKBench(
	b *testing.B,
	setup func(b *testing.B, r *sqlutils.SQLRunner, setupFKs bool),
	run func(b *testing.B, r *sqlutils.SQLRunner),
) {
	configs := []struct {
		name           string
		setupFKs       bool
		optFKOn        bool
		insertFastPath bool
	}{
		{name: "None", setupFKs: false},
		{name: "Old", setupFKs: true, optFKOn: false},
		{name: "New", setupFKs: true, optFKOn: true, insertFastPath: false},
		{name: "FastPath", setupFKs: true, optFKOn: true, insertFastPath: true},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			s, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
			defer s.Stopper().Stop(context.TODO())
			r := sqlutils.MakeSQLRunner(db)
			// Don't let auto stats interfere with the test. Stock stats are
			// sufficient to get the right plans (i.e. lookup join).
			r.Exec(b, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")
			r.Exec(b, fmt.Sprintf("SET optimizer_foreign_keys = %v", cfg.optFKOn))
			r.Exec(b, fmt.Sprintf("SET enable_insert_fast_path = %v", cfg.insertFastPath))
			setup(b, r, cfg.setupFKs)
			b.ResetTimer()
			run(b, r)
		})
	}
}

func BenchmarkFKInsert(b *testing.B) {
	const parentRows = 1000
	setup := func(b *testing.B, r *sqlutils.SQLRunner, setupFKs bool) {
		r.Exec(b, "CREATE TABLE child (k int primary key, p int)")
		r.Exec(b, "CREATE TABLE parent (p int primary key, data int)")

		if setupFKs {
			r.Exec(b, "ALTER TABLE child ADD CONSTRAINT fk FOREIGN KEY (p) REFERENCES parent(p)")
		} else {
			// Create the index on p manually so it's a more fair comparison.
			r.Exec(b, "CREATE INDEX idx ON child(p)")
		}

		r.Exec(b, fmt.Sprintf(
			"INSERT INTO parent SELECT i, i FROM generate_series(0,%d) AS g(i)", parentRows-1,
		))
	}

	b.Run("SingleRow", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			for i := 0; i < b.N; i++ {
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES (%d, %d)", i, i%parentRows))
			}
		})
	})

	const batch = 20
	b.Run("MultiRowSingleParent", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			k := 0
			for i := 0; i < b.N; i++ {
				// All rows in the batch reference the same parent value.
				parent := i % parentRows
				vals := make([]string, batch)
				for j := range vals {
					vals[j] = fmt.Sprintf("(%d, %d)", k, parent)
					k++
				}
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES %s", strings.Join(vals, ",")))
			}
		})
	})

	b.Run("MultiRowMultiParent", func(b *testing.B) {
		runFKBench(b, setup, func(b *testing.B, r *sqlutils.SQLRunner) {
			k := 0
			for i := 0; i < b.N; i++ {
				vals := make([]string, batch)
				for j := range vals {
					vals[j] = fmt.Sprintf("(%d, %d)", k, k%parentRows)
					k++
				}
				r.Exec(b, fmt.Sprintf("INSERT INTO child VALUES %s", strings.Join(vals, ",")))
			}
		})
	})
}
