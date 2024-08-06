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

package sql

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// These tests are in the sql package rather than span package
// so that we have easy access to table descriptor creation.

func TestSpanBuilderCanSplitSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	tcs := []struct {
		sql               string
		index             string
		prefixLen         int
		numNeededFamilies int
		containsNull      bool
		canSplit          bool
	}{
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:             "primary",
			prefixLen:         2,
			numNeededFamilies: 1,
			canSplit:          true,
		},
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c), FAMILY (d)",
			index:             "primary",
			prefixLen:         1,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, d INT, PRIMARY KEY (a, b), FAMILY (a, b, c, d)",
			index:             "primary",
			prefixLen:         2,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			containsNull:      true,
			canSplit:          false,
		},
		{
			sql:               "a INT, b INT, c INT, UNIQUE INDEX i (b) STORING (a, c), FAMILY (a), FAMILY (b), FAMILY (c)",
			index:             "i",
			prefixLen:         1,
			numNeededFamilies: 1,
			containsNull:      false,
			canSplit:          true,
		},
	}
	if _, err := sqlDB.Exec("CREATE DATABASE t"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			if _, err := sqlDB.Exec("DROP TABLE IF EXISTS t.t"); err != nil {
				t.Fatal(err)
			}
			sql := fmt.Sprintf("CREATE TABLE t.t (%s)", tc.sql)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Fatal(err)
			}
			desc := sqlbase.GetTableDescriptor(kvDB, "t", "t")
			idx, _, err := desc.FindIndexByName(tc.index)
			if err != nil {
				t.Fatal(err)
			}
			builder := span.MakeBuilder(desc, idx)
			if res := builder.CanSplitSpanIntoSeparateFamilies(
				tc.numNeededFamilies, tc.prefixLen, tc.containsNull); res != tc.canSplit {
				t.Errorf("expected result to be %v, but found %v", tc.canSplit, res)
			}
		})
	}
}
