// Copyright 2017 The Cockroach Authors.
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

package bank

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/workload/workloadsql"
)

func TestBank(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		rows           int
		ranges         int
		expectedRanges int
	}{
		{10, 0, 1}, // we always have at least one range
		{10, 1, 1},
		{10, 9, 9},
		{10, 10, 10},
		{10, 100, 10}, // don't make more ranges than rows
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE test`)

	for _, test := range tests {
		t.Run(fmt.Sprintf("rows=%d/ranges=%d", test.rows, test.ranges), func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP TABLE IF EXISTS bank`)

			bank := FromConfig(test.rows, test.rows, defaultPayloadBytes, test.ranges)
			bankTable := bank.Tables()[0]
			sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s %s`, bankTable.Name, bankTable.Schema))

			if err := workloadsql.Split(ctx, db, bankTable, 1 /* concurrency */); err != nil {
				t.Fatalf("%+v", err)
			}

			var rangeCount int
			sqlDB.QueryRow(t,
				fmt.Sprintf(`SELECT count(*) FROM [SHOW RANGES FROM TABLE %s]`, bankTable.Name),
			).Scan(&rangeCount)
			if rangeCount != test.expectedRanges {
				t.Errorf("got %d ranges expected %d", rangeCount, test.expectedRanges)
			}
		})
	}
}
