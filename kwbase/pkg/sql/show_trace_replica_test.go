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

package sql_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func TestShowTraceReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Skip("https://gitee.com/kwbasedb/kwbase/issues/34213")

	const numNodes = 4

	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.NumReplicas = proto.Int32(1)

	ctx := context.Background()
	tsArgs := func(node string) base.TestServerArgs {
		return base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride:       &zoneConfig,
					DefaultSystemZoneConfigOverride: &zoneConfig,
				},
			},
			StoreSpecs: []base.StoreSpec{{InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{node}}}},
		}
	}
	tcArgs := base.TestClusterArgs{ServerArgsPerNode: map[int]base.TestServerArgs{
		0: tsArgs(`n1`),
		1: tsArgs(`n2`),
		2: tsArgs(`n3`),
		3: tsArgs(`n4`),
	}}
	tc := testcluster.StartTestCluster(t, numNodes, tcArgs)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `ALTER RANGE "default" CONFIGURE ZONE USING constraints = '[+n4]'`)
	sqlDB.Exec(t, `ALTER DATABASE system CONFIGURE ZONE USING constraints = '[+n4]'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE d.t1 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.t2 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE d.t3 (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `ALTER TABLE d.t1 CONFIGURE ZONE USING constraints = '[+n1]'`)
	sqlDB.Exec(t, `ALTER TABLE d.t2 CONFIGURE ZONE USING constraints = '[+n2]'`)
	sqlDB.Exec(t, `ALTER TABLE d.t3 CONFIGURE ZONE USING constraints = '[+n3]'`)

	tests := []struct {
		query    string
		expected [][]string
		distinct bool
	}{
		{
			// Read-only
			query:    `SELECT * FROM d.t1`,
			expected: [][]string{{`1`, `1`}},
		},
		{
			// Write-only
			query:    `UPSERT INTO d.t2 VALUES (1)`,
			expected: [][]string{{`2`, `2`}},
		},
		{
			// A write to delete the row.
			query:    `DELETE FROM d.t2`,
			expected: [][]string{{`2`, `2`}},
		},
		{
			// Admin command. We use distinct because the ALTER statement is
			// DDL and cause event log / job ranges to be touched too.
			query:    `ALTER TABLE d.t3 SCATTER`,
			expected: [][]string{{`4`, `4`}, {`3`, `3`}},
			distinct: true,
		},
	}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			testutils.SucceedsSoon(t, func() error {
				_ = sqlDB.Exec(t, fmt.Sprintf(`SET tracing = on; %s; SET tracing = off`, test.query))

				distinct := ""
				if test.distinct {
					distinct = "DISTINCT"
				}
				actual := sqlDB.QueryStr(t,
					fmt.Sprintf(`SELECT %s node_id, store_id FROM [SHOW EXPERIMENTAL_REPLICA TRACE FOR SESSION]`, distinct),
				)
				if !reflect.DeepEqual(actual, test.expected) {
					return errors.Errorf(`%s: got %v expected %v`, test.query, actual, test.expected)
				}
				return nil
			})
		})
	}
}
