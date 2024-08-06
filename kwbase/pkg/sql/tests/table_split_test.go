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

package tests_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"github.com/pkg/errors"
)

func TestSplitAtTableBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 3, testClusterArgs)
	defer tc.Stopper().Stop(context.TODO())

	runner := sqlutils.MakeSQLRunner(tc.Conns[0])
	runner.Exec(t, `CREATE DATABASE test`)
	runner.Exec(t, `CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT8)`)

	const tableIDQuery = `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	var tableID uint32
	runner.QueryRow(t, tableIDQuery, "test", "t").Scan(&tableID)
	tableStartKey := keys.MakeTablePrefix(tableID)

	// Wait for new table to split.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(tableStartKey)
		if err != nil {
			t.Fatal(err)
		}
		if !desc.StartKey.Equal(tableStartKey) {
			log.Infof(context.TODO(), "waiting on split results")
			return errors.Errorf("expected range start key %s; got %s", tableStartKey, desc.StartKey)
		}
		return nil
	})
}
