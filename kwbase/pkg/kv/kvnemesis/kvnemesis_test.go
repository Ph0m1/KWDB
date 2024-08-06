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

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestKVNemesisSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("rangeFeed sends intents instead of key/value.")

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	sqlutils.MakeSQLRunner(sqlDB).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = 1, 1
	rng, _ := randutil.NewPseudoRand()
	ct := sqlClosedTimestampTargetInterval{sqlDBs: []*gosql.DB{sqlDB}}
	failures, err := RunNemesis(ctx, rng, ct, config, db)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

func TestKVNemesisMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("rangeFeed sends intents instead of key/value.")

	// 4 nodes so we have somewhere to move 3x replicated ranges to.
	const numNodes = 4
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	dbs, sqlDBs := make([]*kv.DB, numNodes), make([]*gosql.DB, numNodes)
	for i := 0; i < numNodes; i++ {
		dbs[i] = tc.Server(i).DB()
		sqlDBs[i] = tc.ServerConn(i)
	}
	sqlutils.MakeSQLRunner(sqlDBs[0]).Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	config := NewDefaultConfig()
	config.NumNodes, config.NumReplicas = numNodes, 3
	// kvnemesis found a rare bug with closed timestamps when merges happen
	// on a multinode cluster. Disable the combo for now to keep the test
	// from flaking. See #44878.
	config.Ops.Merge = MergeConfig{}
	rng, _ := randutil.NewPseudoRand()
	ct := sqlClosedTimestampTargetInterval{sqlDBs: sqlDBs}
	failures, err := RunNemesis(ctx, rng, ct, config, dbs...)
	require.NoError(t, err, `%+v`, err)

	for _, failure := range failures {
		t.Errorf("failure:\n%+v", failure)
	}
}

type sqlClosedTimestampTargetInterval struct {
	sqlDBs []*gosql.DB
}

func (x sqlClosedTimestampTargetInterval) Set(ctx context.Context, d time.Duration) error {
	var err error
	for i, sqlDB := range x.sqlDBs {
		q := fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '%s'`, d)
		if _, err = sqlDB.Exec(q); err == nil {
			return nil
		}
		log.Infof(ctx, "node %d could not set target duration: %+v", i, err)
	}
	log.Infof(ctx, "all nodes could not set target duration: %+v", err)
	return err
}

func (x sqlClosedTimestampTargetInterval) ResetToDefault(ctx context.Context) error {
	var err error
	for i, sqlDB := range x.sqlDBs {
		q := fmt.Sprintf(`SET CLUSTER SETTING kv.closed_timestamp.target_duration TO DEFAULT`)
		if _, err = sqlDB.Exec(q); err == nil {
			return nil
		}
		log.Infof(ctx, "node %d could not reset target duration: %+v", i, err)
	}
	log.Infof(ctx, "all nodes could not reset target duration: %+v", err)
	return err
}
