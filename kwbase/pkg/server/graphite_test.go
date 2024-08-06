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

package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestGraphite tests that a server pushes metrics data to Graphite endpoint,
// if configured. In addition, it verifies that things don't fall apart when
// the endpoint goes away.
func TestGraphite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ctx := context.Background()

	const setQ = `SET CLUSTER SETTING "%s" = "%s"`
	const interval = 3 * time.Millisecond
	db := sqlutils.MakeSQLRunner(rawDB)
	db.Exec(t, fmt.Sprintf(setQ, graphiteIntervalKey, interval))

	listen := func() {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal("failed to open port", err)
		}
		p := lis.Addr().String()
		log.Infof(ctx, "Open port %s and listening", p)

		defer func() {
			log.Infof(ctx, "Close port %s", p)
			if err := lis.Close(); err != nil {
				t.Fatal("failed to close port", err)
			}
		}()

		db.Exec(t, fmt.Sprintf(setQ, "external.graphite.endpoint", p))
		if _, e := lis.Accept(); e != nil {
			t.Fatal("failed to receive connection", e)
		} else {
			log.Info(ctx, "received connection")
		}
	}

	listen()
	log.Info(ctx, "Make sure things don't fall apart when endpoint goes away.")
	time.Sleep(5 * interval)
	listen()
}
