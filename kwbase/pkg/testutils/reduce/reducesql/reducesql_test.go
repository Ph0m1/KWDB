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

package reducesql_test

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/reduce"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/reduce/reducesql"
	"github.com/jackc/pgx"
)

var printUnknown = flag.Bool("unknown", false, "print unknown types during walk")

func TestReduceSQL(t *testing.T) {
	// These take a bit too long to need to run every time.
	t.Skip("unnecessary")
	reducesql.LogUnknown = *printUnknown

	reduce.Walk(t, "testdata", reducesql.Pretty, isInterestingSQL, reduce.ModeInteresting, reducesql.SQLPasses)
}

func isInterestingSQL(contains string) reduce.InterestingFn {
	return func(ctx context.Context, f reduce.File) bool {
		args := base.TestServerArgs{
			Insecure: true,
		}
		server := server.TestServerFactory.New(args).(*server.TestServer)
		if err := server.Start(args); err != nil {
			panic(err)
		}
		defer server.Stopper().Stop(ctx)

		options := url.Values{}
		options.Add("sslmode", "disable")
		url := url.URL{
			Scheme:   "postgres",
			User:     url.User(security.RootUser),
			Host:     server.ServingSQLAddr(),
			RawQuery: options.Encode(),
		}

		conf, err := pgx.ParseURI(url.String())
		if err != nil {
			panic(err)
		}
		db, err := pgx.Connect(conf)
		if err != nil {
			panic(err)
		}
		_, err = db.ExecEx(ctx, string(f), nil)
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), contains)
	}
}
