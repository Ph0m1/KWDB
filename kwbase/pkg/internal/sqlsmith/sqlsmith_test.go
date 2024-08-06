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

package sqlsmith

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

var (
	flagExec     = flag.Bool("ex", false, "execute (instead of just parse) generated statements")
	flagNum      = flag.Int("num", 100, "number of statements to generate")
	flagSetup    = flag.String("setup", "", "setup for TestGenerateParse, empty for random")
	flagSetting  = flag.String("setting", "", "setting for TestGenerateParse, empty for random")
	flagCheckVec = flag.Bool("check-vec", false, "fail if a generated statement cannot be vectorized")
)

// TestSetups verifies that all setups generate executable SQL.
func TestSetups(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, setup := range Setups {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			rnd, _ := randutil.NewPseudoRand()

			sql := setup(rnd)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Log(sql)
				t.Fatal(err)
			}
		})
	}
}

// TestGenerateParse verifies that statements produced by Generate can be
// parsed. This is useful because since we make AST nodes directly we can
// sometimes put them into bad states that the parser would never do.
func TestGenerateParse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rnd, seed := randutil.NewPseudoRand()
	t.Log("seed:", seed)

	db := sqlutils.MakeSQLRunner(sqlDB)

	setupName := *flagSetup
	if setupName == "" {
		setupName = RandSetup(rnd)
	}
	setup, ok := Setups[setupName]
	if !ok {
		t.Fatalf("unknown setup %s", setupName)
	}
	t.Log("setup:", setupName)
	settingName := *flagSetting
	if settingName == "" {
		settingName = RandSetting(rnd)
	}
	setting, ok := Settings[settingName]
	if !ok {
		t.Fatalf("unknown setting %s", settingName)
	}
	settings := setting(rnd)
	t.Log("setting:", settingName, settings.Options)
	setupSQL := setup(rnd)
	t.Log(setupSQL)
	db.Exec(t, setupSQL)

	smither, err := NewSmither(sqlDB, rnd, settings.Options...)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	seen := map[string]bool{}
	for i := 0; i < *flagNum; i++ {
		stmt := smither.Generate()
		if err != nil {
			t.Fatalf("%v: %v", stmt, err)
		}
		parsed, err := parser.ParseOne(stmt)
		if err != nil {
			t.Fatalf("%v: %v", stmt, err)
		}
		stmt = prettyCfg.Pretty(parsed.AST)
		fmt.Print("STMT: ", i, "\n", stmt, ";\n\n")
		if *flagCheckVec {
			if _, err := sqlDB.Exec(fmt.Sprintf("EXPLAIN (vec) %s", stmt)); err != nil {
				es := err.Error()
				ok := false
				// It is hard to make queries that can always
				// be vectorized. Hard code a list of error
				// messages we are ok with.
				for _, s := range []string{
					// If the optimizer removes stuff due
					// to something like a `WHERE false`,
					// vec will fail with an error message
					// like this. This is hard to fix
					// because things like `WHERE true AND
					// false` similarly remove rows but are
					// harder to detect.
					"num_rows:0",
					"unsorted distinct",
				} {
					if strings.Contains(es, s) {
						ok = true
						break
					}
				}
				if !ok {
					t.Fatal(err)
				}
			}
		}
		if *flagExec {
			db.Exec(t, `SET statement_timeout = '9s'`)
			if _, err := sqlDB.Exec(stmt); err != nil {
				es := err.Error()
				if !seen[es] {
					seen[es] = true
					fmt.Printf("ERR (%d): %v\n", i, err)
				}
			}
		}
	}
}
