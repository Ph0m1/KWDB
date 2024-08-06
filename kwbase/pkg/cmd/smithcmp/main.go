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

// smithcmp is a tool to execute random queries on a database. A TOML
// file provides configuration for which databases to connect to. If there
// is more than one, only non-mutating statements are generated, and the
// output is compared, exiting if there is a difference. If there is only
// one database, mutating and non-mutating statements are generated. A
// flag in the TOML controls whether Postgres-compatible output is generated.
//
// Explicit SQL statements can be specified (skipping sqlsmith generation)
// using the top-level SQL array. Placeholders (`$1`, etc.) are
// supported. Random datums of the correct type will be filled in.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/smithcmp/cmpconn"
	"gitee.com/kwbasedb/kwbase/pkg/internal/sqlsmith"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/BurntSushi/toml"
	"github.com/lib/pq/oid"
)

func usage() {
	const use = `Usage of %s:
	%[1]s config.toml
`

	fmt.Printf(use, os.Args[0])
	os.Exit(1)
}

type options struct {
	Postgres    bool
	InitSQL     string
	Smither     string
	Seed        int64
	TimeoutSecs int
	SQL         []string

	Databases map[string]struct {
		Addr           string
		InitSQL        string
		AllowMutations bool
	}
}

var sqlMutators = []sqlbase.Mutator{mutations.ColumnFamilyMutator}

func enableMutations(shouldEnable bool, mutations []sqlbase.Mutator) []sqlbase.Mutator {
	if shouldEnable {
		return mutations
	}
	return nil
}

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		usage()
	}

	tomlData, err := ioutil.ReadFile(args[0])
	if err != nil {
		log.Fatal(err)
	}

	var opts options
	if err := toml.Unmarshal(tomlData, &opts); err != nil {
		log.Fatal(err)
	}
	timeout := time.Duration(opts.TimeoutSecs) * time.Second
	if timeout <= 0 {
		timeout = time.Minute
	}

	rng := rand.New(rand.NewSource(opts.Seed))
	conns := map[string]*cmpconn.Conn{}
	for name, db := range opts.Databases {
		var err error
		mutators := enableMutations(opts.Databases[name].AllowMutations, sqlMutators)
		if opts.Postgres {
			mutators = append(mutators, mutations.PostgresMutator)
		}
		conns[name], err = cmpconn.NewConn(
			db.Addr, rng, mutators, db.InitSQL, opts.InitSQL)
		if err != nil {
			log.Fatalf("%s (%s): %+v", name, db.Addr, err)
		}
	}
	compare := len(conns) > 1

	if opts.Seed < 0 {
		opts.Seed = timeutil.Now().UnixNano()
		fmt.Println("seed:", opts.Seed)
	}
	smithOpts := []sqlsmith.SmitherOption{
		sqlsmith.AvoidConsts(),
	}
	if opts.Postgres {
		smithOpts = append(smithOpts, sqlsmith.PostgresMode())
	} else if compare {
		smithOpts = append(smithOpts,
			sqlsmith.CompareMode(),
			sqlsmith.DisableKWDBFns(),
		)
	}
	if _, ok := conns[opts.Smither]; !ok {
		log.Fatalf("Smither option not present in databases: %s", opts.Smither)
	}
	var smither *sqlsmith.Smither
	var stmts []statement
	if len(opts.SQL) == 0 {
		smither, err = sqlsmith.NewSmither(conns[opts.Smither].DB, rng, smithOpts...)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		stmts = make([]statement, len(opts.SQL))
		for i, stmt := range opts.SQL {
			ps, err := conns[opts.Smither].PGX.Prepare("", stmt)
			if err != nil {
				log.Fatalf("bad SQL statement on %s: %v\nSQL:\n%s", opts.Smither, stmt, err)
			}
			var placeholders []*types.T
			for _, param := range ps.ParameterOIDs {
				typ, ok := types.OidToType[oid.Oid(param)]
				if !ok {
					log.Fatalf("unknown oid: %v", param)
				}
				placeholders = append(placeholders, typ)
			}
			stmts[i] = statement{
				stmt:         stmt,
				placeholders: placeholders,
			}
		}
	}

	var prep, exec string
	ctx := context.Background()
	for i := 0; true; i++ {
		fmt.Printf("stmt: %d\n", i)
		if smither != nil {
			exec = smither.Generate()
		} else {
			randStatement := stmts[rng.Intn(len(stmts))]
			name := fmt.Sprintf("s%d", i)
			prep = fmt.Sprintf("PREPARE %s AS\n%s;", name, randStatement.stmt)
			var sb strings.Builder
			fmt.Fprintf(&sb, "EXECUTE %s", name)
			for i, typ := range randStatement.placeholders {
				if i > 0 {
					sb.WriteString(", ")
				} else {
					sb.WriteString(" (")
				}
				d := sqlbase.RandDatum(rng, typ, true)
				fmt.Println(i, typ, d, tree.Serialize(d))
				sb.WriteString(tree.Serialize(d))
			}
			if len(randStatement.placeholders) > 0 {
				fmt.Fprintf(&sb, ")")
			}
			fmt.Fprintf(&sb, ";")
			exec = sb.String()
			fmt.Println(exec)
		}
		if compare {
			if err := cmpconn.CompareConns(ctx, timeout, conns, prep, exec); err != nil {
				fmt.Printf("prep:\n%s;\nexec:\n%s;\nERR: %s\n\n", prep, exec, err)
				os.Exit(1)
			}
		} else {
			for _, conn := range conns {
				if err := conn.Exec(ctx, prep+exec); err != nil {
					fmt.Println(err)
				}
			}
		}

		// Make sure the servers are alive.
		for name, conn := range conns {
			start := timeutil.Now()
			fmt.Printf("pinging %s...", name)
			if err := conn.Ping(); err != nil {
				fmt.Printf("\n%s: ping failure: %v\nprevious SQL:\n%s;\n%s;\n", name, err, prep, exec)
				// Try to reconnect.
				db := opts.Databases[name]
				newConn, err := cmpconn.NewConn(db.Addr, rng, enableMutations(db.AllowMutations, sqlMutators), db.InitSQL, opts.InitSQL)
				if err != nil {
					log.Fatalf("tried to reconnect: %v\n", err)
				}
				conns[name] = newConn
			}
			fmt.Printf(" %s\n", timeutil.Since(start))
		}
	}
}

type statement struct {
	stmt         string
	placeholders []*types.T
}
