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

// Package cmpconn assits in comparing results from DB connections.
package cmpconn

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// Conn holds gosql and pgx connections.
type Conn struct {
	DB          *gosql.DB
	PGX         *pgx.Conn
	rng         *rand.Rand
	sqlMutators []sqlbase.Mutator
}

// NewConn returns a new Conn on the given uri and executes initSQL on it. The
// mutators are applied to initSQL.
func NewConn(
	uri string, rng *rand.Rand, sqlMutators []sqlbase.Mutator, initSQL ...string,
) (*Conn, error) {
	c := Conn{
		rng:         rng,
		sqlMutators: sqlMutators,
	}

	{
		connector, err := pq.NewConnector(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pq conn")
		}
		db := gosql.OpenDB(connector)
		c.DB = db
	}

	{
		config, err := pgx.ParseURI(uri)
		if err != nil {
			return nil, errors.Wrap(err, "pgx parse")
		}
		conn, err := pgx.Connect(config)
		if err != nil {
			return nil, errors.Wrap(err, "pgx conn")
		}
		c.PGX = conn
	}

	for _, s := range initSQL {
		if s == "" {
			continue
		}

		s, _ = mutations.ApplyString(rng, s, sqlMutators...)
		if _, err := c.PGX.Exec(s); err != nil {
			return nil, errors.Wrap(err, "init SQL")
		}
	}

	return &c, nil
}

// Close closes the connections.
func (c *Conn) Close() {
	_ = c.DB.Close()
	_ = c.PGX.Close()
}

// Ping pings a connection.
func (c *Conn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.PGX.Ping(ctx)
}

// Exec executes s.
func (c *Conn) Exec(ctx context.Context, s string) error {
	_, err := c.PGX.ExecEx(ctx, s, simpleProtocol)
	return errors.Wrap(err, "exec")
}

// Values executes prep and exec and returns the results of exec. Mutators
// passed in during NewConn are applied only to exec. The mutated exec string
// is returned.
func (c *Conn) Values(
	ctx context.Context, prep, exec string,
) (rows *pgx.Rows, mutated string, err error) {
	if prep != "" {
		rows, err = c.PGX.QueryEx(ctx, prep, simpleProtocol)
		if err != nil {
			return nil, "", err
		}
		rows.Close()
	}
	mutated, _ = mutations.ApplyString(c.rng, exec, c.sqlMutators...)
	rows, err = c.PGX.QueryEx(ctx, mutated, simpleProtocol)
	return rows, mutated, err
}

var simpleProtocol = &pgx.QueryExOptions{SimpleProtocol: true}

// CompareConns executes prep and exec on all connections in conns. If any
// differ, an error is returned. SQL errors are ignored.
func CompareConns(
	ctx context.Context, timeout time.Duration, conns map[string]*Conn, prep, exec string,
) (err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connRows := make(map[string]*pgx.Rows)
	connExecs := make(map[string]string)
	for name, conn := range conns {
		rows, mutated, err := conn.Values(ctx, prep, exec)
		if err != nil {
			return nil //nolint:returnerrcheck
		}
		defer rows.Close()
		connRows[name] = rows
		connExecs[name] = mutated
	}

	// Annotate our error message with the exec queries since they can be
	// mutated and differ per connection.
	defer func() {
		if err == nil {
			return
		}
		var sb strings.Builder
		prev := ""
		for name, mutated := range connExecs {
			fmt.Fprintf(&sb, "\n%s:", name)
			if prev == mutated {
				sb.WriteString(" [same as previous]\n")
			} else {
				fmt.Fprintf(&sb, "\n%s;\n", mutated)
			}
			prev = mutated
		}
		err = fmt.Errorf("%w%s", err, sb.String())
	}()

	var first []interface{}
	var firstName string
	var minCount int
	rowCounts := make(map[string]int)
ReadRows:
	for {
		first = nil
		firstName = ""
		for name, rows := range connRows {
			if !rows.Next() {
				minCount = rowCounts[name]
				break ReadRows
			}
			rowCounts[name]++
			vals, err := rows.Values()
			if err != nil {
				// This function can fail if, for example,
				// a number doesn't fit into a float64. Ignore
				// them and move along to another query.
				return nil //nolint:returnerrcheck
			}
			if firstName == "" {
				firstName = name
				first = vals
			} else {
				if err := CompareVals(first, vals); err != nil {
					return fmt.Errorf("compare %s to %s:\n%v", firstName, name, err)
				}
			}
		}
	}
	// Make sure all are empty.
	for name, rows := range connRows {
		for rows.Next() {
			rowCounts[name]++
		}
		if err := rows.Err(); err != nil {
			// Aww someone had a SQL error maybe, so we can't use this query.
			return nil //nolint:returnerrcheck
		}
	}
	// Ensure each connection returned the same number of rows.
	for name, count := range rowCounts {
		if minCount != count {
			return fmt.Errorf("%s had %d rows, expected %d", name, count, minCount)
		}
	}
	return nil
}
