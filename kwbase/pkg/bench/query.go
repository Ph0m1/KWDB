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

package bench

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"os/exec"

	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
)

// This is the TPC-B(ish) query that pgbench runs.
// We don't use placeholders because pgwire protocol does not
// allow multiple statements in prepared queries.
const tpcbQuery = `BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + %[1]d WHERE aid = %[2]d;
SELECT abalance FROM pgbench_accounts WHERE aid = %[2]d;
UPDATE pgbench_tellers SET tbalance = tbalance + %[1]d WHERE tid = %[3]d;
UPDATE pgbench_branches SET bbalance = bbalance + %[1]d WHERE bid = %[4]d;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (%[3]d, %[4]d, %[2]d, %[1]d, CURRENT_TIMESTAMP);
END;` // vars: 1 delta, 2 aid, 3 tid, 4 bid

// RunOne executes one iteration of the query batch that `pgbench` executes.
// Calls b.Fatalf if it encounters an error.
func RunOne(db sqlutils.DBHandle, r *rand.Rand, accounts int) error {
	account := r.Intn(accounts)
	delta := r.Intn(5000)
	teller := r.Intn(tellers)
	branch := 1

	q := fmt.Sprintf(tpcbQuery, delta, account, teller, branch)
	_, err := db.ExecContext(context.TODO(), q)
	return err
}

// ExecPgbench returns a ready-to-run pgbench Cmd, that will run
// against the db specified by `pgURL`.
func ExecPgbench(pgURL url.URL, dbname string, count int) (*exec.Cmd, error) {
	host, port, err := net.SplitHostPort(pgURL.Host)
	if err != nil {
		return nil, err
	}

	args := []string{
		"-n", // disable (pg-specific) vacuum
		"-r", // print stats
		fmt.Sprintf("--transactions=%d", count),
		"-h", host,
		"-p", port,
	}

	if pgURL.User != nil {
		if user := pgURL.User.Username(); user != "" {
			args = append(args, "-U", user)
		}
	}
	args = append(args, dbname)

	return exec.Command("pgbench", args...), nil
}
