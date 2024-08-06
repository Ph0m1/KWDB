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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/version"
	"github.com/cockroachdb/errors"
)

func registerBackup(r *testRegistry) {
	importBankData := func(ctx context.Context, rows int, t *test, c *cluster) string {
		dest := c.name

		if local {
			rows = 100
			dest += fmt.Sprintf("%d", timeutil.Now().UnixNano())
		}

		c.Put(ctx, workload, "./workload")
		c.Put(ctx, kwbase, "./kwbase")

		// NB: starting the cluster creates the logs dir as a side effect,
		// needed below.
		c.Start(ctx, t)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)
		time.Sleep(time.Second) // wait for csv server to open listener

		importArgs := []string{
			"./workload", "fixtures", "import", "bank",
			"--db=bank", "--payload-bytes=10240", "--ranges=0", "--csv-server", "http://localhost:8081",
			fmt.Sprintf("--rows=%d", rows), "--seed=1", "{pgurl:1}",
		}
		c.Run(ctx, c.Node(1), importArgs...)

		return dest
	}

	backup2TBSpec := makeClusterSpec(10)
	r.Add(testSpec{
		Name:       fmt.Sprintf("backup/2TB/%s", backup2TBSpec),
		Owner:      OwnerBulkIO,
		Cluster:    backup2TBSpec,
		MinVersion: "v2.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			rows := 65104166
			dest := importBankData(ctx, rows, t, c)
			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`running backup`)
				c.Run(ctx, c.Node(1), `./kwbase sql --insecure -e "
				BACKUP bank.bank TO 'gs://kwbasedb-backup-testing/`+dest+`'"`)
				return nil
			})
			m.Wait()
		},
	})

	// backupTPCC continuously runs TPCC, takes a full backup after some time,
	// and incremental after more time. It then restores the two backups and
	// verifies them with a fingerprint.
	r.Add(testSpec{
		Name:    `backupTPCC`,
		Owner:   OwnerBulkIO,
		Cluster: makeClusterSpec(3),
		Timeout: 1 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, kwbase, "./kwbase")
			c.Put(ctx, workload, "./workload")
			c.Start(ctx, t)
			conn := c.Conn(ctx, 1)

			duration := 5 * time.Minute
			if local {
				duration = 5 * time.Second
			}
			warehouses := 10

			backupDir := "gs://kwbasedb-backup-testing/" + c.name
			// Use inter-node file sharing on 20.1+.
			if r.buildVersion.AtLeast(version.MustParse(`v20.1.0-0`)) {
				backupDir = "nodelocal://1/" + c.name
			}
			fullDir := backupDir + "/full"
			incDir := backupDir + "/inc"

			t.Status(`workload initialization`)
			cmd := []string{fmt.Sprintf(
				"./workload init tpcc --warehouses=%d {pgurl:1-%d}",
				warehouses, c.spec.NodeCount,
			)}
			if !t.buildVersion.AtLeast(version.MustParse("v20.2.0")) {
				cmd = append(cmd, "--deprecated-fk-indexes")
			}
			c.Run(ctx, c.Node(1), cmd...)

			m := newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				_, err := conn.ExecContext(ctx, `
					CREATE DATABASE restore_full;
					CREATE DATABASE restore_inc;
				`)
				return err
			})
			m.Wait()

			t.Status(`run tpcc`)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			cmdDone := make(chan error)
			go func() {
				cmd := fmt.Sprintf(
					"./workload run tpcc --warehouses=%d {pgurl:1-%d}",
					warehouses, c.spec.NodeCount,
				)

				cmdDone <- c.RunE(ctx, c.Node(1), cmd)
			}()

			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}

			// Use a time slightly in the past to avoid "cannot specify timestamp in the future" errors.
			tFull := fmt.Sprint(timeutil.Now().Add(time.Second * -2).UnixNano())
			m = newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`full backup`)
				_, err := conn.ExecContext(ctx,
					`BACKUP tpcc.* TO $1 AS OF SYSTEM TIME `+tFull,
					fullDir,
				)
				return err
			})
			m.Wait()

			t.Status(`continue tpcc`)
			select {
			case <-time.After(duration):
			case <-ctx.Done():
				return
			}

			tInc := fmt.Sprint(timeutil.Now().Add(time.Second * -2).UnixNano())
			m = newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`incremental backup`)
				_, err := conn.ExecContext(ctx,
					`BACKUP tpcc.* TO $1 AS OF SYSTEM TIME `+tInc+` INCREMENTAL FROM $2`,
					incDir,
					fullDir,
				)
				if err != nil {
					return err
				}

				// Backups are done, make sure workload is still running.
				select {
				case err := <-cmdDone:
					// Workload exited before it should have.
					return err
				default:
					return nil
				}
			})
			m.Wait()

			m = newMonitor(ctx, c)
			m.Go(func(ctx context.Context) error {
				t.Status(`restore full`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE tpcc.* FROM $1 WITH into_db='restore_full'`,
					fullDir,
				); err != nil {
					return err
				}

				t.Status(`restore incremental`)
				if _, err := conn.ExecContext(ctx,
					`RESTORE tpcc.* FROM $1, $2 WITH into_db='restore_inc'`,
					fullDir,
					incDir,
				); err != nil {
					return err
				}

				t.Status(`fingerprint`)
				// TODO(adityamaru): Pull the fingerprint logic into a utility method
				// which can be shared by multiple roachtests.
				fingerprint := func(db string, asof string) (string, error) {
					var b strings.Builder

					var tables []string
					rows, err := conn.QueryContext(
						ctx,
						fmt.Sprintf("SELECT table_name FROM [SHOW TABLES FROM %s] ORDER BY table_name", db),
					)
					if err != nil {
						return "", err
					}
					defer rows.Close()
					for rows.Next() {
						var name string
						if err := rows.Scan(&name); err != nil {
							return "", err
						}
						tables = append(tables, name)
					}

					for _, table := range tables {
						fmt.Fprintf(&b, "table %s\n", table)
						query := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", db, table)
						if asof != "" {
							query = fmt.Sprintf("SELECT * FROM [%s] AS OF SYSTEM TIME %s", query, asof)
						}
						rows, err = conn.QueryContext(ctx, query)
						if err != nil {
							return "", err
						}
						defer rows.Close()
						for rows.Next() {
							var name, fp string
							if err := rows.Scan(&name, &fp); err != nil {
								return "", err
							}
							fmt.Fprintf(&b, "%s: %s\n", name, fp)
						}
					}

					return b.String(), rows.Err()
				}

				tpccFull, err := fingerprint("tpcc", tFull)
				if err != nil {
					return err
				}
				tpccInc, err := fingerprint("tpcc", tInc)
				if err != nil {
					return err
				}
				restoreFull, err := fingerprint("restore_full", "")
				if err != nil {
					return err
				}
				restoreInc, err := fingerprint("restore_inc", "")
				if err != nil {
					return err
				}

				if tpccFull != restoreFull {
					return errors.Errorf("got %s, expected %s", restoreFull, tpccFull)
				}
				if tpccInc != restoreInc {
					return errors.Errorf("got %s, expected %s", restoreInc, tpccInc)
				}

				return nil
			})
			m.Wait()
		},
	})

}
