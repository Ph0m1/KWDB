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

package main

import (
	"context"
	"fmt"
)

// runManySplits attempts to create 2000 tiny ranges on a 4-node cluster using
// left-to-right splits and check the cluster is still live afterwards.
func runManySplits(ctx context.Context, t *test, c *cluster) {
	args := startArgs("--env=KWBASE_SCAN_MAX_IDLE_TIME=5ms")
	c.Put(ctx, kwbase, "./kwbase")
	c.Start(ctx, t, args)

	db := c.Conn(ctx, 1)
	defer db.Close()

	// Wait for upreplication then create many ranges.
	waitForFullReplication(t, db)

	m := newMonitor(ctx, c, c.All())
	m.Go(func(ctx context.Context) error {
		const numRanges = 2000
		t.l.Printf("creating %d ranges...", numRanges)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE t(x, PRIMARY KEY(x)) AS TABLE generate_series(1,%[1]d);
            ALTER TABLE t SPLIT AT TABLE generate_series(1,%[1]d);
		`, numRanges)); err != nil {
			return err
		}
		return nil
	})
	m.Wait()
}
