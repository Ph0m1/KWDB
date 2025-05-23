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
	"io/ioutil"
	"os"
	"path/filepath"
)

func registerSyncTest(r *testRegistry) {
	const nemesisScript = `#!/usr/bin/env bash

if [[ $1 == "on" ]]; then
  charybdefs-nemesis --probability
else
  charybdefs-nemesis --clear
fi
`

	r.Add(testSpec{
		Skip:       "#48603: broken on Pebble",
		Name:       "synctest",
		Owner:      OwnerStorage,
		MinVersion: "v19.1.0",
		// This test sets up a custom file system; we don't want the cluster reused.
		Cluster: makeClusterSpec(1, reuseNone()),
		Run: func(ctx context.Context, t *test, c *cluster) {
			n := c.Node(1)
			tmpDir, err := ioutil.TempDir("", "synctest")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.RemoveAll(tmpDir)
			}()
			nemesis := filepath.Join(tmpDir, "nemesis")

			if err := ioutil.WriteFile(nemesis, []byte(nemesisScript), 0755); err != nil {
				t.Fatal(err)
			}

			c.Put(ctx, kwbase, "./kwbase")
			c.Put(ctx, nemesis, "./nemesis")
			c.Run(ctx, n, "chmod +x nemesis")
			c.Run(ctx, n, "sudo umount {store-dir}/faulty || true")
			c.Run(ctx, n, "mkdir -p {store-dir}/{real,faulty} || true")
			t.Status("setting up charybdefs")

			if err := execCmd(ctx, t.l, roachprod, "install", c.makeNodes(n), "charybdefs"); err != nil {
				t.Fatal(err)
			}
			c.Run(ctx, n, "sudo charybdefs {store-dir}/faulty -oallow_other,modules=subdir,subdir={store-dir}/real && chmod 777 {store-dir}/{real,faulty}")

			t.Status("running synctest")
			c.Run(ctx, n, "./kwbase debug synctest {store-dir}/faulty ./nemesis")
		},
	})
}
