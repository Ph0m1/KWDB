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

package binfetcher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
)

func TestDownload(t *testing.T) {
	if testutils.NightlyStress() {
		t.Skip()
	}
	t.Skip("disabled by default because downloading files in CI is a silly idea")

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tests := []Options{
		// Cockroach.
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "v1.0.4",
			GOOS:    "linux",
		},
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "v1.0.6",
			GOOS:    "darwin",
		},
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "darwin",
		},
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "v1.1.3",
			GOOS:    "windows",
		},
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "windows",
		},
		// TODO(tschottdorf): seems like SHAs get removed from edge-binaries.kwbasedb.com?
		// {
		// 	Binary:  "kwbase",
		// 	Dir:     dir,
		// 	Version: "bd828feaa309578142fe7ad2d89ee1b70adbd52d",
		// 	GOOS:    "linux",
		// },
		{
			Binary:  "kwbase",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},

		// Load generators.
		{
			Binary:  "loadgen/kv",
			Dir:     dir,
			Version: "LATEST",
			GOOS:    "linux",
		},
		{
			Binary:  "loadgen/tpcc",
			Dir:     dir,
			Version: "619a768955d5e2cb0b3ae77a8950eec5cd06c041",
			GOOS:    "linux",
		},
		{
			Component: "loadgen",
			Binary:    "ycsb",
			Dir:       dir,
			Version:   "LATEST",
			GOOS:      "linux",
		},
	}
	// Run twice to check that that doesn't cause errors.
	for i := 0; i < 1; i++ {
		for j, opts := range tests {
			t.Run(fmt.Sprintf("num=%d,case=%d", i, j), func(t *testing.T) {
				ctx := context.Background()
				s, err := Download(ctx, opts)
				if err != nil {
					t.Fatal(err)
				}
				if stat, err := os.Stat(s); err != nil {
					t.Fatal(err)
				} else if stat.Size() == 0 {
					t.Fatal("empty file")
				}
			})
		}
	}
}
