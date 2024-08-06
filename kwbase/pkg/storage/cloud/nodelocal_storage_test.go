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

package cloud

import (
	"context"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/blobs"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings.ExternalIODir = p
	dest := MakeLocalStorageURI(p)

	testExportStore(t, dest, false)
	testListFiles(t, "nodelocal://1/listing-test/basepath")
}

func TestLocalIOLimits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	const allowed = "/allowed"
	testSettings.ExternalIODir = allowed

	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)

	baseDir, err := ExternalStorageFromURI(
		ctx, "nodelocal://01/", base.ExternalIOConfig{}, testSettings, clientFactory)
	if err != nil {
		t.Fatal(err)
	}

	for dest, expected := range map[string]string{allowed: "", "/../../blah": "not allowed"} {
		u := fmt.Sprintf("nodelocal://01%s", dest)
		e, err := ExternalStorageFromURI(
			ctx, u, base.ExternalIOConfig{}, testSettings, clientFactory)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = e.ListFiles(ctx, ""); !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
		if _, err = baseDir.ListFiles(ctx, dest); !testutils.IsError(err, expected) {
			t.Fatal(err)
		}
	}

	for host, expectErr := range map[string]bool{"": false, "1": false, "0": false, "blah": true} {
		u := fmt.Sprintf("nodelocal://01%s/path/to/file", host)

		var expected string
		if expectErr {
			expected = "host component of nodelocal URI must be a node ID"
		}
		if _, err := ExternalStorageConfFromURI(u); !testutils.IsError(err, expected) {
			t.Fatalf("%q: expected error %q, got %v", u, expected, err)
		}
	}
}
