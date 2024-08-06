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

package cli

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func Example_nodelocal() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	file, cleanUp := createTestFile("test.csv", "content")
	defer cleanUp()

	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file2.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/../../file1.csv", file))
	c.Run(fmt.Sprintf("nodelocal upload notexist.csv /test/file1.csv"))

	// Output:
	// nodelocal upload test.csv /test/file1.csv
	// successfully uploaded to nodelocal://1/test/file1.csv
	// nodelocal upload test.csv /test/file2.csv
	// successfully uploaded to nodelocal://1/test/file2.csv
	// nodelocal upload test.csv /test/file1.csv
	// ERROR: destination file already exists for /test/file1.csv
	// nodelocal upload test.csv /test/../../file1.csv
	// ERROR: local file access to paths outside of external-io-dir is not allowed: /test/../../file1.csv
	// nodelocal upload notexist.csv /test/file1.csv
	// ERROR: open notexist.csv: no such file or directory
}

func Example_nodelocal_disabled() {
	c := newCLITest(cliTestParams{noNodelocal: true})
	defer c.cleanup()

	file, cleanUp := createTestFile("test.csv", "non-empty-file")
	defer cleanUp()

	empty, cleanUpEmpty := createTestFile("empty.csv", "")
	defer cleanUpEmpty()

	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", empty))
	c.Run(fmt.Sprintf("nodelocal upload %s /test/file1.csv", file))

	// Output:
	// nodelocal upload empty.csv /test/file1.csv
	// ERROR: local file access is disabled
	// nodelocal upload test.csv /test/file1.csv
	// ERROR: local file access is disabled
}

func TestNodeLocalFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()

	for i, tc := range []struct {
		name        string
		fileContent []byte
	}{
		{
			"empty",
			[]byte{},
		},
		{
			"exactly-one-chunk",
			make([]byte, chunkSize),
		},
		{
			"exactly-five-chunks",
			make([]byte, chunkSize*5),
		},
		{
			"less-than-one-chunk",
			make([]byte, chunkSize-100),
		},
		{
			"more-than-one-chunk",
			make([]byte, chunkSize+100),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(dir, fmt.Sprintf("file%d.csv", i))
			err := ioutil.WriteFile(filePath, tc.fileContent, 0666)
			if err != nil {
				t.Fatal(err)
			}
			destination := fmt.Sprintf("/test/file%d.csv", i)

			_, err = c.RunWithCapture(fmt.Sprintf("nodelocal upload %s %s", filePath, destination))
			if err != nil {
				t.Fatal(err)
			}
			writtenContent, err := ioutil.ReadFile(filepath.Join(c.Cfg.Settings.ExternalIODir, destination))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(tc.fileContent, writtenContent) {
				t.Fatalf("expected:\n%s\nbut got:\n%s", tc.fileContent, writtenContent)
			}
		})
	}
}

func createTestFile(name, content string) (string, func()) {
	err := ioutil.WriteFile(name, []byte(content), 0666)
	if err != nil {
		return "", func() {}
	}
	return name, func() {
		_ = os.Remove(name)
	}
}
