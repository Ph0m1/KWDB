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

package blobs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/blobs/blobspb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
)

func TestBlobServiceList(t *testing.T) {
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	fileContent := []byte("a")
	files := []string{"/file/dir/a.csv", "/file/dir/b.csv", "/file/dir/c.csv"}
	for _, file := range files {
		writeTestFile(t, filepath.Join(tmpDir, file), fileContent)
	}

	service, err := NewBlobService(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.TODO()

	t.Run("list-correct-files", func(t *testing.T) {
		resp, err := service.List(ctx, &blobspb.GlobRequest{
			Pattern: "file/dir/*.csv",
		})
		if err != nil {
			t.Fatal(err)
		}
		resultList := resp.Files
		if len(resultList) != len(files) {
			t.Fatal("result list does not have the correct number of files")
		}
		for i, f := range resultList {
			if f != files[i] {
				t.Fatalf("result list is incorrect %s", resultList)
			}
		}
	})
	t.Run("not-in-external-io-dir", func(t *testing.T) {
		_, err := service.List(ctx, &blobspb.GlobRequest{
			Pattern: "file/../../*.csv",
		})
		if err == nil {
			t.Fatal("expected error but was not caught")
		}
		if !testutils.IsError(err, "outside of external-io-dir is not allowed") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
}

func TestBlobServiceDelete(t *testing.T) {
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	fileContent := []byte("file_content")
	filename := "path/to/file/content.txt"
	writeTestFile(t, filepath.Join(tmpDir, filename), fileContent)

	service, err := NewBlobService(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.TODO()

	t.Run("delete-correct-file", func(t *testing.T) {
		_, err := service.Delete(ctx, &blobspb.DeleteRequest{
			Filename: filename,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(tmpDir, filename)); !os.IsNotExist(err) {
			t.Fatalf("expected not exists err, got: %s", err)
		}
	})
	t.Run("file-not-exist", func(t *testing.T) {
		_, err := service.Delete(ctx, &blobspb.DeleteRequest{
			Filename: "file/does/not/exist",
		})
		if err == nil {
			t.Fatal("expected error but was not caught")
		}
		if !testutils.IsError(err, "no such file") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
	t.Run("not-in-external-io-dir", func(t *testing.T) {
		_, err := service.Delete(ctx, &blobspb.DeleteRequest{
			Filename: "file/../../content.txt",
		})
		if err == nil {
			t.Fatal("expected error but was not caught")
		}
		if !testutils.IsError(err, "outside of external-io-dir is not allowed") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
}

func TestBlobServiceStat(t *testing.T) {
	tmpDir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	fileContent := []byte("file_content")
	filename := "path/to/file/content.txt"
	writeTestFile(t, filepath.Join(tmpDir, filename), fileContent)

	service, err := NewBlobService(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.TODO()

	t.Run("get-correct-file-size", func(t *testing.T) {
		resp, err := service.Stat(ctx, &blobspb.StatRequest{
			Filename: filename,
		})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Filesize != int64(len(fileContent)) {
			t.Fatalf("expected filesize: %d, got %d", len(fileContent), resp.Filesize)
		}
	})
	t.Run("file-not-exist", func(t *testing.T) {
		_, err := service.Stat(ctx, &blobspb.StatRequest{
			Filename: "file/does/not/exist",
		})
		if err == nil {
			t.Fatal("expected error but was not caught")
		}
		if !testutils.IsError(err, "no such file") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
	t.Run("not-in-external-io-dir", func(t *testing.T) {
		_, err := service.Stat(ctx, &blobspb.StatRequest{
			Filename: "file/../../content.txt",
		})
		if err == nil {
			t.Fatal("expected error but was not caught")
		}
		if !testutils.IsError(err, "outside of external-io-dir is not allowed") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
	t.Run("stat-directory", func(t *testing.T) {
		_, err := service.Stat(ctx, &blobspb.StatRequest{
			Filename: filepath.Dir(filename),
		})
		if err == nil {
			t.Fatalf("expected error but was not caught")
		}
		if !testutils.IsError(err, "expected a file") {
			t.Fatal("incorrect error message: " + err.Error())
		}
	})
}
