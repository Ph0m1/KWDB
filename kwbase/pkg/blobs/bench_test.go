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
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// filesize should be at least 1 GB when running these benchmarks.
// Reduced to 129 K for CI.
const filesize = 129 * 1 << 10

type benchmarkTestCase struct {
	localNodeID       roachpb.NodeID
	remoteNodeID      roachpb.NodeID
	localExternalDir  string
	remoteExternalDir string

	blobClient BlobClient
	fileSize   int64
	fileName   string
}

func writeLargeFile(t testing.TB, file string, size int64) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkStreamingReadFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          filesize,
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingReadFile(b, params)
}

func benchmarkStreamingReadFile(b *testing.B, tc *benchmarkTestCase) {
	writeLargeFile(b, filepath.Join(tc.remoteExternalDir, tc.fileName), tc.fileSize)
	writeTo := LocalStorage{externalIODir: tc.localExternalDir}
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		reader, err := tc.blobClient.ReadFile(context.TODO(), tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		err = writeTo.WriteFile(tc.fileName, reader)
		if err != nil {
			b.Fatal(err)
		}
		stat, err := writeTo.Stat(tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if stat.Filesize != tc.fileSize {
			b.Fatal("incorrect number of bytes written")
		}
	}
}

func BenchmarkStreamingWriteFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          filesize,
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingWriteFile(b, params)
}

func benchmarkStreamingWriteFile(b *testing.B, tc *benchmarkTestCase) {
	content := make([]byte, tc.fileSize)
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		err := tc.blobClient.WriteFile(context.TODO(), tc.fileName, bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}
}
