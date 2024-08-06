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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/blobs/blobspb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/rpc/nodedialer"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/netutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

func createTestResources(t testing.TB) (string, string, *stop.Stopper, func()) {
	localExternalDir, cleanupFn := testutils.TempDir(t)
	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	stopper := stop.NewStopper()
	return localExternalDir, remoteExternalDir, stopper, func() {
		cleanupFn()
		cleanupFn2()
		stopper.Stop(context.TODO())
		leaktest.AfterTest(t)()
	}
}

func setUpService(
	t testing.TB,
	rpcContext *rpc.Context,
	localNodeID roachpb.NodeID,
	remoteNodeID roachpb.NodeID,
	localExternalDir string,
	remoteExternalDir string,
) BlobClientFactory {
	s := rpc.NewServer(rpcContext)
	remoteBlobServer, err := NewBlobService(remoteExternalDir)
	if err != nil {
		t.Fatal(err)
	}
	blobspb.RegisterBlobServer(s, remoteBlobServer)
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	s2 := rpc.NewServer(rpcContext)
	localBlobServer, err := NewBlobService(localExternalDir)
	if err != nil {
		t.Fatal(err)
	}
	blobspb.RegisterBlobServer(s2, localBlobServer)
	ln2, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s2, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	localDialer := nodedialer.New(rpcContext,
		func(nodeID roachpb.NodeID) (net.Addr, error) {
			if nodeID == remoteNodeID {
				return ln.Addr(), nil
			} else if nodeID == localNodeID {
				return ln2.Addr(), nil
			}
			return nil, errors.Errorf("node %d not found", nodeID)
		},
	)
	return NewBlobClientFactory(
		localNodeID,
		localDialer,
		localExternalDir,
	)
}

func writeTestFile(t testing.TB, file string, content []byte) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBlobClientReadFile(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(t)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClientFactory := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)

	for _, tc := range []struct {
		name        string
		nodeID      roachpb.NodeID
		filename    string
		fileContent []byte
		err         string
	}{
		{
			"read-remote-file",
			remoteNodeID,
			"test/remote.csv",
			remoteFileContent,
			"",
		},
		{
			"read-local-file",
			localNodeID,
			"test/local.csv",
			localFileContent,
			"",
		},
		{
			"read-file-not-exist",
			remoteNodeID,
			"test/notexist.csv",
			nil,
			"no such file",
		},
		{
			"read-dir-exists",
			remoteNodeID,
			"test",
			nil,
			"is a directory",
		},
		{
			"read-check-calling-clean",
			remoteNodeID,
			"../test/remote.csv",
			nil,
			"outside of external-io-dir is not allowed",
		},
		{
			"read-outside-extern-dir",
			remoteNodeID,
			// this file exists, but is not within remote node's externalIODir
			filepath.Join("../..", localExternalDir, "test/local.csv"),
			nil,
			"outside of external-io-dir is not allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			blobClient, err := blobClientFactory(ctx, tc.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := blobClient.ReadFile(ctx, tc.filename)
			if err != nil {
				if testutils.IsError(err, tc.err) {
					// correct error was returned
					return
				}
				t.Fatal(err)
			}
			// Check that fetched file content is correct
			content, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(content, tc.fileContent) {
				t.Fatal(fmt.Sprintf(`fetched file content incorrect, expected %s, got %s`, tc.fileContent, content))
			}
		})
	}
}

func TestBlobClientWriteFile(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(t)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClientFactory := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	for _, tc := range []struct {
		name               string
		nodeID             roachpb.NodeID
		filename           string
		fileContent        string
		destinationNodeDir string
		err                string
	}{
		{
			"write-remote-file",
			remoteNodeID,
			"test/remote.csv",
			"remotefile",
			remoteExternalDir,
			"",
		},
		{
			"write-local-file",
			localNodeID,
			"test/local.csv",
			"localfile",
			localExternalDir,
			"",
		},
		{
			"write-outside-extern-dir",
			remoteNodeID,
			"/../../../outside.csv",
			"remotefile",
			remoteExternalDir,
			"not allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			blobClient, err := blobClientFactory(ctx, tc.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			byteContent := []byte(tc.fileContent)
			err = blobClient.WriteFile(ctx, tc.filename, bytes.NewReader(byteContent))
			if err != nil {
				if testutils.IsError(err, tc.err) {
					// correct error was returned
					return
				}
				t.Fatal(err)
			}
			// Check that file is now in correct node
			content, err := ioutil.ReadFile(filepath.Join(tc.destinationNodeDir, tc.filename))
			if err != nil {
				t.Fatal(err, "unable to read fetched file")
			}
			if !bytes.Equal(content, byteContent) {
				t.Fatal(fmt.Sprintf(`fetched file content incorrect, expected %s, got %s`, tc.fileContent, content))
			}
		})
	}
}

func TestBlobClientList(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(t)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClientFactory := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileNames := []string{"/file/local/dataA.csv", "/file/local/dataB.csv", "/file/local/dataC.csv"}
	remoteFileNames := []string{"/file/remote/A.csv", "/file/remote/B.csv", "/file/remote/C.csv"}
	for _, fileName := range localFileNames {
		fullPath := filepath.Join(localExternalDir, fileName)
		writeTestFile(t, fullPath, []byte("testLocalFile"))
	}
	for _, fileName := range remoteFileNames {
		fullPath := filepath.Join(remoteExternalDir, fileName)
		writeTestFile(t, fullPath, []byte("testRemoteFile"))
	}

	for _, tc := range []struct {
		name         string
		nodeID       roachpb.NodeID
		dirName      string
		expectedList []string
		err          string
	}{
		{
			"list-local",
			localNodeID,
			"file/local/*.csv",
			localFileNames,
			"",
		},
		{
			"list-remote",
			remoteNodeID,
			"file/remote/*.csv",
			remoteFileNames,
			"",
		},
		{
			"list-local-no-match",
			localNodeID,
			"file/doesnotexist/*",
			[]string{},
			"",
		},
		{
			"list-remote-no-match",
			remoteNodeID,
			"file/doesnotexist/*",
			[]string{},
			"",
		},
		{
			"list-empty-pattern",
			remoteNodeID,
			"",
			[]string{},
			"pattern cannot be empty",
		},
		{
			// should list files in top level directory
			"list-star",
			remoteNodeID,
			"*",
			[]string{"/file"},
			"",
		},
		{
			"list-outside-external-dir",
			remoteNodeID,
			"../*", // will error out
			[]string{},
			"outside of external-io-dir is not allowed",
		},
		{
			"list-backout-external-dir",
			remoteNodeID,
			"..",
			[]string{},
			"outside of external-io-dir is not allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			blobClient, err := blobClientFactory(ctx, tc.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			list, err := blobClient.List(ctx, tc.dirName)
			if err != nil {
				if testutils.IsError(err, tc.err) {
					// correct error returned
					return
				}
				t.Fatal(err)
			}
			// Check that returned list matches expected list
			if len(list) != len(tc.expectedList) {
				t.Fatal(`listed incorrect number of files`, list)
			}
			for i, f := range list {
				if f != tc.expectedList[i] {
					t.Fatal("incorrect list returned ", list)
				}
			}
		})
	}
}

func TestBlobClientDeleteFrom(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(t)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClientFactory := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote2.csv"), remoteFileContent)

	for _, tc := range []struct {
		name     string
		nodeID   roachpb.NodeID
		filename string
		err      string
	}{
		{
			"delete-remote-file",
			remoteNodeID,
			"test/remote.csv",
			"",
		},
		{
			"delete-local-file",
			localNodeID,
			"test/local.csv",
			"",
		},
		{
			"delete-remote-file-does-not-exist",
			remoteNodeID,
			"test/doesnotexist",
			"no such file",
		},
		{
			"delete-directory-not-empty",
			remoteNodeID,
			"test",
			"directory not empty",
		},
		{
			"delete-directory-empty", // this should work
			localNodeID,
			"test",
			"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			blobClient, err := blobClientFactory(ctx, tc.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			err = blobClient.Delete(ctx, tc.filename)
			if err != nil {
				if testutils.IsError(err, tc.err) {
					// the correct error was returned
					return
				}
				t.Fatal(err)
			}

			_, err = ioutil.ReadFile(filepath.Join(localExternalDir, tc.filename))
			if err == nil {
				t.Fatal(err, "file should have been deleted")
			}
		})
	}
}

func TestBlobClientStat(t *testing.T) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(t)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	blobClientFactory := setUpService(t, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)

	localFileContent := []byte("local_file")
	remoteFileContent := []byte("remote_file")
	writeTestFile(t, filepath.Join(localExternalDir, "test/local.csv"), localFileContent)
	writeTestFile(t, filepath.Join(remoteExternalDir, "test/remote.csv"), remoteFileContent)

	for _, tc := range []struct {
		name         string
		nodeID       roachpb.NodeID
		filename     string
		expectedSize int64
		err          string
	}{
		{
			"stat-remote-file",
			remoteNodeID,
			"test/remote.csv",
			int64(len(remoteFileContent)),
			"",
		},
		{
			"stat-local-file",
			localNodeID,
			"test/local.csv",
			int64(len(localFileContent)),
			"",
		},
		{
			"stat-remote-file-does-not-exist",
			remoteNodeID,
			"test/doesnotexist",
			0,
			"no such file",
		},
		{
			"stat-directory",
			remoteNodeID,
			"test",
			0,
			"is a directory",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			blobClient, err := blobClientFactory(ctx, tc.nodeID)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := blobClient.Stat(ctx, tc.filename)
			if err != nil {
				if testutils.IsError(err, tc.err) {
					// the correct error was returned
					return
				}
				t.Fatal(err)
			}
			if resp.Filesize != tc.expectedSize {
				t.Fatalf("expected size: %d got: %d", tc.expectedSize, resp)
			}
		})
	}
}
