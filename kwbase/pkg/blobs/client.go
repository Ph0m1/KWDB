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
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/blobs/blobspb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/rpc/nodedialer"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

// BlobClient provides an interface for file access on all nodes' local storage.
// Given the nodeID of the node on which the operation should occur, the a blob
// client should be able to find the correct node and call its blob service API.
type BlobClient interface {
	// ReadFile fetches the named payload from the requested node,
	// and stores it in memory. It then returns an io.ReadCloser to
	// read the contents.
	ReadFile(ctx context.Context, file string) (io.ReadCloser, error)

	// AppendWrite sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID. append write.
	AppendWrite(ctx context.Context, file string, content io.ReadSeeker) error

	// WriteFile sends the named payload to the requested node.
	// This method will read entire content of file and send
	// it over to another node, based on the nodeID.
	WriteFile(ctx context.Context, file string, content io.ReadSeeker) error

	// List lists the corresponding filenames from the requested node.
	// The requested node can be the current node.
	List(ctx context.Context, pattern string) ([]string, error)

	// Delete deletes the specified file or empty directory from a remote node.
	Delete(ctx context.Context, file string) error

	// Stat gets the size (in bytes) of a specified file from a remote node.
	Stat(ctx context.Context, file string) (*blobspb.BlobStat, error)

	// Seek sets the offset for the next Read on file to offset.It then returns
	// an io.ReadCloser to read the contents. interpreted according to whence:
	// 0 means relative to the origin of the file, 1 means relative to the current
	// offset, and 2 means relative to the end.
	Seek(ctx context.Context, filename string, offset int64, whence int) (io.ReadCloser, error)
}

var _ BlobClient = &remoteClient{}

// remoteClient uses the node dialer and blob service clients
// to Read or Write bulk files from/to other nodes.
type remoteClient struct {
	blobClient blobspb.BlobClient
}

// newRemoteClient instantiates a remote blob service client.
func newRemoteClient(blobClient blobspb.BlobClient) BlobClient {
	return &remoteClient{blobClient: blobClient}
}

func (c *remoteClient) AppendWrite(ctx context.Context, file string, content io.ReadSeeker) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "filename", file, "append", "true")
	stream, err := c.blobClient.PutStream(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_, closeErr := stream.CloseAndRecv()
		if err == nil {
			err = closeErr
		}
	}()
	err = streamContent(stream, content)
	return err
}

func (c *remoteClient) ReadFile(ctx context.Context, file string) (io.ReadCloser, error) {
	// Check that file exists before reading from it
	_, err := c.Stat(ctx, file)
	if err != nil {
		return nil, err
	}
	stream, err := c.blobClient.GetStream(ctx, &blobspb.GetRequest{
		Filename: file,
	})
	return newGetStreamReader(stream), errors.Wrap(err, "fetching file")
}

func (c *remoteClient) WriteFile(
	ctx context.Context, file string, content io.ReadSeeker,
) (err error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "filename", file)
	stream, err := c.blobClient.PutStream(ctx)
	if err != nil {
		return
	}
	defer func() {
		_, closeErr := stream.CloseAndRecv()
		if err == nil {
			err = closeErr
		}
	}()
	err = streamContent(stream, content)
	return
}

func (c *remoteClient) List(ctx context.Context, pattern string) ([]string, error) {
	resp, err := c.blobClient.List(ctx, &blobspb.GlobRequest{
		Pattern: pattern,
	})
	if err != nil {
		return nil, errors.Wrap(err, "fetching list")
	}
	return resp.Files, nil
}

func (c *remoteClient) Delete(ctx context.Context, file string) error {
	_, err := c.blobClient.Delete(ctx, &blobspb.DeleteRequest{
		Filename: file,
	})
	return err
}

func (c *remoteClient) Stat(ctx context.Context, file string) (*blobspb.BlobStat, error) {
	resp, err := c.blobClient.Stat(ctx, &blobspb.StatRequest{
		Filename: file,
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *remoteClient) Seek(
	ctx context.Context, filename string, offset int64, whence int,
) (io.ReadCloser, error) {
	// Check that file exists before reading from it
	_, err := c.Stat(ctx, filename)
	if err != nil {
		return nil, err
	}
	stream, err := c.blobClient.Seek(context.TODO(), &blobspb.SeekRequest{
		Filename: filename,
		Offset:   offset,
		Whence:   int32(whence),
	})
	if err != nil {
		return nil, err
	}
	return newGetStreamReader(stream), errors.Wrap(err, "fetching file")
}

var _ BlobClient = &localClient{}

// localClient executes the local blob service's code
// to Read or Write bulk files on the current node.
type localClient struct {
	localStorage *LocalStorage
}

// newLocalClient instantiates a local blob service client.
func newLocalClient(externalIODir string) (BlobClient, error) {
	storage, err := NewLocalStorage(externalIODir)
	if err != nil {
		return nil, errors.Wrap(err, "creating local client")
	}
	return &localClient{localStorage: storage}, nil
}

func (c *localClient) AppendWrite(ctx context.Context, file string, content io.ReadSeeker) error {
	return c.localStorage.AppendWrite(file, content)
}

func (c *localClient) ReadFile(ctx context.Context, file string) (io.ReadCloser, error) {
	return c.localStorage.ReadFile(file)
}

func (c *localClient) WriteFile(ctx context.Context, file string, content io.ReadSeeker) error {
	return c.localStorage.WriteFile(file, content)
}

func (c *localClient) List(ctx context.Context, pattern string) ([]string, error) {
	return c.localStorage.List(pattern)
}

func (c *localClient) Delete(ctx context.Context, file string) error {
	return c.localStorage.Delete(file)
}

func (c *localClient) Stat(ctx context.Context, file string) (*blobspb.BlobStat, error) {
	return c.localStorage.Stat(file)
}

func (c *localClient) Seek(
	ctx context.Context, filename string, offset int64, whence int,
) (io.ReadCloser, error) {
	return c.localStorage.Seek(filename, offset, whence)
}

// BlobClientFactory creates a blob client based on the nodeID we are dialing.
type BlobClientFactory func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error)

// NewBlobClientFactory returns a BlobClientFactory
func NewBlobClientFactory(
	localNodeID roachpb.NodeID, dialer *nodedialer.Dialer, externalIODir string,
) BlobClientFactory {
	return func(ctx context.Context, dialing roachpb.NodeID) (BlobClient, error) {
		if dialing == 0 || localNodeID == dialing {
			return newLocalClient(externalIODir)
		}
		conn, err := dialer.Dial(ctx, dialing, rpc.DefaultClass)
		if err != nil {
			return nil, errors.Wrapf(err, "connecting to node %d", dialing)
		}
		return newRemoteClient(blobspb.NewBlobClient(conn)), nil
	}
}
