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

/*
Package blobs contains a gRPC service to be used for remote file access.

It is used for bulk file reads and writes to files on any CockroachDB node.
Each node will run a blob service, which serves the file access for files on
that node. Each node will also have a blob client, which uses the nodedialer
to connect to another node's blob service, and access its files. The blob client
is the point of entry to this service and it supports the `BlobClient` interface,
which includes the following functionalities:
  - ReadFile
  - WriteFile
  - List
  - Delete
  - Stat
*/
package blobs

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/blobs/blobspb"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

// Service implements the gRPC BlobService which exchanges bulk files between different nodes.
type Service struct {
	localStorage *LocalStorage
}

var _ blobspb.BlobServer = &Service{}

// NewBlobService instantiates a blob service server.
func NewBlobService(externalIODir string) (*Service, error) {
	localStorage, err := NewLocalStorage(externalIODir)
	return &Service{localStorage: localStorage}, err
}

// GetStream implements the gRPC service.
func (s *Service) GetStream(req *blobspb.GetRequest, stream blobspb.Blob_GetStreamServer) error {
	content, err := s.localStorage.ReadFile(req.Filename)
	if err != nil {
		return err
	}
	defer content.Close()
	return streamContent(stream, content)
}

// PutStream implements the gRPC service.
func (s *Service) PutStream(stream blobspb.Blob_PutStreamServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return errors.New("could not fetch metadata")
	}
	filename := md.Get("filename")
	if len(filename) < 1 || filename[0] == "" {
		return errors.New("no filename in metadata")
	}

	reader := newPutStreamReader(stream)
	defer reader.Close()

	append := md.Get("append")
	if len(append) < 1 || append[0] == "false" {
		err := s.localStorage.WriteFile(filename[0], reader)
		return err
	}
	err := s.localStorage.AppendWrite(filename[0], reader)

	return err
}

// List implements the gRPC service.
func (s *Service) List(
	ctx context.Context, req *blobspb.GlobRequest,
) (*blobspb.GlobResponse, error) {
	matches, err := s.localStorage.List(req.Pattern)
	return &blobspb.GlobResponse{Files: matches}, err
}

// Delete implements the gRPC service.
func (s *Service) Delete(
	ctx context.Context, req *blobspb.DeleteRequest,
) (*blobspb.DeleteResponse, error) {
	return &blobspb.DeleteResponse{}, s.localStorage.Delete(req.Filename)
}

// Stat implements the gRPC service.
func (s *Service) Stat(ctx context.Context, req *blobspb.StatRequest) (*blobspb.BlobStat, error) {
	return s.localStorage.Stat(req.Filename)
}

// Seek implements the gRPC service.
func (s *Service) Seek(req *blobspb.SeekRequest, stream blobspb.Blob_SeekServer) error {
	content, err := s.localStorage.Seek(req.Filename, req.Offset, int(req.Whence))
	if err != nil {
		return err
	}
	defer content.Close()
	return streamContent(stream, content)
}
