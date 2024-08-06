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
	"io"
	"path"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/blobs"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

type localFileStorage struct {
	cfg        roachpb.ExternalStorage_LocalFilePath // contains un-prefixed filepath -- DO NOT use for I/O ops.
	base       string                                // relative filepath prefixed with externalIODir, for I/O ops on this node.
	blobClient blobs.BlobClient                      // inter-node file sharing service
}

var _ ExternalStorage = &localFileStorage{}

// MakeLocalStorageURI converts a local path (should always be relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) string {
	return fmt.Sprintf("nodelocal://01/%s", path)
}

func makeNodeLocalURIWithNodeID(nodeID roachpb.NodeID, path string) string {
	path = strings.TrimPrefix(path, "/")
	return fmt.Sprintf("nodelocal://%d/%s", nodeID, path)
}

func makeLocalStorage(
	ctx context.Context,
	cfg roachpb.ExternalStorage_LocalFilePath,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
) (ExternalStorage, error) {
	if cfg.Path == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	client, err := blobClientFactory(ctx, cfg.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create blob client")
	}
	return &localFileStorage{base: cfg.Path, cfg: cfg, blobClient: client}, nil
}

func (l *localFileStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:  roachpb.ExternalStorageProvider_LocalFile,
		LocalFile: l.cfg,
	}
}

func joinRelativePath(filePath string, file string) string {
	// Joining "." to make this a relative path.
	// This ensures path.Clean does not simplify in unexpected ways.
	return path.Join(".", filePath, file)
}

func (l *localFileStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return l.blobClient.WriteFile(ctx, joinRelativePath(l.base, basename), content)
}

func (l *localFileStorage) AppendWrite(
	ctx context.Context, file string, content io.ReadSeeker,
) error {
	return l.blobClient.AppendWrite(ctx, joinRelativePath(l.base, file), content)
}

func (l *localFileStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	return l.blobClient.ReadFile(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return l.blobClient.Seek(ctx, joinRelativePath(l.base, filename), offset, whence)
}

func (l *localFileStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	pattern := l.base
	if patternSuffix != "" {
		if containsGlob(l.base) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = joinRelativePath(pattern, patternSuffix)
		l.base = path.Clean(l.base)
	}

	var fileList []string
	matches, err := l.blobClient.List(ctx, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "unable to match pattern provided")
	}

	for _, fileName := range matches {
		if patternSuffix != "" {
			fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(fileName, l.base), "/"))
		} else {
			fileList = append(fileList, makeNodeLocalURIWithNodeID(l.cfg.NodeID, fileName))
		}
	}
	return fileList, nil
}

func (l *localFileStorage) Delete(ctx context.Context, basename string) error {
	return l.blobClient.Delete(ctx, joinRelativePath(l.base, basename))
}

func (l *localFileStorage) Size(ctx context.Context, basename string) (int64, error) {
	stat, err := l.blobClient.Stat(ctx, joinRelativePath(l.base, basename))
	if err != nil {
		return 0, err
	}
	return stat.Filesize, nil
}

func (*localFileStorage) Close() error {
	return nil
}
