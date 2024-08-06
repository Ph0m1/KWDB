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
	"net/url"
	"path"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/cockroachdb/errors"
)

func azureQueryParams(conf *roachpb.ExternalStorage_Azure) string {
	q := make(url.Values)
	if conf.AccountName != "" {
		q.Set(AzureAccountNameParam, conf.AccountName)
	}
	if conf.AccountKey != "" {
		q.Set(AzureAccountKeyParam, conf.AccountKey)
	}
	return q.Encode()
}

type azureStorage struct {
	conf      *roachpb.ExternalStorage_Azure
	container azblob.ContainerURL
	prefix    string
	settings  *cluster.Settings
}

var _ ExternalStorage = &azureStorage{}

func makeAzureStorage(
	conf *roachpb.ExternalStorage_Azure, settings *cluster.Settings,
) (ExternalStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("azure upload requested but info missing")
	}
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, errors.Wrap(err, "azure credential")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName))
	if err != nil {
		return nil, errors.Wrap(err, "azure: account name is not valid")
	}
	serviceURL := azblob.NewServiceURL(*u, p)
	return &azureStorage{
		conf:      conf,
		container: serviceURL.NewContainerURL(conf.Container),
		prefix:    conf.Prefix,
		settings:  settings,
	}, nil
}

func (s *azureStorage) getBlob(basename string) azblob.BlockBlobURL {
	name := path.Join(s.prefix, basename)
	return s.container.NewBlockBlobURL(name)
}

func (s *azureStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:    roachpb.ExternalStorageProvider_Azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	err := contextutil.RunWithTimeout(ctx, "write azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Upload(
				ctx, content, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{},
			)
			return err
		})
	return errors.Wrapf(err, "write file: %s", basename)
}

func (s *azureStorage) AppendWrite(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return errors.New("azure does not support additional writing")
}

func (s *azureStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://gitee.com/kwbasedb/kwbase/issues/23859
	blob := s.getBlob(basename)
	get, err := blob.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create azure reader")
	}
	reader := get.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})
	return reader, nil
}

func (s *azureStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.New("azure does not support seek")
}

func (s *azureStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	pattern := s.prefix
	if patternSuffix != "" {
		if containsGlob(s.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}
	var fileList []string
	response, err := s.container.ListBlobsFlatSegment(ctx,
		azblob.Marker{},
		azblob.ListBlobsSegmentOptions{Prefix: getPrefixBeforeWildcard(s.prefix)},
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list files for specified blob")
	}

	for _, blob := range response.Segment.BlobItems {
		matches, err := path.Match(pattern, blob.Name)
		if err != nil {
			continue
		}
		if matches {
			azureURL := url.URL{
				Scheme:   "azure",
				Host:     strings.TrimPrefix(s.container.URL().Path, "/"),
				Path:     blob.Name,
				RawQuery: azureQueryParams(s.conf),
			}
			fileList = append(fileList, azureURL.String())
		}
	}

	return fileList, nil
}

func (s *azureStorage) Delete(ctx context.Context, basename string) error {
	err := contextutil.RunWithTimeout(ctx, "delete azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props *azblob.BlobGetPropertiesResponse
	err := contextutil.RunWithTimeout(ctx, "size azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			var err error
			props, err = blob.GetProperties(ctx, azblob.BlobAccessConditions{})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "get file properties")
	}
	return props.ContentLength(), nil
}

func (s *azureStorage) Close() error {
	return nil
}
