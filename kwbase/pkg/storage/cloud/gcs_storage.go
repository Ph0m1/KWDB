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
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	gcs "cloud.google.com/go/storage"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/contextutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func gcsQueryParams(conf *roachpb.ExternalStorage_GCS) string {
	q := make(url.Values)
	if conf.Auth != "" {
		q.Set(AuthParam, conf.Auth)
	}
	if conf.Credentials != "" {
		q.Set(CredentialsParam, conf.Credentials)
	}
	if conf.BillingProject != "" {
		q.Set(GoogleBillingProjectParam, conf.BillingProject)
	}
	return q.Encode()
}

type gcsStorage struct {
	bucket   *gcs.BucketHandle
	client   *gcs.Client
	conf     *roachpb.ExternalStorage_GCS
	prefix   string
	settings *cluster.Settings
}

var _ ExternalStorage = &gcsStorage{}

func (g *gcsStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:          roachpb.ExternalStorageProvider_GoogleCloud,
		GoogleCloudConfig: g.conf,
	}
}

func makeGCSStorage(
	ctx context.Context,
	ioConf base.ExternalIOConfig,
	conf *roachpb.ExternalStorage_GCS,
	settings *cluster.Settings,
) (ExternalStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	const scope = gcs.ScopeReadWrite
	opts := []option.ClientOption{option.WithScopes(scope)}

	// "default": only use the key in the settings; error if not present.
	// "specified": the JSON object for authentication is given by the CREDENTIALS param.
	// "implicit": only use the environment data.
	// "": if default key is in the settings use it; otherwise use environment data.
	if ioConf.DisableImplicitCredentials && conf.Auth != authParamSpecified {
		return nil, errors.New(
			"implicit credentials disallowed for gs due to --external-io-disable-implicit-credentials flag")
	}

	switch conf.Auth {
	case "", authParamDefault:
		var key string
		if settings != nil {
			key = gcsDefault.Get(&settings.SV)
		}
		// We expect a key to be present if default is specified.
		if conf.Auth == authParamDefault && key == "" {
			return nil, errors.Errorf("expected settings value for %s", cloudstorageGSDefaultKey)
		}
		if key != "" {
			source, err := google.JWTConfigFromJSON([]byte(key), scope)
			if err != nil {
				return nil, errors.Wrap(err, "creating GCS oauth token source")
			}
			opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
		}
	case authParamSpecified:
		if conf.Credentials == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				AuthParam,
				authParamSpecified,
				CredentialsParam,
			)
		}
		decodedKey, err := base64.StdEncoding.DecodeString(conf.Credentials)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("decoding value of %s", CredentialsParam))
		}
		source, err := google.JWTConfigFromJSON(decodedKey, scope)
		if err != nil {
			return nil, errors.Wrap(err, "creating GCS oauth token source from specified credentials")
		}
		opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
	case authParamImplicit:
		// Do nothing; use implicit params:
		// https://godoc.org/golang.org/x/oauth2/google#FindDefaultCredentials
	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, AuthParam)
	}
	g, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create google cloud client")
	}
	bucket := g.Bucket(conf.Bucket)
	if conf.BillingProject != `` {
		bucket = bucket.UserProject(conf.BillingProject)
	}
	return &gcsStorage{
		bucket:   bucket,
		client:   g,
		conf:     conf,
		prefix:   conf.Prefix,
		settings: settings,
	}, nil
}

func (g *gcsStorage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	const maxAttempts = 3
	err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		if _, err := content.Seek(0, io.SeekStart); err != nil {
			return err
		}
		// Set the timeout within the retry loop.
		return contextutil.RunWithTimeout(ctx, "put gcs file", timeoutSetting.Get(&g.settings.SV),
			func(ctx context.Context) error {
				w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
				if _, err := io.Copy(w, content); err != nil {
					_ = w.Close()
					return err
				}
				return w.Close()
			})
	})
	return errors.Wrap(err, "write to google cloud")
}

func (g *gcsStorage) AppendWrite(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	return errors.New("gcsStorage does not support additional writing")
}

// resumingGoogleStorageReader is a GS reader which retries
// reads in case of a transient connection reset by peer error.
type resumingGoogleStorageReader struct {
	ctx    context.Context   // Reader context
	bucket *gcs.BucketHandle // Bucket to read the data from
	object string            // Object to read
	data   *gcs.Reader       // Currently opened object data stream
	pos    int64             // How much data was received so far
}

var _ io.ReadCloser = &resumingGoogleStorageReader{}

func (r *resumingGoogleStorageReader) openStream() error {
	return delayedRetry(r.ctx, func() error {
		var readErr error
		r.data, readErr = r.bucket.Object(r.object).NewRangeReader(r.ctx, r.pos, -1)
		return readErr
	})
}

func (r *resumingGoogleStorageReader) Read(p []byte) (int, error) {
	var lastErr error

	for retries := 0; lastErr == nil; retries++ {
		if r.data == nil {
			lastErr = r.openStream()
		}

		if lastErr == nil {
			n, readErr := r.data.Read(p)
			if readErr == nil {
				r.pos += int64(n)
				return n, nil
			}
			lastErr = readErr
		}

		if !errors.IsAny(lastErr, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(r.ctx, "GCS:Read err: %s", lastErr)
		}

		if isResumableHTTPError(lastErr) {
			if retries > maxNoProgressReads {
				return 0, errors.Wrap(lastErr, "multiple Read calls return no data")
			}
			log.Errorf(r.ctx, "GCS:Retry: error %s", lastErr)
			lastErr = nil
			r.data = nil
		}
	}

	return 0, lastErr
}

func (r *resumingGoogleStorageReader) Close() error {
	if r.data != nil {
		return r.data.Close()
	}
	return nil
}

func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://gitee.com/kwbasedb/kwbase/issues/23859
	reader := &resumingGoogleStorageReader{
		ctx:    ctx,
		bucket: g.bucket,
		object: path.Join(g.prefix, basename),
	}
	if err := reader.openStream(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (g *gcsStorage) Seek(
	ctx context.Context, filename string, offset int64, endOffset int64, whence int,
) (io.ReadCloser, error) {
	return nil, errors.New("gcsStorage does not support seek")
}

func (g *gcsStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string
	it := g.bucket.Objects(ctx, &gcs.Query{
		Prefix: getPrefixBeforeWildcard(g.prefix),
	})

	pattern := g.prefix
	if patternSuffix != "" {
		if containsGlob(g.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to list files in gcs bucket")
		}

		matches, errMatch := path.Match(pattern, attrs.Name)
		if errMatch != nil {
			continue
		}
		if matches {
			if patternSuffix != "" {
				if !strings.HasPrefix(attrs.Name, g.prefix) {
					// TODO(dt): return a nice rel-path instead of erroring out.
					return nil, errors.New("pattern matched file outside of path")
				}
				fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(attrs.Name, g.prefix), "/"))
			} else {
				gsURL := url.URL{
					Scheme:   "gs",
					Host:     attrs.Bucket,
					Path:     attrs.Name,
					RawQuery: gcsQueryParams(g.conf),
				}
				fileList = append(fileList, gsURL.String())
			}
		}
	}

	return fileList, nil
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, "delete gcs file",
		timeoutSetting.Get(&g.settings.SV),
		func(ctx context.Context) error {
			return g.bucket.Object(path.Join(g.prefix, basename)).Delete(ctx)
		})
}

func (g *gcsStorage) Size(ctx context.Context, basename string) (int64, error) {
	var r *gcs.Reader
	if err := contextutil.RunWithTimeout(ctx, "size gcs file",
		timeoutSetting.Get(&g.settings.SV),
		func(ctx context.Context) error {
			var err error
			r, err = g.bucket.Object(path.Join(g.prefix, basename)).NewReader(ctx)
			return err
		}); err != nil {
		return 0, err
	}
	sz := r.Attrs.Size
	_ = r.Close()
	return sz, nil
}

func (g *gcsStorage) Close() error {
	return g.client.Close()
}
