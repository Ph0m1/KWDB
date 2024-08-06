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
	"net/url"
	"os"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/blobs"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/stretchr/testify/require"
)

func TestPutS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skip("No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	ctx := context.TODO()
	t.Run("auth-empty-no-cred", func(t *testing.T) {
		_, err := ExternalStorageFromURI(
			ctx, fmt.Sprintf("s3://%s/%s", bucket, "backup-test-default"),
			base.ExternalIOConfig{}, testSettings, blobs.TestEmptyBlobClientFactory,
		)
		require.EqualError(t, err, fmt.Sprintf(
			`%s is set to '%s', but %s is not set`,
			AuthParam,
			authParamSpecified,
			S3AccessKeyParam,
		))
	})
	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			t.Skip(err)
		}

		testExportStore(
			t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s",
				bucket, "backup-test-default",
				AuthParam, authParamImplicit,
			),
			false,
		)
	})

	t.Run("auth-specified", func(t *testing.T) {
		testExportStore(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "backup-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
			false,
		)
		testListFiles(t,
			fmt.Sprintf(
				"s3://%s/%s?%s=%s&%s=%s",
				bucket, "listing-test",
				S3AccessKeyParam, url.QueryEscape(creds.AccessKeyID),
				S3SecretParam, url.QueryEscape(creds.SecretAccessKey),
			),
		)
	})
}

func TestPutS3Endpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := make(url.Values)
	expect := map[string]string{
		"AWS_S3_ENDPOINT":        S3EndpointParam,
		"AWS_S3_ENDPOINT_KEY":    S3AccessKeyParam,
		"AWS_S3_ENDPOINT_REGION": S3RegionParam,
		"AWS_S3_ENDPOINT_SECRET": S3SecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			t.Skipf("%s env var must be set", env)
		}
		q.Add(param, v)
	}

	bucket := os.Getenv("AWS_S3_ENDPOINT_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_ENDPOINT_BUCKET env var must be set")
	}

	u := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     "backup-test",
		RawQuery: q.Encode(),
	}

	testExportStore(t, u.String(), false)
}

func TestS3DisallowCustomEndpoints(t *testing.T) {
	s3, err := makeS3Storage(context.TODO(),
		base.ExternalIOConfig{DisableHTTP: true},
		&roachpb.ExternalStorage_S3{Endpoint: "http://do.not.go.there/"}, nil,
	)
	require.Nil(t, s3)
	require.Error(t, err)
}

func TestS3DisallowImplicitCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s3, err := makeS3Storage(context.TODO(),
		base.ExternalIOConfig{DisableImplicitCredentials: true},
		&roachpb.ExternalStorage_S3{
			Endpoint: "http://do-not-go-there",
			Auth:     authParamImplicit,
		}, testSettings,
	)
	require.Nil(t, s3)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "implicit"))
}
