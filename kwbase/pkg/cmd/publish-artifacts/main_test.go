// Copyright 2017 The Cockroach Authors.
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

package main

import (
	"os"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kr/pretty"
)

// Whether to run slow tests.
var slow bool

func init() {
	if err := os.Setenv("AWS_ACCESS_KEY_ID", "testing"); err != nil {
		panic(err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", "hunter2"); err != nil {
		panic(err)
	}
}

type recorder struct {
	reqs []s3.PutObjectInput
}

func (r *recorder) PutObject(req *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	r.reqs = append(r.reqs, *req)
	return &s3.PutObjectOutput{}, nil
}

func mockPutter(p s3putter) func() {
	origPutter := testableS3
	f := func() {
		testableS3 = origPutter
	}
	testableS3 = func() (s3putter, error) {
		return p, nil
	}
	return f
}

func TestMain(t *testing.T) {
	if !slow {
		t.Skip("only to be run manually via `./build/builder.sh go test -tags slow -timeout 1h -v ./pkg/cmd/publish-artifacts`")
	}
	r := &recorder{}
	undo := mockPutter(r)
	defer undo()

	shaPat := regexp.MustCompile(`[a-f0-9]{40}`)
	const shaStub = "<sha>"

	type testCase struct {
		Bucket, ContentDisposition, Key, WebsiteRedirectLocation, CacheControl string
	}
	exp := []testCase{
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=kwbase.darwin-amd64." + shaStub,
			Key:                "/kwbase/kwbase.darwin-amd64." + shaStub,
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/kwbase.darwin-amd64.LATEST",
			WebsiteRedirectLocation: "/kwbase/kwbase.darwin-amd64." + shaStub,
		},
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=kwbase.linux-gnu-amd64." + shaStub,
			Key:                "/kwbase/kwbase.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/kwbase.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/kwbase/kwbase.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=kwbase.race.linux-gnu-amd64." + shaStub,
			Key:                "/kwbase/kwbase.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/kwbase.race.linux-gnu-amd64.LATEST",
			WebsiteRedirectLocation: "/kwbase/kwbase.race.linux-gnu-amd64." + shaStub,
		},
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=kwbase.linux-musl-amd64." + shaStub,
			Key:                "/kwbase/kwbase.linux-musl-amd64." + shaStub,
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/kwbase.linux-musl-amd64.LATEST",
			WebsiteRedirectLocation: "/kwbase/kwbase.linux-musl-amd64." + shaStub,
		},
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=kwbase.windows-amd64." + shaStub + ".exe",
			Key:                "/kwbase/kwbase.windows-amd64." + shaStub + ".exe",
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/kwbase.windows-amd64.LATEST",
			WebsiteRedirectLocation: "/kwbase/kwbase.windows-amd64." + shaStub + ".exe",
		},
		{
			Bucket:             "kwbase",
			ContentDisposition: "attachment; filename=workload." + shaStub,
			Key:                "/kwbase/workload." + shaStub,
		},
		{
			Bucket:                  "kwbase",
			CacheControl:            "no-cache",
			Key:                     "kwbase/workload.LATEST",
			WebsiteRedirectLocation: "/kwbase/workload." + shaStub,
		},
		{
			Bucket: "binaries.kwbasedb.com",
			Key:    "kwbase-v42.42.42.src.tgz",
		},
		{
			Bucket:       "binaries.kwbasedb.com",
			CacheControl: "no-cache",
			Key:          "kwbase-latest.src.tgz",
		},
		{
			Bucket: "binaries.kwbasedb.com",
			Key:    "kwbase-v42.42.42.darwin-10.9-amd64.tgz",
		},
		{
			Bucket:       "binaries.kwbasedb.com",
			CacheControl: "no-cache",
			Key:          "kwbase-latest.darwin-10.9-amd64.tgz",
		},
		{
			Bucket: "binaries.kwbasedb.com",
			Key:    "kwbase-v42.42.42.linux-amd64.tgz",
		},
		{
			Bucket:       "binaries.kwbasedb.com",
			CacheControl: "no-cache",
			Key:          "kwbase-latest.linux-amd64.tgz",
		},
		{
			Bucket: "binaries.kwbasedb.com",
			Key:    "kwbase-v42.42.42.linux-musl-amd64.tgz",
		},
		{
			Bucket:       "binaries.kwbasedb.com",
			CacheControl: "no-cache",
			Key:          "kwbase-latest.linux-musl-amd64.tgz",
		},
		{
			Bucket: "binaries.kwbasedb.com",
			Key:    "kwbase-v42.42.42.windows-6.2-amd64.zip",
		},
		{
			Bucket:       "binaries.kwbasedb.com",
			CacheControl: "no-cache",
			Key:          "kwbase-latest.windows-6.2-amd64.zip",
		},
	}

	if err := os.Setenv("TC_BUILD_BRANCH", "master"); err != nil {
		t.Fatal(err)
	}
	main()

	if err := os.Setenv("TC_BUILD_BRANCH", "v42.42.42"); err != nil {
		t.Fatal(err)
	}
	*isRelease = true
	main()

	var acts []testCase
	for _, req := range r.reqs {
		var act testCase
		if req.Bucket != nil {
			act.Bucket = *req.Bucket
		}
		if req.ContentDisposition != nil {
			act.ContentDisposition = shaPat.ReplaceAllLiteralString(*req.ContentDisposition, shaStub)
		}
		if req.Key != nil {
			act.Key = shaPat.ReplaceAllLiteralString(*req.Key, shaStub)
		}
		if req.WebsiteRedirectLocation != nil {
			act.WebsiteRedirectLocation = shaPat.ReplaceAllLiteralString(*req.WebsiteRedirectLocation, shaStub)
		}
		if req.CacheControl != nil {
			act.CacheControl = *req.CacheControl
		}
		acts = append(acts, act)
	}

	for i := len(exp); i < len(acts); i++ {
		exp = append(exp, testCase{})
	}
	for i := len(acts); i < len(exp); i++ {
		acts = append(acts, testCase{})
	}

	if len(pretty.Diff(acts, exp)) > 0 {
		t.Error("diff(act, exp) is nontrivial")
		pretty.Ldiff(t, acts, exp)
	}
}
