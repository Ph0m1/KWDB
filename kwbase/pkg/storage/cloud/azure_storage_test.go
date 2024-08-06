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
	"fmt"
	"net/url"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestPutAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		t.Skip("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY env vars must be set")
	}
	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		t.Skip("AZURE_CONTAINER env var must be set")
	}

	testExportStore(t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "backup-test",
			AzureAccountNameParam, url.QueryEscape(accountName),
			AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
		false,
	)
	testListFiles(
		t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "listing-test",
			AzureAccountNameParam, url.QueryEscape(accountName),
			AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
	)
}
