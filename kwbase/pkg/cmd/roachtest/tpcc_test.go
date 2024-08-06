// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func TestTPCCSupportedWarehouses(t *testing.T) {
	const expectPanic = -1
	tests := []struct {
		cloud        string
		spec         clusterSpec
		buildVersion *version.Version
		expected     int
	}{
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`V2.1.0`), 1300},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`V19.1.0-rc.1`), 1250},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`V19.1.0`), 1250},

		{"aws", makeClusterSpec(4, cpu(16)), version.MustParse(`V19.1.0-rc.1`), 2100},
		{"aws", makeClusterSpec(4, cpu(16)), version.MustParse(`V19.1.0`), 2100},

		{"nope", makeClusterSpec(4, cpu(16)), version.MustParse(`V2.1.0`), expectPanic},
		{"gce", makeClusterSpec(5, cpu(160)), version.MustParse(`V2.1.0`), expectPanic},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`V1.0.0`), expectPanic},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			r := &testRunner{buildVersion: *test.buildVersion}
			if test.expected == expectPanic {
				require.Panics(t, func() {
					w := maxSupportedTPCCWarehouses(r.buildVersion, test.cloud, test.spec)
					t.Errorf("%s %s got unexpected result %d", test.cloud, &test.spec, w)
				})
			} else {
				require.Equal(t, test.expected, maxSupportedTPCCWarehouses(r.buildVersion, test.cloud, test.spec))
			}
		})
	}
}
