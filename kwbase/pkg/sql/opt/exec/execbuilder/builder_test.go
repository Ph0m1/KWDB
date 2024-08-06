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

package execbuilder_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/logictest"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestExecBuild runs logic tests that are specific to how the optimizer builds
// queries.
//
// The test files should use combinations of the local, fakedist and
// 5node configs. For tests that only have EXPLAIN (PLAN) statements,
// it's sufficient to run on a single configuration.
func TestExecBuild(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer sql.TestingOverrideExplainEnvVersion("CockroachDB execbuilder test version")()
	t.Skip()
	logictest.RunLogicTest(t, logictest.TestServerArgs{
		// Several test files in execbuilder verify that mutations behave as
		// expected; however, if we add the randomization of the mutations max
		// batch size, then the output becomes non-deterministic, so we disable
		// that randomization.
		DisableMutationsMaxBatchSizeRandomization: true,
	}, "testdata/[^.]*")
}
