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

package norm_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/opttester"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/datadriven"
)

// TestNormRules tests the various Optgen normalization rules found in the rules
// directory. The tests are data-driven cases of the form:
//
//	<command>
//	<SQL statement>
//	----
//	<expected results>
//
// See OptTester.Handle for supported commands.
//
// Rules files can be run separately like this:
//
//	make test PKG=./pkg/sql/opt/norm TESTS="TestNormRules/bool"
//	make test PKG=./pkg/sql/opt/norm TESTS="TestNormRules/comp"
//	...
func TestNormRules(t *testing.T) {
	const fmtFlags = memo.ExprFmtHideStats | memo.ExprFmtHideCost | memo.ExprFmtHideRuleProps |
		memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes
	datadriven.Walk(t, "testdata/rules", func(t *testing.T, path string) {
		catalog := testcat.New()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			tester := opttester.New(catalog, d.Input)
			tester.Flags.ExprFormat = fmtFlags
			return tester.RunCommand(t, d)
		})
	})
}

// Ensure that every binary commutative operator overload can have its operands
// switched. Patterns like CommuteConst rely on this being possible.
func TestRuleBinaryAssumption(t *testing.T) {
	fn := func(op opt.Operator) {
		for _, overload := range tree.BinOps[opt.BinaryOpReverseMap[op]] {
			binOp := overload.(*tree.BinOp)
			if !memo.BinaryOverloadExists(op, binOp.RightType, binOp.LeftType) {
				t.Errorf("could not find inverse for overload: %+v", op)
			}
		}
	}

	// Only include commutative binary operators.
	fn(opt.PlusOp)
	fn(opt.MultOp)
	fn(opt.BitandOp)
	fn(opt.BitorOp)
	fn(opt.BitxorOp)
}

// These constants are copied from sql/sem/builtins/builtins.go.
const (
	categorySystemInfo  = "System info"
	categoryDateAndTime = "Date and time"
)

// TestRuleFunctionAssumption checks that we do not fold impure functions.
// There are other functions we should not fold such as current_user() because
// they depend on context, but it is very difficult to test that we avoid all
// such cases. Instead, we rely on clues like the category.
func TestRuleFunctionAssumption(t *testing.T) {
	for name := range norm.FoldFunctionWhitelist {
		props, _ := builtins.GetBuiltinProperties(name)
		if props == nil {
			t.Errorf("could not find properties for function %s", name)
			continue
		}
		if props.Impure {
			t.Errorf("%s should not be folded because it is impure", name)
		}
		if props.Category == categorySystemInfo || props.Category == categoryDateAndTime {
			switch name {
			case "kwdb_internal.locality_value", "experimental_strftime", "experimental_strptime",
				"extract", "date_trunc", "extract_duration", "timezone":
				// OK to fold this function.

			default:
				t.Errorf("%s should not be folded because it has category %s", name, props.Category)
			}
		}
	}
}
