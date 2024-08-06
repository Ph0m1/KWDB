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

package tree

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestCastToCollatedString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cases := []struct {
		typ      *types.T
		contents string
	}{
		{types.MakeCollatedString(types.String, "de"), "test"},
		{types.MakeCollatedString(types.String, "en"), "test"},
		{types.MakeCollatedString(types.MakeString(5), "en"), "test"},
		{types.MakeCollatedString(types.MakeString(4), "en"), "test"},
		{types.MakeCollatedString(types.MakeString(3), "en"), "tes"},
	}
	for _, cas := range cases {
		t.Run("", func(t *testing.T) {
			expr := &CastExpr{Expr: NewDString("test"), Type: cas.typ, SyntaxMode: CastShort}
			typedexpr, err := expr.TypeCheck(&SemaContext{}, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			val, err := typedexpr.Eval(evalCtx)
			if err != nil {
				t.Fatal(err)
			}
			switch v := val.(type) {
			case *DCollatedString:
				if v.Locale != cas.typ.Locale() {
					t.Errorf("expected locale %q but got %q", cas.typ.Locale(), v.Locale)
				}
				if v.Contents != cas.contents {
					t.Errorf("expected contents %q but got %q", cas.contents, v.Contents)
				}
			default:
				t.Errorf("expected type *DCollatedString but got %T", v)
			}
		})
	}
}
