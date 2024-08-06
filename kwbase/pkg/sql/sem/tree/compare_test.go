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

func TestEvalComparisonExprCaching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testExprs := []struct {
		op          ComparisonOperator
		left, right string
		cacheCount  int
	}{
		// Comparisons.
		{EQ, `0`, `1`, 0},
		{LT, `0`, `1`, 0},
		// LIKE and NOT LIKE
		{Like, `TEST`, `T%T`, 1},
		{NotLike, `TEST`, `%E%T`, 1},
		// ILIKE and NOT ILIKE
		{ILike, `TEST`, `T%T`, 1},
		{NotILike, `TEST`, `%E%T`, 1},
		// SIMILAR TO and NOT SIMILAR TO
		{SimilarTo, `abc`, `(b|c)%`, 1},
		{NotSimilarTo, `abc`, `%(b|d)%`, 1},
		// ~, !~, ~*, and !~*
		{RegMatch, `abc`, `(b|c).`, 1},
		{NotRegMatch, `abc`, `(b|c).`, 1},
		{RegIMatch, `abc`, `(b|c).`, 1},
		{NotRegIMatch, `abc`, `(b|c).`, 1},
	}
	for _, d := range testExprs {
		expr := &ComparisonExpr{
			Operator: d.op,
			Left:     NewDString(d.left),
			Right:    NewDString(d.right),
		}
		ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer ctx.Mon.Stop(context.Background())
		ctx.ReCache = NewRegexpCache(8)
		typedExpr, err := TypeCheck(expr, nil, types.Any)
		if err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if _, err := typedExpr.Eval(ctx); err != nil {
			t.Fatalf("%v: %v", d, err)
		}
		if typedExpr.(*ComparisonExpr).fn.Fn == nil {
			t.Errorf("%s: expected the comparison function to be looked up and memoized, but it wasn't", expr)
		}
		if count := ctx.ReCache.Len(); count != d.cacheCount {
			t.Errorf("%s: expected regular expression cache to contain %d compiled patterns, but found %d", expr, d.cacheCount, count)
		}
	}
}
