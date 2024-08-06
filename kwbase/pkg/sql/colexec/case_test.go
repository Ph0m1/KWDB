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

package colexec

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/apd"
)

func TestCaseOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	spec := &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{{}},
		Core: execinfrapb.ProcessorCoreUnion{
			Noop: &execinfrapb.NoopCoreSpec{},
		},
		Post: execinfrapb.PostProcessSpec{
			RenderExprs: []execinfrapb.Expression{{}},
		},
	}

	decs := make([]apd.Decimal, 2)
	decs[0].SetInt64(0)
	decs[1].SetInt64(1)
	zero := decs[0]
	one := decs[1]

	for _, tc := range []struct {
		tuples     tuples
		renderExpr string
		expected   tuples
		inputTypes []types.T
	}{
		{
			// Basic test.
			tuples:     tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 0 END",
			expected:   tuples{{0}, {1}, {0}, {0}},
			inputTypes: []types.T{*types.Int},
		},
		{
			// Test "reordered when's."
			tuples:     tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   tuples{{2}, {1}, {2}, {0}},
			inputTypes: []types.T{*types.Int, *types.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::DECIMAL WHEN @1 / @2 = 1 THEN 1::DECIMAL END",
			expected:   tuples{{nil}, {zero}, {nil}, {one}},
			inputTypes: []types.T{*types.Int, *types.Int},
		},
	} {
		runTests(t, []tuples{tc.tuples}, tc.expected, orderedVerifier, func(inputs []Operator) (Operator, error) {
			spec.Input[0].ColumnTypes = tc.inputTypes
			spec.Post.RenderExprs[0].Expr = tc.renderExpr
			args := NewColOperatorArgs{
				Spec:                spec,
				Inputs:              inputs,
				StreamingMemAccount: testMemAcc,
			}
			args.TestingKnobs.UseStreamingMemAccountForBuffering = true
			result, err := NewColOperator(ctx, flowCtx, args)
			if err != nil {
				return nil, err
			}
			return result.Op, nil
		})
	}
}
