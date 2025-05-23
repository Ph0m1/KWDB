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
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/colcontainerutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

type windowFnTestCase struct {
	tuples       []tuple
	expected     []tuple
	windowerSpec execinfrapb.WindowerSpec
}

func (tc *windowFnTestCase) init() {
	for i := range tc.windowerSpec.WindowFns {
		tc.windowerSpec.WindowFns[i].FilterColIdx = noFilterIdx
	}
}

func TestWindowFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	evalCtx.SessionData.VectorizeMode = sessiondata.VectorizeOn
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DiskMonitor: testDiskMonitor,
		},
	}
	// All supported window function operators will use from 0 to 3 disk queues
	// with each using a single FD at any point in time. Additionally, the
	// disk-backed sorter (that will be planned depending on PARTITION BY and
	// ORDER BY combinations) will be limited to this number using a testing
	// knob, so 3 is necessary and sufficient.
	const maxNumberFDs = 3
	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	rowNumberFn := execinfrapb.WindowerSpec_ROW_NUMBER
	rankFn := execinfrapb.WindowerSpec_RANK
	denseRankFn := execinfrapb.WindowerSpec_DENSE_RANK
	percentRankFn := execinfrapb.WindowerSpec_PERCENT_RANK
	cumeDistFn := execinfrapb.WindowerSpec_CUME_DIST
	accounts := make([]*mon.BoundAccount, 0)
	monitors := make([]*mon.BytesMonitor, 0)
	for _, spillForced := range []bool{false, true} {
		flowCtx.Cfg.TestingKnobs.ForceDiskSpill = spillForced
		for _, tc := range []windowFnTestCase{
			// With PARTITION BY, no ORDER BY.
			//
			// Without ORDER BY, the output of row_number is non-deterministic, so we
			// skip such a case for rowNumberFn.
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1}, {nil, 1}, {1, 1}, {1, 1}, {2, 1}, {3, 1}, {3, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 0}, {nil, 0}, {1, 0}, {1, 0}, {2, 0}, {3, 0}, {3, 0}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1.0}, {nil, 1.0}, {1, 1.0}, {1, 1.0}, {2, 1.0}, {3, 1.0}, {3, 1.0}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							OutputColIdx: 1,
						},
					},
				},
			},

			// No PARTITION BY, with ORDER BY.
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1}, {nil, 2}, {1, 3}, {1, 4}, {2, 5}, {3, 6}, {3, 7}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1}, {nil, 1}, {1, 3}, {1, 3}, {2, 5}, {3, 6}, {3, 6}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 1}, {nil, 1}, {1, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 4}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 0}, {nil, 0}, {1, 2.0 / 7}, {1, 2.0 / 7}, {1, 2.0 / 7}, {2, 5.0 / 7}, {3, 6.0 / 7}, {3, 6.0 / 7}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},
			{
				tuples:   tuples{{3}, {1}, {2}, {1}, {nil}, {1}, {nil}, {3}},
				expected: tuples{{nil, 2.0 / 8}, {nil, 2.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {1, 5.0 / 8}, {2, 6.0 / 8}, {3, 1.0}, {3, 1.0}},
				windowerSpec: execinfrapb.WindowerSpec{
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 0}}},
							OutputColIdx: 1,
						},
					},
				},
			},

			// With both PARTITION BY and ORDER BY.
			{
				tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 1}, {nil, nil, 2}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rowNumberFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 3}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &rankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   tuples{{3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 1}, {nil, nil, 1}, {nil, 1, 2}, {1, nil, 1}, {1, 2, 2}, {2, 1, 1}, {3, 1, 1}, {3, 2, 2}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &denseRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 0}, {nil, nil, 0}, {nil, 1, 2.0 / 3}, {nil, 2, 1}, {1, nil, 0}, {1, 2, 1.0 / 2}, {1, 3, 1}, {2, 1, 0}, {3, 1, 0}, {3, 2, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &percentRankFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
			{
				tuples:   tuples{{nil, 2}, {3, 2}, {1, nil}, {2, 1}, {nil, nil}, {1, 2}, {nil, 1}, {1, 3}, {nil, nil}, {3, 1}},
				expected: tuples{{nil, nil, 2.0 / 4}, {nil, nil, 2.0 / 4}, {nil, 1, 3.0 / 4}, {nil, 2, 1}, {1, nil, 1.0 / 3}, {1, 2, 2.0 / 3}, {1, 3, 1}, {2, 1, 1}, {3, 1, 1.0 / 2}, {3, 2, 1}},
				windowerSpec: execinfrapb.WindowerSpec{
					PartitionBy: []uint32{0},
					WindowFns: []execinfrapb.WindowerSpec_WindowFn{
						{
							Func:         execinfrapb.WindowerSpec_Func{WindowFunc: &cumeDistFn},
							Ordering:     execinfrapb.Ordering{Columns: []execinfrapb.Ordering_Column{{ColIdx: 1}}},
							OutputColIdx: 2,
						},
					},
				},
			},
		} {
			t.Run(fmt.Sprintf("spillForced=%t/%s", spillForced, tc.windowerSpec.WindowFns[0].Func.String()), func(t *testing.T) {
				var semsToCheck []semaphore.Semaphore
				runTests(t, []tuples{tc.tuples}, tc.expected, unorderedVerifier, func(inputs []Operator) (Operator, error) {
					tc.init()
					ct := make([]types.T, len(tc.tuples[0]))
					for i := range ct {
						ct[i] = *types.Int
					}
					spec := &execinfrapb.ProcessorSpec{
						Input: []execinfrapb.InputSyncSpec{{ColumnTypes: ct}},
						Core: execinfrapb.ProcessorCoreUnion{
							Windower: &tc.windowerSpec,
						},
					}
					sem := NewTestingSemaphore(maxNumberFDs)
					args := NewColOperatorArgs{
						Spec:                spec,
						Inputs:              inputs,
						StreamingMemAccount: testMemAcc,
						DiskQueueCfg:        queueCfg,
						FDSemaphore:         sem,
					}
					semsToCheck = append(semsToCheck, sem)
					args.TestingKnobs.UseStreamingMemAccountForBuffering = true
					args.TestingKnobs.NumForcedRepartitions = maxNumberFDs
					result, err := NewColOperator(ctx, flowCtx, args)
					accounts = append(accounts, result.OpAccounts...)
					monitors = append(monitors, result.OpMonitors...)
					return result.Op, err
				})
				for i, sem := range semsToCheck {
					require.Equal(t, 0, sem.GetCount(), "sem still reports open FDs at index %d", i)
				}
			})
		}
	}

	for _, acc := range accounts {
		acc.Close(ctx)
	}

	for _, m := range monitors {
		m.Stop(ctx)
	}
}
