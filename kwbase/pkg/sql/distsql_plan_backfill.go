// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/pkg/errors"
)

func initBackfillerSpec(
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
) (execinfrapb.BackfillerSpec, error) {
	ret := execinfrapb.BackfillerSpec{
		Table:       desc,
		Duration:    duration,
		ChunkSize:   chunkSize,
		OtherTables: otherTables,
		ReadAsOf:    readAsOf,
	}
	switch backfillType {
	case indexBackfill:
		ret.Type = execinfrapb.BackfillerSpec_Index
	case columnBackfill:
		ret.Type = execinfrapb.BackfillerSpec_Column
	default:
		return execinfrapb.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
	}
	return ret, nil
}

// createBackfiller generates a plan consisting of index/column backfiller
// processors, one for each node that has spans that we are reading. The plan is
// finalized.
func (dsp *DistSQLPlanner) createBackfiller(
	planCtx *PlanningCtx,
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	spans []roachpb.Span,
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
) (PhysicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize, otherTables, readAsOf)
	if err != nil {
		return PhysicalPlan{}, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, spans)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var p PhysicalPlan
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		ib := &execinfrapb.BackfillerSpec{}
		*ib = spec
		ib.Spans = make([]execinfrapb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			ib.Spans[j].Span = sp.Spans[j]
		}

		proc := physicalplan.Processor{
			Node: sp.Node,
			Spec: execinfrapb.ProcessorSpec{
				Core:   execinfrapb.ProcessorCoreUnion{Backfiller: ib},
				Output: []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
