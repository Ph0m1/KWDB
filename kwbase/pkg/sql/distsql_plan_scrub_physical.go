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

package sql

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/physicalplan"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
)

// createScrubPhysicalCheck generates a plan for running a physical
// check for an index. The plan consists of TableReaders, with IsCheck
// enabled, that scan an index span. By having IsCheck enabled, the
// TableReaders will only emit errors encountered during scanning
// instead of row data. The plan is finalized.
func (dsp *DistSQLPlanner) createScrubPhysicalCheck(
	planCtx *PlanningCtx,
	n *scanNode,
	desc sqlbase.TableDescriptor,
	indexDesc sqlbase.IndexDescriptor,
	readAsOf hlc.Timestamp,
) (PhysicalPlan, error) {
	spec, _, err := initTableReaderSpec(n, planCtx, nil /* indexVarMap */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	spanPartitions, err := dsp.PartitionSpans(planCtx, n.spans)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var p PhysicalPlan
	stageID := p.NewStageID()
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		tr := &execinfrapb.TableReaderSpec{}
		*tr = *spec
		tr.Spans = make([]execinfrapb.TableReaderSpan, len(sp.Spans))
		for j := range sp.Spans {
			tr.Spans[j].Span = sp.Spans[j]
		}

		proc := physicalplan.Processor{
			Node: sp.Node,
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{TableReader: tr},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	// Set the plan's result types to be ScrubTypes.
	p.ResultTypes = rowexec.ScrubTypes
	p.PlanToStreamColMap = identityMapInPlace(make([]int, len(rowexec.ScrubTypes)))

	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
