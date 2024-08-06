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

package rowexec

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/mon"
	"github.com/pkg/errors"
)

// streamGroupAccumulator groups input rows coming from src into groups dictated
// by equality according to the ordering columns.
type streamGroupAccumulator struct {
	src   execinfra.RowSource
	types []types.T

	// srcConsumed is set once src has been exhausted.
	srcConsumed bool
	ordering    sqlbase.ColumnOrdering

	// curGroup maintains the rows accumulated in the current group.
	curGroup   []sqlbase.EncDatumRow
	datumAlloc sqlbase.DatumAlloc

	// leftoverRow is the first row of the next group. It's saved in the
	// accumulator after the current group is returned, so the accumulator can
	// resume later.
	leftoverRow sqlbase.EncDatumRow

	rowAlloc sqlbase.EncDatumRowAlloc

	memAcc mon.BoundAccount
}

func makeStreamGroupAccumulator(
	src execinfra.RowSource, ordering sqlbase.ColumnOrdering, memMonitor *mon.BytesMonitor,
) streamGroupAccumulator {
	return streamGroupAccumulator{
		src:      src,
		types:    src.OutputTypes(),
		ordering: ordering,
		memAcc:   memMonitor.MakeBoundAccount(),
	}
}

func (s *streamGroupAccumulator) start(ctx context.Context) {
	s.src.Start(ctx)
}

// nextGroup returns the next group from the inputs. The returned slice is not safe
// to use after the next call to nextGroup.
func (s *streamGroupAccumulator) nextGroup(
	ctx context.Context, evalCtx *tree.EvalContext,
) ([]sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if s.srcConsumed {
		// If src has been exhausted, then we also must have advanced away from the
		// last group.
		return nil, nil
	}

	if s.leftoverRow != nil {
		s.curGroup = append(s.curGroup, s.leftoverRow)
		s.leftoverRow = nil
	}

	for {
		row, meta := s.src.Next()
		if meta != nil {
			return nil, meta
		}
		if row == nil {
			s.srcConsumed = true
			return s.curGroup, nil
		}

		if err := s.memAcc.Grow(ctx, int64(row.Size())); err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		row = s.rowAlloc.CopyRow(row)

		if len(s.curGroup) == 0 {
			if s.curGroup == nil {
				s.curGroup = make([]sqlbase.EncDatumRow, 0, 64)
			}
			s.curGroup = append(s.curGroup, row)
			continue
		}

		cmp, err := s.curGroup[0].Compare(s.types, &s.datumAlloc, s.ordering, evalCtx, row)
		if err != nil {
			return nil, &execinfrapb.ProducerMetadata{Err: err}
		}
		if cmp == 0 {
			s.curGroup = append(s.curGroup, row)
		} else if cmp == 1 {
			return nil, &execinfrapb.ProducerMetadata{
				Err: errors.Errorf(
					"detected badly ordered input: %s > %s, but expected '<'",
					s.curGroup[0].String(s.types), row.String(s.types)),
			}
		} else {
			n := len(s.curGroup)
			ret := s.curGroup[:n:n]
			s.curGroup = s.curGroup[:0]
			s.memAcc.Empty(ctx)
			s.leftoverRow = row
			return ret, nil
		}
	}
}

func (s *streamGroupAccumulator) close(ctx context.Context) {
	s.memAcc.Close(ctx)
}
