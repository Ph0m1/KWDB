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

package row

import (
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/span"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// spanForValues produce access spans for a single FK constraint and a
// tuple of columns.
func (f fkExistenceCheckBaseHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	if values == nil {
		key := roachpb.Key(f.spanBuilder.KeyPrefix)
		return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
	}
	return FKCheckSpan(f.spanBuilder, values, f.ids, f.prefixLen)
}

// FKCheckSpan returns a span that can be scanned to ascertain existence of a
// specific row in a given index.
func FKCheckSpan(
	s *span.Builder, values []tree.Datum, colMap map[sqlbase.ColumnID]int, numCols int,
) (roachpb.Span, error) {
	span, containsNull, err := s.SpanFromDatumRow(values, numCols, colMap)
	if err != nil {
		return roachpb.Span{}, err
	}
	// If it is safe to split this lookup into multiple families, generate a point lookup for
	// family 0. Because we are just checking for existence, we only need family 0.
	if s.CanSplitSpanIntoSeparateFamilies(1 /* numNeededFamilies */, numCols, containsNull) {
		return s.SpanToPointSpan(span, 0), nil
	}
	return span, nil
}
