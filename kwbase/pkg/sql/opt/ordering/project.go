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

package ordering

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

func projectCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Project can pass through its ordering if the ordering depends only on
	// columns present in the input.
	proj := expr.(*memo.ProjectExpr)
	inputCols := proj.Input.Relational().OutputCols

	if required.CanProjectCols(inputCols) {
		return true
	}

	// We may be able to "remap" columns using the internal FD set.
	if fdSet := proj.InternalFDs(); required.CanSimplify(fdSet) {
		simplified := required.Copy()
		simplified.Simplify(fdSet)
		return simplified.CanProjectCols(inputCols)
	}

	return false
}

func projectBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}

	// Project can prune input columns, which can cause its FD set to be
	// pruned as well. Check the ordering to see if it can be simplified
	// with respect to the internal FD set.
	proj := parent.(*memo.ProjectExpr)
	simplified := *required
	if fdSet := proj.InternalFDs(); simplified.CanSimplify(fdSet) {
		simplified = simplified.Copy()
		simplified.Simplify(fdSet)
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	result := projectOrderingToInput(proj.Input, &simplified)

	return result
}

// projectOrderingToInput projects out columns from an ordering (if necessary);
// can only be used if the ordering can be expressed in terms of the input
// columns. If projection is not necessary, returns a shallow copy of the
// ordering.
func projectOrderingToInput(
	input memo.RelExpr, ordering *physical.OrderingChoice,
) physical.OrderingChoice {
	childOutCols := input.Relational().OutputCols
	if ordering.SubsetOfCols(childOutCols) {
		return *ordering
	}
	result := ordering.Copy()
	result.ProjectCols(childOutCols)
	return result
}

func projectBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	p := expr.(*memo.ProjectExpr)
	// Project can only satisfy required orderings that refer to projected
	// columns; it should always be possible to remap the columns in the input's
	// provided ordering.
	return remapProvided(
		p.Input.ProvidedPhysical().Ordering,
		p.InternalFDs(),
		p.Relational().OutputCols,
	)
}
