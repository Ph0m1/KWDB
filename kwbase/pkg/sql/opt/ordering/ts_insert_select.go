// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

// if select clause contains order clause when we use insert into ... select ...
// we need to use the functions in this file to handle this situation.

package ordering

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

// insert into ... select ... must pass through ordering to its input.
func tsInsertSelectCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	return true
}

// the ordering that must be imposed on its given child in order to satisfy the required ordering.
func tsInsertSelectBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	return *required
}

// We need to handle the input of insert into ... select ...
// in order to make the best-effort attempt to simplify the provided orderings
// as much as possible (while still satisfying the required ordering).
func tsInsertSelectBuildProvided(
	expr memo.RelExpr, required *physical.OrderingChoice,
) opt.Ordering {
	return BuildProvided(expr.(*memo.TSInsertSelectExpr).Input, required)
}
