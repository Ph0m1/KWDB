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
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// queueFkExistenceChecksForRow initiates FK existence checks for a
// given mutated row.
func queueFkExistenceChecksForRow(
	ctx context.Context,
	checkRunner *fkExistenceBatchChecker,
	mutatedIdxHelpers []fkExistenceCheckBaseHelper,
	mutatedRow tree.Datums,
	traceKV bool,
) error {
outer:
	for i, fk := range mutatedIdxHelpers {
		// See https://gitee.com/kwbasedb/kwbase/issues/20305 or
		// https://www.postgresql.org/docs/11/sql-createtable.html for details on the
		// different composite foreign key matching methods.
		//
		// TODO(knz): it is inefficient to do this dynamic dispatch based on
		// the match type and column layout again for every row. Consider
		// hoisting some of these checks to once per logical plan.
		switch fk.ref.Match {
		case sqlbase.ForeignKeyReference_SIMPLE:
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return errors.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					continue outer
				}
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &mutatedIdxHelpers[i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_FULL:
			var nulls, notNulls bool
			for _, colID := range fk.searchIdx.ColumnIDs[:fk.prefixLen] {
				found, ok := fk.ids[colID]
				if !ok {
					return errors.AssertionFailedf("fk ids (%v) missing column id %d", fk.ids, colID)
				}
				if mutatedRow[found] == tree.DNull {
					nulls = true
				} else {
					notNulls = true
				}
			}
			if nulls && notNulls {
				// TODO(bram): expand this error to show more details.
				return pgerror.Newf(pgcode.ForeignKeyViolation,
					"foreign key violation: MATCH FULL does not allow mixing of null and nonnull values %s for %s",
					mutatedRow, fk.ref.Name,
				)
			}
			// Never check references for MATCH FULL that are all nulls.
			if nulls {
				continue
			}
			if err := checkRunner.addCheck(ctx, mutatedRow, &mutatedIdxHelpers[i], traceKV); err != nil {
				return err
			}

		case sqlbase.ForeignKeyReference_PARTIAL:
			return unimplemented.NewWithIssue(20305, "MATCH PARTIAL not supported")

		default:
			return errors.AssertionFailedf("unknown composite key match type: %v", fk.ref.Match)
		}
	}
	return nil
}
