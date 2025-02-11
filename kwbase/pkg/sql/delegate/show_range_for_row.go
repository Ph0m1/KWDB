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

package delegate

import (
	"encoding/hex"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// delegateShowRangeForRow rewrites ShowRangeForRow statement to select statement which returns
// range_id, lease_holder... from kwdb_internal.ranges, kwdb_internal.gossip_nodes...
func (d *delegator) delegateShowRangeForRow(n *tree.ShowRangeForRow) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	idx, resName, err := cat.ResolveTableIndex(d.ctx, d.catalog, flags, &n.TableOrIndex)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}
	span := idx.Span()
	table := idx.Table()

	if len(n.Row) != table.ColumnCount() {
		return nil, errors.New("number of values in row must equal number of columns in the requested table")
	}

	// Process the Datums within the expressions.
	var semaCtx tree.SemaContext
	var rowExprs tree.Exprs
	for i, expr := range n.Row {
		colTyp := table.Column(i).DatumType()
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(expr, colTyp, "range-for-row", &semaCtx, false, false, "")
		if err != nil {
			return nil, err
		}
		if !tree.IsConst(d.evalCtx, typedExpr) {
			return nil, pgerror.Newf(pgcode.Syntax, "%s: row values must be constant", typedExpr)
		}
		datum, err := typedExpr.Eval(d.evalCtx)
		if err != nil {
			return nil, errors.Wrap(err, typedExpr.String())
		}
		rowExprs = append(rowExprs, datum)
	}

	idxSpanStart := hex.EncodeToString([]byte(span.Key))
	idxSpanEnd := hex.EncodeToString([]byte(span.EndKey))

	sqltelemetry.IncrementShowCounter(sqltelemetry.RangeForRow)

	// Format the expressions into a string to be passed into the kwdb_internal.encode_key function.
	// We have to be sneaky here and special case when exprs has length 1 and place a comma after the
	// the single tuple element so that we can deduce the expression actually has a tuple type for
	// the kwdb_internal.encode_key function.
	// Example: exprs = (1)
	// Output when used: kwdb_internal.encode_key(x, y, (1,))
	var fmtCtx tree.FmtCtx
	fmtCtx.WriteString("(")
	if len(rowExprs) == 1 {
		fmtCtx.FormatNode(rowExprs[0])
		fmtCtx.WriteString(",")
	} else {
		fmtCtx.FormatNode(&rowExprs)
	}
	fmtCtx.WriteString(")")
	rowString := fmtCtx.String()

	const query = `
SELECT
	CASE WHEN r.start_key < x'%[5]s' THEN NULL ELSE kwdb_internal.pretty_key(r.start_key, 2) END AS start_key,
	CASE WHEN r.end_key >= x'%[6]s' THEN NULL ELSE kwdb_internal.pretty_key(r.end_key, 2) END AS end_key,
	range_id,
	lease_holder,
	gossip_nodes.locality as lease_holder_locality,
	replicas,
	replica_localities
FROM %[4]s.kwdb_internal.ranges AS r
LEFT JOIN %[4]s.kwdb_internal.gossip_nodes ON lease_holder = node_id
WHERE (r.start_key <= kwdb_internal.encode_key(%[1]d, %[2]d, %[3]s))
  AND (r.end_key   >  kwdb_internal.encode_key(%[1]d, %[2]d, %[3]s)) ORDER BY r.start_key
	`
	// note: CatalogName.String() != Catalog()
	return parse(
		fmt.Sprintf(
			query,
			table.ID(),
			idx.ID(),
			rowString,
			resName.CatalogName.String(),
			idxSpanStart,
			idxSpanEnd,
		),
	)

}
